"""Deploy the PDF architecture to AWS. Idempotent: rerun after any code or .env change.

  python -m polarapt.deploy            create / update everything
  python -m polarapt.deploy --pause    disable all schedules (keep data)
  python -m polarapt.deploy --resume   enable all schedules
  python -m polarapt.deploy --destroy  delete everything (asks to type DELETE)

Task 1: EventBridge (3 rules, every minute) -> lambda_jobs_ingest / lambda_benchmarks_scraper / lambda_fx_ingest
        -> s3://data-hackathon-smit-<name>/raw/<source>/YYYY/MM/DD/HHMM.json  (contract in S3 metadata + file)
Task 2: S3 ObjectCreated -> SNS hcm-raw-events -> lambda_contract_router -> SNS FIFO hcm-routed-events.fifo
        -> filter policy on `source` -> jobs-queue.fifo / benchmarks-queue.fifo / fx-queue.fifo
        EventBridge -> lambda_benchmarks_transform -> S3 processed/
        EventBridge -> lambda_fx_to_sqlserver      -> RDS SQL Server Express (hcm.dbo.fx_rates)
        EventBridge Scheduler -> Glue glue_jobs_to_snowflake -> Snowflake HCM.TALENT.JOB_POSTINGS
"""
from __future__ import annotations

import argparse
import io
import json
import os
import secrets
import shutil
import string
import subprocess
import sys
import time
import zipfile

from botocore.exceptions import ClientError

from .aws import client
from .config import ROOT, SOURCES, cfg
from .log import log

RUNTIME = "python3.12"
DEPS = ["requests>=2.31", "beautifulsoup4>=4.12", "python-dotenv>=1.0", "pymssql>=2.3"]
ENV_KEYS = ["BUCKET_OWNER", "BUCKET_NAME", "MOCK_SOURCES", "USAJOBS_API_KEY", "USAJOBS_USER_AGENT",
            "USAJOBS_RESULTS_PER_PAGE", "USAJOBS_KEYWORD", "OXR_APP_ID", "FX_SYMBOLS", "BLS_URL", "BLS_TOP_N",
            "BLS_USER_AGENT"]
BASIC_EXEC = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
GLUE_SERVICE = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"


def _quiet(fn, *a, **kw):
    try:
        return fn(*a, **kw)
    except ClientError as exc:
        log("deploy", "skip", detail=str(exc)[:160])
        return None


# ------------------------------------------------------------------ preflight
def preflight() -> str:
    c = cfg()
    if c.owner == "yourname" and not c.bucket_override:
        sys.exit("Set BUCKET_OWNER in .env (e.g. BUCKET_OWNER=vardan -> data-hackathon-smit-vardan)")
    try:
        ident = client("sts").get_caller_identity()
    except Exception as exc:  # noqa: BLE001
        sys.exit(f"No working AWS credentials: {exc}")
    log("deploy", "preflight_ok", account=ident["Account"], region=c.region, bucket=c.bucket)
    return ident["Account"]


# ------------------------------------------------------------------ IAM
def _role(name: str, service: str, managed: list[str], inline: dict) -> str:
    iam = client("iam")
    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": service},
                                                     "Action": "sts:AssumeRole"}]}
    created = False
    try:
        arn = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        created = True
    except iam.exceptions.EntityAlreadyExistsException:
        arn = iam.get_role(RoleName=name)["Role"]["Arn"]
    for m in managed:
        iam.attach_role_policy(RoleName=name, PolicyArn=m)
    iam.put_role_policy(RoleName=name, PolicyName=f"{name}-inline", PolicyDocument=json.dumps(inline))
    if created:
        log("deploy", "role_created", role=name)
    return arn


def roles(account: str) -> dict:
    c, r = cfg(), cfg().region
    b = c.bucket
    queues = [f"arn:aws:sqs:{r}:{account}:{c.queue(s)}" for s in SOURCES]
    secrets_arn = f"arn:aws:secretsmanager:{r}:{account}:secret:hcm/*"
    lam = _role(c.lambda_role, "lambda.amazonaws.com", [BASIC_EXEC], {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": f"arn:aws:s3:::{b}/*"},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{b}"},
        {"Effect": "Allow", "Action": ["sns:Publish"], "Resource": f"arn:aws:sns:{r}:{account}:{c.routed_topic}"},
        {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueUrl",
                                       "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"], "Resource": queues},
        {"Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"], "Resource": secrets_arn},
        {"Effect": "Allow", "Action": ["rds:DescribeDBInstances"], "Resource": "*"},
    ]})
    glue = _role(c.glue_role, "glue.amazonaws.com", [GLUE_SERVICE], {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": f"arn:aws:s3:::{b}/*"},
        {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": f"arn:aws:s3:::{b}"},
        {"Effect": "Allow", "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueUrl",
                                       "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"], "Resource": queues},
        {"Effect": "Allow", "Action": ["secretsmanager:GetSecretValue"], "Resource": secrets_arn},
    ]})
    sched = _role(c.scheduler_role, "scheduler.amazonaws.com", [], {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": ["glue:StartJobRun"],
         "Resource": f"arn:aws:glue:{r}:{account}:job/{c.glue_job}"}]})
    return {"lambda": lam, "glue": glue, "scheduler": sched}


# ------------------------------------------------------------------ SQL Server (start first: takes ~15-20 min)
def ensure_sqlserver() -> str:
    c, rds, ec2, sm = cfg(), client("rds"), client("ec2"), client("secretsmanager")
    try:
        db = rds.describe_db_instances(DBInstanceIdentifier=c.rds_id)["DBInstances"][0]
        log("deploy", "sqlserver_exists", status=db["DBInstanceStatus"])
        return db["DBInstanceStatus"]
    except rds.exceptions.DBInstanceNotFoundFault:
        pass
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])["Vpcs"]
    if not vpcs:
        sys.exit("No default VPC in this region. VPC console > Actions > Create default VPC, then rerun.")
    vpc = vpcs[0]["VpcId"]
    sgs = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [c.rds_sg]},
                                                {"Name": "vpc-id", "Values": [vpc]}])["SecurityGroups"]
    sg = sgs[0]["GroupId"] if sgs else ec2.create_security_group(
        GroupName=c.rds_sg, Description="HCM hackathon SQL Server 1433", VpcId=vpc)["GroupId"]
    try:
        ec2.authorize_security_group_ingress(GroupId=sg, IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 1433, "ToPort": 1433,
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Lambda (no VPC) + laptop; hackathon only"}]}])
    except ClientError as exc:
        if "Duplicate" not in str(exc):
            raise
    try:
        sec = json.loads(sm.get_secret_value(SecretId=c.sql_secret)["SecretString"])
    except sm.exceptions.ResourceNotFoundException:
        pw = "".join(secrets.choice(string.ascii_letters + string.digits) for _ in range(24))
        sec = {"username": "hcmadmin", "password": pw, "database": c.sql_database, "db_instance": c.rds_id}
        sm.create_secret(Name=c.sql_secret, SecretString=json.dumps(sec))
    last = None
    for cls in ("db.t3.micro", "db.t3.small"):
        try:
            rds.create_db_instance(
                DBInstanceIdentifier=c.rds_id, Engine="sqlserver-ex", DBInstanceClass=cls, AllocatedStorage=20,
                StorageType="gp2", MasterUsername=sec["username"], MasterUserPassword=sec["password"],
                PubliclyAccessible=True, VpcSecurityGroupIds=[sg], BackupRetentionPeriod=0, MultiAZ=False,
                LicenseModel="license-included", DeletionProtection=False, AutoMinorVersionUpgrade=True)
            log("deploy", "sqlserver_creating", instance_class=cls, note="takes ~15-20 minutes")
            return "creating"
        except ClientError as exc:
            last = exc
            if "InvalidParameterCombination" not in str(exc):
                raise
    raise last


# ------------------------------------------------------------------ storage + messaging
def ensure_bucket() -> None:
    s3, c = client("s3"), cfg()
    try:
        s3.head_bucket(Bucket=c.bucket)
        return
    except ClientError:
        pass
    kw = {} if c.region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": c.region}}
    s3.create_bucket(Bucket=c.bucket, **kw)
    log("deploy", "bucket_created", bucket=c.bucket)


def ensure_topics(account: str) -> tuple[str, str]:
    sns, c = client("sns"), cfg()
    raw_arn = sns.create_topic(Name=c.raw_topic)["TopicArn"]
    sns.set_topic_attributes(TopicArn=raw_arn, AttributeName="Policy", AttributeValue=json.dumps({
        "Version": "2012-10-17", "Statement": [{
            "Sid": "S3Publish", "Effect": "Allow", "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sns:Publish", "Resource": raw_arn,
            "Condition": {"ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{c.bucket}"},
                          "StringEquals": {"aws:SourceAccount": account}}}]}))
    fifo_arn = sns.create_topic(Name=c.routed_topic, Attributes={"FifoTopic": "true",
                                                                  "ContentBasedDeduplication": "false"})["TopicArn"]
    client("s3").put_bucket_notification_configuration(Bucket=c.bucket, NotificationConfiguration={
        "TopicConfigurations": [{"Id": "raw-objects-to-sns", "TopicArn": raw_arn, "Events": ["s3:ObjectCreated:*"],
                                 "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "raw/"},
                                                                    {"Name": "suffix", "Value": ".json"}]}}}]})
    log("deploy", "topics_ready", raw=c.raw_topic, fifo=c.routed_topic)
    return raw_arn, fifo_arn


def ensure_queues(fifo_topic_arn: str) -> None:
    sqs, sns, c = client("sqs"), client("sns"), cfg()
    existing = {s["Endpoint"]: s for s in sns.list_subscriptions_by_topic(TopicArn=fifo_topic_arn)["Subscriptions"]}
    for s in SOURCES:
        dlq = sqs.create_queue(QueueName=c.dlq(s), Attributes={"FifoQueue": "true"})["QueueUrl"]
        dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        url = sqs.create_queue(QueueName=c.queue(s), Attributes={"FifoQueue": "true"})["QueueUrl"]
        arn = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
        sqs.set_queue_attributes(QueueUrl=url, Attributes={
            "VisibilityTimeout": "900" if s == "jobs" else "120",  # > consumer run time (Glue up to 15 min)
            "MessageRetentionPeriod": "345600",
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "3"}),
            "Policy": json.dumps({"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "sns.amazonaws.com"}, "Action": "sqs:SendMessage",
                "Resource": arn, "Condition": {"ArnEquals": {"aws:SourceArn": fifo_topic_arn}}}]})})
        attrs = {"FilterPolicy": json.dumps({"source": [s]}), "RawMessageDelivery": "true"}
        if arn in existing:
            for k, v in attrs.items():
                sns.set_subscription_attributes(SubscriptionArn=existing[arn]["SubscriptionArn"],
                                                AttributeName=k, AttributeValue=v)
        else:
            sns.subscribe(TopicArn=fifo_topic_arn, Protocol="sqs", Endpoint=arn, Attributes=attrs)
        log("deploy", "queue_ready", queue=c.queue(s), dlq=c.dlq(s), filter=s)


# ------------------------------------------------------------------ Lambda
def build_zip() -> bytes:
    build = ROOT / "build" / "lambda"
    if build.exists():
        shutil.rmtree(build)
    build.mkdir(parents=True)
    if shutil.which("uv"):
        cmd = ["uv", "pip", "install", "--quiet", "--target", str(build), "--python-version", "3.12",
               "--python-platform", "x86_64-manylinux_2_28", "--only-binary", ":all:", *DEPS]
    else:
        cmd = [sys.executable, "-m", "pip", "install", "--quiet", "--target", str(build), "--python-version",
               "3.12", "--platform", "manylinux_2_28_x86_64", "--only-binary=:all:", *DEPS]
    subprocess.run(cmd, check=True)
    shutil.copytree(ROOT / "src" / "polarapt", build / "polarapt", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copytree(ROOT / "fixtures", build / "fixtures")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for f in build.rglob("*"):
            if f.is_file() and "__pycache__" not in f.parts:
                z.write(f, f.relative_to(build).as_posix())
    log("deploy", "package_built", mb=round(len(buf.getvalue()) / 1e6, 2))
    return buf.getvalue()


def lambda_env(extra: dict | None = None) -> dict:
    env = {k: os.environ[k] for k in ENV_KEYS if os.environ.get(k)}
    env.update(POLARAPT_ROOT="/var/task", **(extra or {}))
    return env


def upsert_lambda(name: str, handler: str, role: str, code: bytes, env: dict, timeout: int, memory: int,
                  scheduled: bool) -> str:
    lam = client("lambda")
    conf = dict(FunctionName=name, Runtime=RUNTIME, Handler=handler, Role=role, Timeout=timeout, MemorySize=memory,
                Environment={"Variables": env})
    try:
        lam.get_function(FunctionName=name)
        lam.update_function_code(FunctionName=name, ZipFile=code)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        lam.update_function_configuration(**conf)
        lam.get_waiter("function_updated_v2").wait(FunctionName=name)
        log("deploy", "lambda_updated", name=name)
    except lam.exceptions.ResourceNotFoundException:
        for attempt in range(15):  # new IAM roles take a few seconds to become assumable
            try:
                lam.create_function(**conf, Code={"ZipFile": code}, Architectures=["x86_64"])
                break
            except ClientError as exc:
                if "role" in str(exc).lower() and attempt < 14:
                    time.sleep(4)
                    continue
                raise
        lam.get_waiter("function_active_v2").wait(FunctionName=name)
        log("deploy", "lambda_created", name=name)
    if scheduled:  # don't let EventBridge retry an API call twice more on failure (quota)
        lam.put_function_event_invoke_config(FunctionName=name, MaximumRetryAttempts=0,
                                             MaximumEventAgeInSeconds=120)
    return lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]


def every_minute(rule: str, fn_name: str, fn_arn: str) -> None:
    ev, lam = client("events"), client("lambda")
    rule_arn = ev.put_rule(Name=rule, ScheduleExpression="rate(1 minute)", State="ENABLED")["RuleArn"]
    try:
        lam.add_permission(FunctionName=fn_name, StatementId=f"evb-{rule}", Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    ev.put_targets(Rule=rule, Targets=[{"Id": "fn", "Arn": fn_arn}])
    log("deploy", "rule_ready", rule=rule, target=fn_name)


def subscribe_router(raw_topic_arn: str, router_arn: str) -> None:
    sns, lam, c = client("sns"), client("lambda"), cfg()
    try:
        lam.add_permission(FunctionName=c.router_lambda, StatementId="sns-raw-events", Action="lambda:InvokeFunction",
                           Principal="sns.amazonaws.com", SourceArn=raw_topic_arn)
    except lam.exceptions.ResourceConflictException:
        pass
    subs = sns.list_subscriptions_by_topic(TopicArn=raw_topic_arn)["Subscriptions"]
    if not any(s["Endpoint"] == router_arn for s in subs):
        sns.subscribe(TopicArn=raw_topic_arn, Protocol="lambda", Endpoint=router_arn)
    log("deploy", "router_subscribed", topic=c.raw_topic)


# ------------------------------------------------------------------ Glue + schedule
def ensure_glue(role_arn: str, sched_role: str, account: str) -> None:
    glue, s3, c = client("glue"), client("s3"), cfg()
    script_key = "scripts/glue_jobs_to_snowflake.py"
    s3.upload_file(str(ROOT / "src" / "polarapt" / "glue" / "jobs_to_snowflake.py"), c.bucket, script_key)
    job = {"Role": role_arn, "Command": {"Name": "pythonshell", "PythonVersion": "3.9",
                                         "ScriptLocation": f"s3://{c.bucket}/{script_key}"},
           "DefaultArguments": {"--additional-python-modules": "snowflake-connector-python==3.12.4,urllib3==1.26.20",
                                "--QUEUE_NAME": c.queue("jobs"), "--SECRET_NAME": c.snowflake_secret,
                                "--REGION": c.region, "library-set": "analytics"},
           "MaxCapacity": 0.0625, "Timeout": 15, "MaxRetries": 0, "ExecutionProperty": {"MaxConcurrentRuns": 1},
           "Description": "jobs-queue.fifo -> Snowflake HCM.TALENT.JOB_POSTINGS"}
    try:
        glue.get_job(JobName=c.glue_job)
        glue.update_job(JobName=c.glue_job, JobUpdate=job)
        log("deploy", "glue_updated", job=c.glue_job)
    except glue.exceptions.EntityNotFoundException:
        for attempt in range(10):
            try:
                glue.create_job(Name=c.glue_job, **job)
                break
            except ClientError as exc:
                if "role" in str(exc).lower() and attempt < 9:
                    time.sleep(4)
                    continue
                raise
        log("deploy", "glue_created", job=c.glue_job)

    sm = client("secretsmanager")
    try:
        sm.describe_secret(SecretId=c.snowflake_secret)
        has_sf = True
    except sm.exceptions.ResourceNotFoundException:
        has_sf = False
    sch = client("scheduler")
    params = dict(Name=c.glue_schedule_name, ScheduleExpression=c.glue_schedule,
                  FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED" if has_sf else "DISABLED",
                  Target={"Arn": "arn:aws:scheduler:::aws-sdk:glue:startJobRun", "RoleArn": sched_role,
                          "Input": json.dumps({"JobName": c.glue_job}),
                          "RetryPolicy": {"MaximumRetryAttempts": 0}})
    for attempt in range(10):
        try:
            try:
                sch.get_schedule(Name=c.glue_schedule_name)
                sch.update_schedule(**params)
            except sch.exceptions.ResourceNotFoundException:
                sch.create_schedule(**params)
            break
        except ClientError as exc:
            if "role" in str(exc).lower() and attempt < 9:
                time.sleep(4)
                continue
            raise
    log("deploy", "glue_schedule_ready", schedule=c.glue_schedule, enabled=has_sf)
    if not has_sf:
        print("NOTE: Glue schedule is OFF until you run:  uv run python -m polarapt.snowflake_setup --account <id>")


# ------------------------------------------------------------------ commands
def deploy() -> None:
    account = preflight()
    c = cfg()
    status = ensure_sqlserver()               # slowest resource: kick it off first
    ensure_bucket()
    raw_arn, fifo_arn = ensure_topics(account)
    ensure_queues(fifo_arn)
    r = roles(account)
    time.sleep(8)                              # IAM propagation
    code = build_zip()
    env = lambda_env()
    for s in SOURCES:
        handler = f"polarapt.ingest.{s}_handler"
        arn = upsert_lambda(c.ingest_lambda[s], handler, r["lambda"], code, env, 60, 256, scheduled=True)
        every_minute(c.ingest_rule(s), c.ingest_lambda[s], arn)
    router_arn = upsert_lambda(c.router_lambda, "polarapt.router.handler", r["lambda"], code,
                               lambda_env({"ROUTED_TOPIC_ARN": fifo_arn}), 30, 256, scheduled=False)
    subscribe_router(raw_arn, router_arn)
    b_arn = upsert_lambda(c.benchmarks_lambda, "polarapt.process_benchmarks.handler", r["lambda"], code, env,
                          60, 256, scheduled=True)
    every_minute(c.process_rule("benchmarks"), c.benchmarks_lambda, b_arn)
    f_arn = upsert_lambda(c.fx_lambda, "polarapt.process_fx.handler", r["lambda"], code, env, 60, 256,
                          scheduled=True)
    every_minute(c.process_rule("fx"), c.fx_lambda, f_arn)
    ensure_glue(r["glue"], r["scheduler"], account)
    print(f"\nDEPLOYED  bucket={c.bucket}  region={c.region}  sqlserver={status}")
    print("Watch it:  uv run python -m polarapt.status --watch 15")


def set_state(enabled: bool) -> None:
    preflight()
    c, ev, sch = cfg(), client("events"), client("scheduler")
    rules = [c.ingest_rule(s) for s in SOURCES] + [c.process_rule("benchmarks"), c.process_rule("fx")]
    for rule in rules:
        _quiet(ev.enable_rule if enabled else ev.disable_rule, Name=rule)
    try:
        cur = sch.get_schedule(Name=c.glue_schedule_name)
        sch.update_schedule(Name=cur["Name"], ScheduleExpression=cur["ScheduleExpression"],
                            FlexibleTimeWindow=cur["FlexibleTimeWindow"], Target=cur["Target"],
                            State="ENABLED" if enabled else "DISABLED")
    except ClientError as exc:
        log("deploy", "skip", detail=str(exc)[:120])
    log("deploy", "resumed" if enabled else "paused")


def destroy() -> None:
    preflight()
    c = cfg()
    ev, lam, sns, sqs, s3, iam, glue, sch, rds, sm, ec2 = (client(x) for x in (
        "events", "lambda", "sns", "sqs", "s3", "iam", "glue", "scheduler", "rds", "secretsmanager", "ec2"))
    for rule in [c.ingest_rule(s) for s in SOURCES] + [c.process_rule("benchmarks"), c.process_rule("fx")]:
        _quiet(ev.remove_targets, Rule=rule, Ids=["fn"])
        _quiet(ev.delete_rule, Name=rule)
    _quiet(sch.delete_schedule, Name=c.glue_schedule_name)
    _quiet(glue.delete_job, JobName=c.glue_job)
    for fn in [*c.ingest_lambda.values(), c.router_lambda, c.benchmarks_lambda, c.fx_lambda]:
        _quiet(lam.delete_function, FunctionName=fn)
    for t in sns.list_topics()["Topics"]:
        if t["TopicArn"].split(":")[-1] in (c.raw_topic, c.routed_topic):
            _quiet(sns.delete_topic, TopicArn=t["TopicArn"])
    for s in SOURCES:
        for q in (c.queue(s), c.dlq(s)):
            try:
                _quiet(sqs.delete_queue, QueueUrl=sqs.get_queue_url(QueueName=q)["QueueUrl"])
            except ClientError:
                pass
    try:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=c.bucket):
            keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
            if keys:
                s3.delete_objects(Bucket=c.bucket, Delete={"Objects": keys})
        _quiet(s3.delete_bucket, Bucket=c.bucket)
    except ClientError as exc:
        log("destroy", "skip", detail=str(exc)[:120])
    _quiet(rds.delete_db_instance, DBInstanceIdentifier=c.rds_id, SkipFinalSnapshot=True,
           DeleteAutomatedBackups=True)
    for sec in (c.sql_secret, c.snowflake_secret):
        _quiet(sm.delete_secret, SecretId=sec, ForceDeleteWithoutRecovery=True)
    for role, managed in ((c.lambda_role, [BASIC_EXEC]), (c.glue_role, [GLUE_SERVICE]), (c.scheduler_role, [])):
        _quiet(iam.delete_role_policy, RoleName=role, PolicyName=f"{role}-inline")
        for m in managed:
            _quiet(iam.detach_role_policy, RoleName=role, PolicyArn=m)
        _quiet(iam.delete_role, RoleName=role)
    print("Destroyed. The SQL Server takes ~10 min to finish deleting; its security group "
          f"'{c.rds_sg}' can be deleted from the EC2 console afterwards (it costs nothing).")


def main() -> None:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--pause", action="store_true")
    g.add_argument("--resume", action="store_true")
    g.add_argument("--destroy", action="store_true")
    a = ap.parse_args()
    if a.destroy:
        if input("Type DELETE to remove every HCM resource from AWS: ").strip() == "DELETE":
            destroy()
    elif a.pause:
        set_state(False)
    elif a.resume:
        set_state(True)
    else:
        deploy()


if __name__ == "__main__":
    main()
