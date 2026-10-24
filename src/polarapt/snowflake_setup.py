"""One-time Snowflake setup for the Glue loader (key-pair auth, works with MFA-enforced trial accounts).

  python -m polarapt.snowflake_setup --account ABCDEFG-XY12345

1. Generates an RSA key pair (saved in ./secrets/, never committed).
2. Writes snowflake_setup.sql: paste it into a Snowsight worksheet and click Run All.
3. Stores the connection details + private key in AWS Secrets Manager (hcm/snowflake) for the Glue job.
"""
from __future__ import annotations

import argparse
import json

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from .aws import client
from .config import ROOT, cfg

USER, ROLE, WH, DB, SCHEMA = "HCM_LOADER", "HCM_LOADER_ROLE", "COMPUTE_WH", "HCM", "TALENT"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", required=True, help="account identifier, e.g. ABCDEFG-XY12345")
    a = ap.parse_args()
    account = a.account.strip().replace(".snowflakecomputing.com", "").replace("https://", "")

    secrets_dir = ROOT / "secrets"
    secrets_dir.mkdir(exist_ok=True)
    key_file = secrets_dir / "snowflake_key.p8"
    if key_file.exists():
        key = serialization.load_pem_private_key(key_file.read_bytes(), password=None)
    else:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                               serialization.NoEncryption()))
    pem = key_file.read_text()
    pub = key.public_key().public_bytes(serialization.Encoding.PEM,
                                        serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    pub_body = "".join(l for l in pub.splitlines() if "-----" not in l)

    sql = f"""-- Paste into a Snowsight worksheet (Projects > Worksheets > +) and click "Run All" (or Ctrl+Shift+Enter)
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS {WH} WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
CREATE DATABASE IF NOT EXISTS {DB};
CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA};
CREATE ROLE IF NOT EXISTS {ROLE};
GRANT USAGE ON WAREHOUSE {WH} TO ROLE {ROLE};
GRANT USAGE ON DATABASE {DB} TO ROLE {ROLE};
GRANT ALL ON SCHEMA {DB}.{SCHEMA} TO ROLE {ROLE};
GRANT ALL ON FUTURE TABLES IN SCHEMA {DB}.{SCHEMA} TO ROLE {ROLE};
CREATE USER IF NOT EXISTS {USER} TYPE = SERVICE DEFAULT_ROLE = {ROLE} DEFAULT_WAREHOUSE = {WH};
ALTER USER {USER} SET RSA_PUBLIC_KEY = '{pub_body}';
GRANT ROLE {ROLE} TO USER {USER};
GRANT ROLE {ROLE} TO ROLE ACCOUNTADMIN;
-- After the pipeline runs:  SELECT * FROM {DB}.{SCHEMA}.JOB_POSTINGS ORDER BY LAST_SEEN_AT DESC LIMIT 50;
"""
    (ROOT / "snowflake_setup.sql").write_text(sql)

    secret = json.dumps({"account": account, "user": USER, "role": ROLE, "warehouse": WH, "database": DB,
                         "schema": SCHEMA, "private_key": pem})
    sm = client("secretsmanager")
    try:
        sm.create_secret(Name=cfg().snowflake_secret, SecretString=secret)
    except sm.exceptions.ResourceExistsException:
        sm.put_secret_value(SecretId=cfg().snowflake_secret, SecretString=secret)
    print(f"Saved Snowflake connection to AWS Secrets Manager: {cfg().snowflake_secret}")
    print(f"NEXT: open {ROOT / 'snowflake_setup.sql'}, copy all of it into a Snowsight worksheet, Run All.")
    print("Then run the deploy again so the Glue schedule is switched on.")


if __name__ == "__main__":
    main()
