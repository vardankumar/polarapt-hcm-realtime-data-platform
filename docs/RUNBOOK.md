# RUNBOOK v2 — Deploy the PDF architecture (Windows PowerShell)

Run every command in the **VS Code terminal (PowerShell)**, inside the folder that contains `SPEC.md`.

## A. Replace the project folder (3 min)
1. In File Explorer, rename your old folder `polarapt-backbone` to `polarapt-backbone-OLD`.
2. Extract the new `polarapt-backbone.zip` in the same place (Desktop\Projects\Hackathon).
3. In VS Code: **File → Open Folder…** → select the new `polarapt-backbone` folder.
4. Open the terminal (**Terminal → New Terminal**) and install the libraries:
   ```powershell
   uv sync --extra dev
   ```

## B. Settings (5 min)
1. Create the settings file:
   ```powershell
   copy .env.example .env
   ```
2. Open `.env` and fill these in (copy the values from `polarapt-backbone-OLD\.env`). Use no quotes and no spaces around `=`.
   ```
   AWS_ACCESS_KEY_ID=AKIA...
   AWS_SECRET_ACCESS_KEY=...
   BUCKET_OWNER=vardan
   USAJOBS_API_KEY=...
   USAJOBS_USER_AGENT=your-registered-email
   BLS_USER_AGENT=Mozilla/5.0 (compatible; PolaraptHCM/1.0; your-registered-email)
   OXR_APP_ID=...
   ```
   Keep `MOCK_SOURCES=false`. Press **Ctrl+S** to save.
3. Run the tests:
   ```powershell
   uv run pytest -q
   ```
   ✅ Expected: `14 passed`

## C. Deploy (5–8 min of commands; SQL Server then finishes by itself in ~15–20 min)
```powershell
uv run python -m polarapt.deploy
```
✅ It ends with `DEPLOYED  bucket=data-hackathon-smit-vardan ... sqlserver=creating`.
Then the line `NOTE: Glue schedule is OFF until you run ... snowflake_setup` appears. That's expected; Part D fixes it.

## D. Connect Snowflake (5 min)
1. Run this, using your account identifier from the Snowflake trial:
   ```powershell
   uv run python -m polarapt.snowflake_setup --account ABCDEFG-XY12345
   ```
2. Open the new file `snowflake_setup.sql` in VS Code and copy all of it (**Ctrl+A**, **Ctrl+C**).
3. In Snowflake (Snowsight): **Projects → Worksheets → +** (new SQL worksheet). Paste the SQL, then click the **▼ next to Run → Run All**.
   ✅ Every statement shows success.
4. Run the deploy again, so the Glue schedule switches on:
   ```powershell
   uv run python -m polarapt.deploy
   ```

## E. Watch it (keep running)
```powershell
uv run python -m polarapt.status --watch 15
```
| Time | What you should see |
|---|---|
| 1–2 min | RAW ZONE: jobs / benchmarks / fx files growing by 1 every minute, `records=… status=200` |
| 2–3 min | `S3 processed/benchmarks` files growing; `#1 Pediatric surgeons $559,030 (BLS 2025)` |
| 5–7 min | `Glue -> Snowflake last run SUCCEEDED` |
| ~20 min | `SQL Server (RDS) available`; the fx queue drains on the next minute |
| always | **DLQ = 0** for every queue |

The fx queue **growing while SQL Server is still being created is normal**: the messages wait and are loaded once the database is ready. That's the queue doing its job.

## F. Prove each target (screenshots for judges)
- **S3:** your bucket → `raw/jobs/2026/10/25/` → click a file → **Metadata** section shows `x-amz-meta-source`, `record-count`, `response-status`, `ingested-at`.
- **SNS:** `hcm-routed-events.fifo` → Subscriptions → 3 queues, each with a filter policy.
- **SQS:** 3 `.fifo` queues + 3 `-dlq.fifo` queues.
- **Snowflake worksheet:**
  ```sql
  SELECT COUNT(*) FROM HCM.TALENT.JOB_POSTINGS;
  SELECT TITLE, ORGANIZATION, PAY_MIN_ANNUAL, PAY_MAX_ANNUAL, CLOSE_DATE FROM HCM.TALENT.JOB_POSTINGS LIMIT 20;
  ```
- **SQL Server:** AWS console → **RDS → Query Editor** isn't available for SQL Server, so show the **Lambda logs** (`lambda_fx_to_sqlserver` → Monitor → CloudWatch logs → `"merged", "rows": 6`). Optionally, connect with SSMS or Azure Data Studio using the endpoint from status, user `hcmadmin`, and the password from **Secrets Manager → hcm/sqlserver → Retrieve secret value**:
  ```sql
  SELECT TOP 20 * FROM hcm.dbo.fx_rates ORDER BY loaded_at DESC;
  ```
- **S3 processed:** `processed/benchmarks/latest.json` and the `.csv` files.

## G. After judging
```powershell
uv run python -m polarapt.deploy --pause      # stop the per-minute runs
uv run python -m polarapt.deploy --destroy    # delete everything (type DELETE)
```

## Errors
| Message | Fix |
|---|---|
| `Set BUCKET_OWNER in .env` | Add `BUCKET_OWNER=yourname` (lowercase). |
| `BucketAlreadyExists` | That name is taken worldwide: change `BUCKET_OWNER`, e.g. `vardankumar`. |
| `No default VPC` | VPC console → Actions → Create default VPC, then deploy again. |
| `package_built` step fails mentioning `pymssql` | Send me the error. |
| Glue run `FAILED` | Copy the ERROR shown in status (or Glue console → job → Runs → Error) and send it to me. |
| DLQ > 0 | CloudWatch logs of that processor → copy the `process_error` line. |
