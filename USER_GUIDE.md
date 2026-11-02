# SIT validation: user guide

This guide explains what the SIT validation notebook does, how to set it up once, how to run it after a load, and how to read and act on the results. For every config setting, see [CONFIG.md](CONFIG.md). For a short overview, see [README.md](README.md).

## 1. What it is

After data moves RAW → STG → TGT (CNFZ), someone has to confirm the load did what it should. This notebook does that check for you. For each table (called an *entity*) you list in a config file, it compares the three layers and reports, check by check, what passed and what did not.

It is deliberately light:
- One Databricks notebook, run by hand after a load.
- Nothing is written to Delta tables. Results are shown in the notebook and can be saved as CSV and Excel files.
- No alerts, no scheduling, no CI/CD.
- It only reads data. The queries it runs for a `raw` query or a data quality `sql` rule are wrapped so that statements which change data fail to parse and never run.

It does not check referential integrity (foreign keys), and it does not yet handle incremental loads (see [section 9](#9-what-it-does-not-do-yet)).

## 2. How it works

```
config file (your entities)            SQL files (optional, for RAW built from several tables)
        │                                        │
        └──────────────┬─────────────────────────┘
                       ▼
        sit_validation notebook  ── reads ──►  RAW, STG, TGT tables (your own credentials)
                       │                       TGT information_schema (types, lengths, defaults)
                       ▼
   summary by category + full check list + sample rows of failures
                       │
                       ▼ (optional, if output_dir is set)
        sit_results_latest.csv, sit_results_<time>.csv, sit_results_<time>.xlsx
```

For each entity the notebook:
1. Checks the config for mistakes.
2. Loads the RAW, STG and TGT tables (and any rejects tables). If a table does not exist yet, the entity is **skipped**.
3. Runs every check the config gives it enough information for. A check whose settings are missing does not run.
4. Moves on to the next entity, whatever happened.

## 3. What it covers

Your 12 SIT tests, and where each one appears in the results:

| # | Test | Result category / check |
|---|---|---|
| 1 | Record counts Raw vs STG vs TGT | Record Count: *RAW vs STG*, *STG vs TGT* |
| 2 | Primary key unique and not null | Primary Key: *Not null*, *Unique* (per layer) |
| 3 | Alternate key unique | Alternate Key: *Unique* |
| 4 | Full row reconciliation STG vs TGT | Reconciliation: *Full row: STG vs TGT* |
| 5 | Key reconciliation RAW vs TGT | Reconciliation: *Key: RAW vs TGT* |
| 6 | Column presence vs mapping | Column Mapping: *Columns present*, *Unmapped TGT columns* |
| 7 | Data types vs mapping | Column Mapping: *Data types* |
| 8 | Column length vs mapping | Column Mapping: *Lengths*, *Length declared in catalog* |
| 9 | Default values | Nullability and Defaults: *Default values* |
| 10 | Nullability / mandatory columns | Nullability and Defaults: *Declared nullability vs mapping*, *Mandatory columns populated* |
| 11 | Data quality | Data Quality: one row per rule you define |
| 12 | Audit columns | Audit Columns: *Present and populated* |

Extras: aggregate reconciliation (sum, min, max and count of chosen columns), and extra data quality rule types (no leading or trailing spaces, not empty, A < B, range).

## 4. One-time setup

### 4.1 Put the notebook in Databricks
Import `sit_validation.py` (or `sit_validation.ipynb`) into your workspace, for example under `/Workspace/Users/<you>/`. Both files contain the same code. Everyone who runs SIT uses the same notebook but their own config.

### 4.2 Check your access
The identity that runs the notebook (you, when you run it by hand) needs:
- read access to the RAW, STG and TGT tables for every entity (and every table used in a `raw` query, and any rejects tables);
- read access to the config file and any SQL files, wherever you uploaded them;
- the ability to read `information_schema` for the TGT table. It is not a separate grant: it lists the tables you can already access. If it returns nothing you get a warning and the type, declared length, declared nullability and declared default comparisons are skipped;
- if you set `output_dir`, write access to that folder (for a volume, the write permission on the volume).

### 4.3 Write a config file
Create a `.json` (or `.yaml`) file with one entry per entity, and upload it to the workspace, for example `/Workspace/Users/<you>/sit_config/customer.json`. Start from [example_config.json](example_config.json). The minimum for an entity is its three full table names, which gives you the record counts:

```json
{
  "customer": {
    "raw": "RAW.crm.customer",
    "stg": "STG.crm.customer_stg",
    "tgt": "CNFZ.core.dim_customer"
  }
}
```

Add `keys`, `columns`, `audit_cols`, `dq` and the other settings to switch on more checks. [CONFIG.md](CONFIG.md) lists each setting and which checks it enables.

Tips:
- Add entities gradually and run after each addition, so a mistake is easy to find.
- A large config is fine. If it gets hard to manage, you can keep one file per team or per subject area, and point `config_path` at the one you need.
- If the entity's tables are not created yet, add it anyway. It is skipped until they exist.

### 4.4 RAW built from several tables (optional)
When STG is produced by joining several RAW tables, the `raw` setting can be a query. Upload the SQL file to the workspace and refer to it:

```json
"raw": {"sql_file": "/Workspace/Users/<you>/sit_sql/orders_join.sql", "params": {"env": "dev"}}
```

Things to know:
- The file must hold one `SELECT` (or `WITH ... SELECT`), saved as UTF-8. If your ETL file is an `INSERT ... SELECT`, upload a copy with only the `SELECT`.
- Use full `catalog.schema.table` names inside the query.
- The file is a copy. When the original changes, upload it again. Each run shows the file path and when it was last changed (Setup / *RAW source*).
- If the query is a copy of the ETL's own join, the RAW checks prove the load matches that join. They will not catch a join that was wrong to begin with.

Details are in [CONFIG.md](CONFIG.md#raw-built-from-several-tables-a-query).

### 4.5 Create the job (optional, recommended)
You can run the notebook directly, but a job gives you a one-click "run with these settings".

1. In Databricks, go to **Workflows → Create job**.
2. Add one task: type **Notebook**, source **Workspace**, and the path to `sit_validation`.
3. Choose **serverless** compute (or an existing cluster).
4. Add these parameters (names must match exactly):

| Parameter | Value |
|---|---|
| `config_path` | Path to your config file |
| `entities` | Blank for every entity in the file, or a comma-separated list such as `customer,orders` |
| `output_dir` | A folder of your own for result files, or blank for none |
| `fail_run_on` | `setup` (recommended), `never` or `any` |

Menu names can differ slightly between Databricks versions. The only requirement is a notebook task with those four parameters.

## 5. Running it

**After each load:**
1. Open the job and choose **Run now** (or **Run now with different parameters** to change `entities` or `config_path` for this run).
2. When it finishes, open the run and read the notebook output (see [section 6](#6-reading-the-results)).

**Interactively:** open the notebook, fill in the four widgets at the top, and **Run all**.

**Does a failed check fail the job?** Not by default. One or two failed checks are expected in SIT, so with `fail_run_on = setup` the job only goes red when something could not run at all (a config mistake, no access to a table, an error inside a check). The options:

| `fail_run_on` | Job is marked failed when |
|---|---|
| `setup` (default) | something could not run (any failed *Setup* row: a config mistake, a table that could not be read, an unknown entity name, or an error inside a check) |
| `never` | never |
| `any` | any check failed, or something could not run |

Skipped entities and warnings never fail the job. The notebook's last line is a one-line summary, for example:
`SIT completed: 118 passed, 2 failed, 1 warnings, 3 skipped across 6 entity(ies)`

## 6. Reading the results

### 6.1 Statuses

| Status | Meaning |
|---|---|
| `PASS` | The check ran and the layers agree. |
| `FAIL` | The check ran and found a difference. This is a SIT finding. |
| `WARN` | Worth a look, but not a finding on its own, for example a catalog that does not declare a length. Never fails the job. |
| `SKIPPED` | The entity was not checked because one of its tables does not exist yet. Never fails the job. |

### 6.2 What the notebook shows
1. **Summary by category.** One row per entity and category, worst status first. Start here.
2. **All checks.** One row per check with `expected`, `actual` and `detail`. Failures come first.
3. **Sample rows.** Up to 20 example rows for each failed full row or key reconciliation, each duplicate primary key, and each failed data quality rule, so you can see what is wrong. Alternate key and aggregate failures have no sample rows; the `detail` column says what differs.

If `output_dir` is set, the same results are written to `sit_results_latest.csv` (overwritten each run), `sit_results_<time>.csv` (one per run) and `sit_results_<time>.xlsx` (Run, Summary and Results sheets, plus one sheet per set of sample rows). Every CSV row carries `run_time_utc` and `run_by`.

### 6.3 What each check means and what to do when it fails

| Category / check | What it tests | A failure usually means |
|---|---|---|
| **Record Count** / RAW vs STG | RAW rows = STG rows + rows in the STG rejects table | Rows were lost or duplicated between RAW and STG. Check the rejects table and any filters. |
| **Record Count** / STG vs TGT | STG rows = TGT rows + rows in the TGT rejects table | Rows missing from, or extra in, TGT. |
| **Primary Key** / Not null | No nulls in the key columns, per layer | A key column was not populated. |
| **Primary Key** / Unique | No key appears twice, per layer | Duplicates. The sample rows show the repeated keys. If RAW is a join, a join can multiply rows. |
| **Alternate Key** / Unique | The alternate key has no duplicates (nulls are left out and counted in `detail`) | A business key is repeated. |
| **Reconciliation** / Full row: STG vs TGT | Every mapped column, row by row, in both directions. STG values are cast to the TGT type and compared as text. | Rows in STG only, or in TGT only. Samples show them. A transformed column without an `expr` in the mapping reports every row as different. |
| **Reconciliation** / Key: RAW vs TGT | The set of keys is the same in RAW and TGT | Keys lost or created. With rejects, rejected RAW keys show as "in RAW only". |
| **Reconciliation** / Aggregates: STG vs TGT | sum (numbers only), min, max and count of the columns marked `agg`, STG vs TGT | A value changed on the way (rounding, truncation). |
| **Column Mapping** / Columns present | Every mapped column exists in TGT and STG | A column was renamed, dropped or mistyped in the mapping. |
| **Column Mapping** / Unmapped TGT columns (warning) | TGT has columns that are neither mapped nor audit columns | The mapping is missing a column, or it is a new column. |
| **Column Mapping** / Data types | TGT type matches the mapping | A column was created with a different type. |
| **Column Mapping** / Lengths | Declared length matches, and no stored value is longer than the mapped length | Table definition differs from the mapping, or the data is too long. |
| **Column Mapping** / Length declared in catalog (warning) | The catalog gives no length (common for `STRING`) | Only the data was checked. Not a defect. |
| **Nullability and Defaults** / Default values | The declared default matches the mapping. Rows with no source value hold the default in TGT (without `stg` or keys in the mapping: the column has no nulls) | The default was not applied. |
| **Nullability and Defaults** / Declared nullability vs mapping (warning) | The catalog's NOT NULL setting matches the mapping | The table definition differs from the mapping. |
| **Nullability and Defaults** / Mandatory columns populated | Mandatory columns have no nulls in TGT | Nulls in a required column. |
| **Data Quality** / your rule name | Your own rules: SQL, accepted values, regex, spaces, not empty, A < B, range | The data breaks the rule. Samples show the rows. A rule can allow a number of exceptions with `max_violations`. |
| **Audit Columns** / Present and populated | The audit columns exist, have no nulls, and timestamps are not in the future | A missing or empty audit column, or a bad clock. |
| **Setup** | Anything that could not run (see below) | Fix the cause and re-run. |

### 6.4 Setup rows
Setup rows are about the notebook not being able to do its job, not about your data:

| Setup row | Meaning | What to do |
|---|---|---|
| *Config settings* (FAIL) | A mistake in the entity's config, such as a table name that is not three parts or key lists of different lengths | Fix the config. The entity did not run. |
| *Config settings* (WARN) | An unknown setting was ignored | Usually a typo in a setting name. |
| *Load tables* (FAIL) | A table could not be read: no permission, a bad `filters` expression, a missing SQL file, an unfilled `${placeholder}`, or a file that is not a single SELECT | Read `detail`. |
| *Skipped: tables not found* (SKIPPED) | A table, schema or catalog does not exist | Expected for entities added early. If you expected it to run, check the spelling of the name in `detail`. |
| *Unknown entity* (FAIL) | A name in `entities` is not in the config | A typo. Names are case-sensitive. |
| *TGT column metadata* (WARN) | `information_schema` has no rows, or could not be read, for TGT | The data type, declared length, declared nullability and declared default were not compared (and the declared nullability row is left out). The data checks still ran: longest value, mandatory columns populated, default values in the data. Check your access. |
| *Error in …* (FAIL) | One check or rule hit an error, for example a rule with an unknown operator or no rule type | The other checks still ran. Read `detail`. |
| *Unexpected error* (FAIL) | Something unforeseen stopped one entity | The other entities still ran. Read `detail`. |
| *RAW source* (PASS) | Which SQL file was used, and when it was last changed | Check it if a result looks out of date. |

### 6.5 A small example

An entity with a duplicate key in STG and a default that was not applied would look like this in the summary:

| entity | category | status | checks | passed | failed | warnings | skipped |
|---|---|---|---|---|---|---|---|
| customer | Primary Key | FAIL | 6 | 5 | 1 | 0 | 0 |
| customer | Nullability and Defaults | FAIL | 3 | 2 | 1 | 0 | 0 |
| orders | Setup | SKIPPED | 1 | 0 | 0 | 0 | 1 |
| customer | Record Count | PASS | 2 | 2 | 0 | 0 | 0 |

The *All checks* list then tells you exactly which: for example *Unique (STG)* with `expected 0`, `actual 3`, and the sample rows list the three duplicated keys.

## 7. Getting results into your Excel tracker

Excel Online cannot read a Databricks folder directly, so the file is moved by hand:
1. Download `sit_results_latest.csv` (or the timestamped file) from your `output_dir` in Databricks.
2. Upload it to a SharePoint folder.
3. In the tracker: **Data → Get Data → From Text/CSV**, pick the file, **Close & Load**. After uploading a newer file, use **Data → Refresh All**.

To keep a history, upload each run's timestamped CSV to one SharePoint folder and use **From SharePoint Folder** to combine them. Check that refresh works in your tenant. This part has not been tested.

## 8. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| An entity I expected to run shows `SKIPPED` | A table name is misspelt, or the table is not created yet. `detail` names the table it could not find. |
| The job is green but there are failures | That is by design with `fail_run_on = setup`. Read the summary, or set `fail_run_on = any` for a job that goes red on any failure. |
| The job is red | Look at the *Setup* rows. Something could not run. |
| Everything is a mismatch in *Full row: STG vs TGT* | A column is transformed between STG and TGT but the mapping has no `expr`. Add `expr`, or set `recon` to `false` for that column. |
| Type, length or default checks are missing | *TGT column metadata* warning: no access to `information_schema`, or the table is not in it. |
| "no value in 'params'" | The SQL file contains `${name}` and the config's `params` has no value for it. |
| "the file must hold one SELECT" | The SQL file starts with `INSERT`, `CREATE`, `USE` or similar. Upload a copy with only the `SELECT`. |
| "SQL file could not be opened" | Wrong path, or the upload turned the file into a notebook. Use the absolute path and check the `.sql` extension. |
| The results look stale after the SQL changed | The uploaded SQL is a copy. Re-upload it and check the *RAW source* row's date. |
| A run is slow | Each check reads the tables again, and the target is scanned about a dozen times per entity. Run fewer entities at a time with the `entities` widget. |
| CSV did not appear | `output_dir` was blank, or writing failed. A failed write is reported in the one-line summary and does not stop the run. |

## 9. What it does not do yet

- **Incremental loads.** There is no batch filter, so each run checks whole tables. Batch filtering, SCD2 history checks (from < to, no overlaps, one current row per key) and freshness checks are planned for when incremental loads start. For SCD2 targets you can already compare only current rows with `"filters": {"tgt": "is_current = true"}`.
- **Referential integrity.** Out of scope.
- **History.** Nothing is stored in Delta. Keep the timestamped CSV files if you want a record of past runs.
- **Alerts.** None. You read the results.

## 10. Known limits

- It was tested on a local Spark setup (classic and Spark Connect) that imitates Unity Catalog, and not yet on a real Databricks workspace. The first real run may show differences in what `information_schema` returns (lengths, defaults), in file paths for saving results, or in error messages used to detect missing tables.
- Reading a SQL file or config file from `/Workspace` or `/Volumes` paths with ordinary file access is expected to work on serverless but is unconfirmed.
- Large tables take longer. The target is read many times per entity.
- `char(n)` columns may show differences in the full row comparison if Spark pads the values, and timestamp audit columns without a time zone are judged against the notebook's clock. Neither has been seen on a real workspace; check them on the first run if you use those column types.
- With `rejects` set, *Key: RAW vs TGT* reports rejected RAW keys as "in RAW only", so it fails on every run that has rejects.
- A mapping file exported from a spreadsheet may hold text such as `"N"` where the config needs `true` or `false`. The notebook reports that as a config error.

## 11. Quick reference

| I want to… | Do this |
|---|---|
| Check one entity | Set `entities` to its name |
| Check everything in my file | Leave `entities` blank |
| Use my own settings | Point `config_path` at your own file |
| Save results | Set `output_dir` to a folder of your own |
| Make the job go red on any failure | Set `fail_run_on` to `any` |
| Add an entity before its tables exist | Add it. It shows `SKIPPED` until they exist. |
| Compare only current SCD2 rows | `"filters": {"tgt": "is_current = true"}` |
| Allow some bad rows in a rule | `"max_violations": N` on the rule |
