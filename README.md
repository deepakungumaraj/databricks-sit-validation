# Databricks SIT validation (RAW -> STG -> TGT)

A single Databricks notebook, `sit_validation.py` (also provided as `sit_validation.ipynb`), that runs by hand after each load and compares the RAW, STG and TGT (CNFZ) layers for the tables you list in a config file.

Checks, in the order they appear in the results:
- **Record Count**: RAW vs STG vs TGT, allowing for rejected rows
- **Primary Key**: not null and unique, per layer
- **Alternate Key**: unique
- **Reconciliation**: full row STG vs TGT, key RAW vs TGT, aggregates (sum, min, max, count)
- **Column Mapping**: columns present, data types, lengths
- **Nullability and Defaults**: declared nullability, mandatory columns populated, default values
- **Data Quality**: your own rules (SQL, accepted values, regex, spaces, not empty, A < B, range)
- **Audit Columns**: present, populated, not in the future
- **Setup**: anything that could not run (a missing table, a config mistake, an error in one rule)

Nothing is saved to Delta tables. Results are shown in the notebook and can be written to CSV and Excel files.

## Running it
**As a Databricks job (normal use).** Create a job with one **Notebook** task on serverless compute. Add these job parameters, whose names match the notebook's widgets:

| Parameter | Meaning |
|---|---|
| `config_path` | Path to your config file (`.json`, `.yaml` or `.yml`). See [CONFIG.md](CONFIG.md) |
| `entities` | Blank for everything in your file, or a comma-separated subset |
| `output_dir` | A folder of your own for the result files, so `sit_results_latest.csv` is not overwritten by someone else. Leave blank to write nothing |
| `fail_run_on` | `setup` (default): the run fails only if something could not run. `never`: the run never fails. `any`: the run fails if any check failed |

One or two failed checks are expected in SIT, so by default they do not mark the run as failed. The run ends with a one-line summary such as `SIT completed: 118 passed, 2 failed, 1 warnings across 6 entity(ies)`.

**Interactively.** Open the notebook, set the same widgets, and run all cells. Each person uses their own config file.

## Results
The notebook shows a summary by category and a full list of checks, failures first, then up to 20 sample rows for each failed reconciliation or data quality check. If `output_dir` is set it also writes:
- `sit_results_<time>.csv`: every check, with `run_time_utc` and `run_by` columns
- `sit_results_latest.csv`: the same, overwritten each run, so Excel can point at one file name
- `sit_results_<time>.xlsx`: Run, Summary and Results sheets, plus one sheet per set of sample rows

A problem writing the files does not stop the run. It is added to the one-line summary.

### Getting the results into an Excel tracker (Excel Online)
Excel Online cannot reach a Databricks folder, so the file has to be moved by hand:
1. Download `sit_results_latest.csv` (or the timestamped one) from the `output_dir` folder in the Databricks workspace or Volume.
2. Upload it to a SharePoint folder.
3. In Excel Online, use **Data → Get Data → From Text/CSV**, pick the file and **Close & Load**. After uploading a newer file, use **Data → Refresh All**.

If you upload every run's timestamped CSV to one SharePoint folder, **From SharePoint Folder** can combine them into a history. Check that refresh works in your tenant, as it depends on your Excel Online setup.

## Config
Every config setting is described in [CONFIG.md](CONFIG.md), and [example_config.json](example_config.json) is a working example.

## Planned for incremental loads (not built yet)
- Batch filtering: a batch column per layer plus a batch value widget, so one load can be validated on its own.
- SCD2 history checks: from < to, no overlapping periods, exactly one current row per key.
- Freshness: the latest load timestamp in TGT is within an agreed number of hours.
