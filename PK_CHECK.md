# Primary key check

`pk_check.py` (also provided as `pk_check.ipynb`) is a small Databricks notebook that answers one question: does the primary key of a table hold, meaning it is never null and never repeated. It is separate from `sit_validation.py` and does not need a config file for a single table.

## Inputs
Set these as notebook widgets, or as job parameters with the same names.

| Widget | Meaning |
|---|---|
| `table` | Full name, `catalog.schema.table` |
| `key_columns` | Key columns, comma separated, for example `order_id,line_no`. One column or several |
| `filter` | Optional SQL condition applied before checking, for example `is_current = true` for an SCD2 table |
| `config_path` | Optional `.json` file for several tables, on a path Python can open, such as `/Workspace/...` or `/Volumes/...`. When set, the three widgets above are ignored |

Column names are matched without regard to case. If a key column is not in the table, the run stops with a message that lists the table's real columns. Names are trimmed of spaces and surrounding backticks. A column name that contains a comma needs the JSON list form, and a backtick inside a column name is not supported.

Config file format, one object per table (`filter` is optional, `key_columns` can be a list or a comma separated string):

```json
[
  {"table": "cnfz.core.fact_order_line", "key_columns": ["order_id", "line_no"]},
  {"table": "cnfz.core.dim_customer", "key_columns": "customer_id", "filter": "is_current = true"}
]
```

## What is counted
- **Not null**: a row fails if any key column is null. The result shows the number of such rows, and a null count per key column so you can see which column is the cause.
- **Unique**: the key is all key columns taken together, so `(1, 2)` and `(2, 1)` are different keys. Rows with a null in any key column are left out of this count, because they are already reported under not null and one problem should not be reported twice. `NaN` in a float column is a value, not a null, so it is counted as a normal key value.
- **Duplicate keys**: the number of key values that appear more than once. **Extra rows**: the rows beyond the first for each of those values. A key that appears 3 times counts as 1 duplicate key and 2 extra rows.
- Key columns of type map or variant cannot be grouped, so they raise an error. The table is scanned a few times (nulls, duplicates, samples), with no caching.
- Each check shows `PASS` (count is 0) or `FAIL`. An empty table, or a filter that matches no rows, gives `PASS` with `rows_checked` 0.
- When uniqueness fails, up to 20 duplicate keys are shown with their row counts, most repeated first.

## Outputs
- A summary table with one row per table: `rows_checked`, `not_null`, `rows_with_null_key`, `unique`, `duplicate_keys`, `extra_rows`.
- A table of null counts per key column.
- The duplicate key samples, one table per table that failed uniqueness.
- A one-line result from `dbutils.notebook.exit`, for example `PK check completed: 2 table(s) checked, 1 passed, 1 failed, 0 error(s)`.

A failed check does not fail the run. Only a real error does: a missing table or column, no access, a malformed table name, an empty key list or a filter that is not valid SQL. When several tables are listed, the other tables are still checked and shown, and the run is failed at the end with every error message. Nothing is written to Delta tables or files.

## Example
Input: `table` = `cnfz.core.fact_order_line`, `key_columns` = `order_id,line_no`.

| table | key_columns | rows_checked | not_null | rows_with_null_key | unique | duplicate_keys | extra_rows |
|---|---|---|---|---|---|---|---|
| cnfz.core.fact_order_line | order_id, line_no | 6 | PASS | 0 | FAIL | 2 | 3 |

Duplicate keys:

| order_id | line_no | rows_with_this_key |
|---|---|---|
| 1 | 2 | 3 |
| 3 | 4 | 2 |

## Testing status
Checked locally with PySpark 4.0.1, in classic mode and in Spark Connect mode, with the tables replaced by temporary views. It has not been run on a Databricks workspace, so these are not confirmed there: reading Unity Catalog tables with your credentials, `display()` of the result tables, and `dbutils.notebook.exit` in a job.
