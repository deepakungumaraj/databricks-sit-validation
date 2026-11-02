# Config file reference

The config tells the notebook which RAW, STG and TGT tables belong together, how STG columns map to TGT columns, and which extra checks to run. This page describes every setting and which checks it switches on.

A complete example is in [example_config.json](example_config.json).

## Where the config comes from
- **Config file (normal use):** set the `config_path` widget (or job parameter) to a `.json`, `.yaml` or `.yml` file. The file must be readable by the user or identity running the notebook, for example under `/Workspace/Users/<you>/`.
- **Inline:** if `config_path` is blank, the notebook uses the `CONFIG` dictionary in its second code cell.
- `config_path` takes **one file**. Loading a whole folder is not built yet.

## File structure
The top level is an object. Each key is an **entity name** and each value holds that entity's settings.

```json
{
  "customer": { "raw": "...", "stg": "...", "tgt": "...", "...": "..." },
  "orders":   { "raw": "...", "stg": "...", "tgt": "..." }
}
```

- The entity name is a free-text label. It appears in the results and is what you list in the `entities` widget (comma separated). A blank `entities` runs every entity in the file.
- JSON needs lowercase `true`, `false` and `null`, double quotes, and no trailing commas. YAML allows comments, so it is easier to maintain by hand.

## Entity settings at a glance
| Setting | Required | What it is |
|---|---|---|
| `raw`, `stg`, `tgt` | Yes | Full table name in each layer, as `catalog.schema.table`. `raw` can also be a query that joins several tables |
| `filters` | No | A SQL filter per layer |
| `keys` | No | Primary key columns per layer |
| `alt_keys` | No | Alternate (unique) keys per layer |
| `rejects` | No | Tables holding rejected rows |
| `columns` | No | The STG to TGT column mapping |
| `audit_cols` | No | Audit columns on TGT |
| `dq` | No | Data quality rules |

A check only runs when the settings it needs are present. With only `raw`, `stg` and `tgt`, you get just the record counts.

## Table names: `raw`, `stg`, `tgt`
Each layer has its own catalog, schema and table, so each is written in full:

```json
"raw": "RAW.crm.customer",
"stg": "STG.crm.customer_stg",
"tgt": "CNFZ.core.dim_customer"
```

- Always three parts separated by dots. Do not add backticks. The notebook quotes the names itself.
- The user running the notebook needs read access to all three tables (every table used, when `raw` is a query), and to `information_schema` in the TGT catalog.

## `raw` built from several tables (a query)
When STG is built by joining (or unioning) several RAW tables, give `raw` as a query instead of a table name. The query result is then treated as the RAW data for that entity.

```json
"raw": {
  "sql_file": "/Workspace/Users/you@company.com/sit_sql/orders_join.sql",
  "params": {"env": "dev"}
},
"keys": {"raw": ["order_id"], "stg": ["order_id"], "tgt": ["order_id"]}
```

| Setting | What it is |
|---|---|
| `sql_file` | Path to a `.sql` file you uploaded to the workspace (`/Workspace/...`) or a volume (`/Volumes/...`). The file holds one query |
| `sql` | The query written directly in the config, as text. Use this or `sql_file`, not both |
| `params` | Optional. Each `${name}` in the query is replaced with the value given for `name` |

- **Use full table names in the query**, for example `FROM RAW.sales.orders o JOIN RAW.crm.customer c ON ...`.
- **The file must hold one plain `SELECT` (or `WITH ... SELECT`) and nothing else.** The notebook runs it as a sub-query, so `INSERT`, `CREATE`, `MERGE`, `USE` or `DROP` never run. A file that starts with one of those fails in Setup (*Load tables*) with a message to upload a copy that holds only the `SELECT` part. This matters if the ETL file is an `INSERT ... SELECT` or `CREATE TABLE AS SELECT`. Two statements in one file also fail. Trailing blank lines, whole-line `--` comments and `;` at the end are removed, so a closing `-- end of file` after the `;` is fine, but a comment on the same line as the `;`, or a `/* */` comment after it, is not.
- **Save the file as UTF-8** (a byte order mark is fine). Other encodings, such as the Notepad "Unicode" option, fail with a message saying so.
- **Give `sql_file` as an absolute path to the uploaded file.** A relative path is read from the notebook's working directory, not from the config file's folder. When you upload a file, check that it keeps its `.sql` extension and has not been turned into a notebook.
- **Only `${name}` placeholders are replaced.** `:name` markers and `{{name}}` are passed to Spark as they are and will fail. A `${name}` inside a comment also needs a value in `params`.
- **Name every output column, and avoid duplicates.** Two columns both called `id` make any check that uses `id` fail with an ambiguous reference. Give them aliases.
- **`keys.raw`, `filters.raw` and `dq` rules with layer `raw` refer to the columns the query returns.** `filters.raw` is applied on top of the query result.
- **Placeholders must all have a value.** If the query still contains a `${name}` with no value in `params`, the entity fails in Setup (*Load tables*) and tells you which one.
- **The notebook shows which file ran.** Each entity whose `raw` is a query and which loaded gets a Setup / *RAW source* row with the file path and when the file was last changed (UTC), or "inline query from the config". Check it if a result looks out of date: an uploaded file does not follow changes made to the original, so upload it again when the original changes.
- **How the checks read the query.** The RAW vs STG count compares the number of rows the query returns. Duplicates caused by a join show up as a failed *Primary Key: Unique (RAW)*, which needs `keys.raw` to be set. Rows dropped by an inner join show up as a count mismatch. The query runs again for each check that reads RAW, which is fine at SIT volumes.
- **What "read only" covers.** The `raw` query, and the queries in `dq` rules with `sql`, are run as sub-queries, so statements that change data fail to parse and never run. A `SELECT` can still call functions with side effects, so keep to plain queries.
- If a table used in the query does not exist yet, the entity is skipped (see below), and the detail names the missing table.
- Only `raw` can be a query. `stg`, `tgt` and `rejects` are always table names.

If this query is a copy of the ETL's own join, the RAW checks only prove the load matches that join. They still catch failed or truncated loads, rejected rows and duplicates, but not a join that was wrong to begin with.

## Entities whose tables do not exist yet
You can add an entity to the config before its tables are created. If its RAW, STG or TGT table, a table used in a `raw` query, or a table named in `rejects`, does not exist (the table, its schema or its catalog cannot be found), the entity is **skipped**:
- It gets one result row, Setup / *Skipped: tables not found*, with status `SKIPPED`. The `detail` column names each missing table (layer and full name) and lists the tables that were found.
- No other checks run for that entity, and the notebook carries on with the next one. If every entity is skipped, the run still ends normally.
- `SKIPPED` is not a failure or a warning, so it never marks the run as failed, whatever `fail_run_on` is set to. Skipped rows are counted separately in the summary (`skipped`).
- Any other problem loading a table, such as missing permissions or a `filters` expression that does not work, is still a Setup failure (*Load tables*), because it will not fix itself when the table is created.
- A name in the `entities` widget that is not in the config at all is still a Setup failure (*Unknown entity*), as it is usually a typo.

## `filters`
A SQL expression applied to a layer after it is read. Every check for that entity then sees only the filtered rows.

```json
"filters": {"tgt": "is_current = true"}
```

- Keys are `raw`, `stg` and `tgt`. Use any of them.
- Typical use: compare only the current rows of a history (SCD2) target.
- The filter applies to the row counts too, so apply matching filters to both sides if you want like-for-like counts.
- `rejects` tables are not filtered.

## `keys` (primary keys)
Primary key columns for each layer. Lists are matched by **position** across layers, so `raw[0]` corresponds to `stg[0]` and `tgt[0]`.

```json
"keys": {"raw": ["cust_id"], "stg": ["customer_id"], "tgt": ["customer_id"]}
```

What it enables:
- **Primary Key** checks: for every layer you list, no nulls (*Not null*) and no duplicates (*Unique*).
- **Key: RAW vs TGT** (needs `raw` and `tgt`): distinct keys present in one layer but not the other, in both directions. RAW values are cast to the TGT key type first, so `'001'` in RAW matches `1` in TGT.
- **Default values** data check (needs `stg` and `tgt`): used to join STG to TGT.

Composite keys list several columns in the same order in every layer.

## `alt_keys`
Alternate keys that must also be unique. For each layer, a list of keys, and each key is a list of columns.

```json
"alt_keys": {"tgt": [["email"], ["account_no", "region"]]}
```

- Rows where any key column is null are left out of the duplicate test. The number of such rows is shown in the `detail` column.
- One result row per key.

## `rejects`
Tables holding rows rejected during a load. They are used only in the record counts. If a rejects table does not exist yet, the entity is skipped (see [Entities whose tables do not exist yet](#entities-whose-tables-do-not-exist-yet)).

```json
"rejects": {"stg": "STG.crm.customer_rejects", "tgt": "CNFZ.core.customer_rejects"}
```

- `stg`: rows rejected going RAW to STG. The count rule becomes RAW = STG + rejected.
- `tgt`: rows rejected going STG to TGT. The count rule becomes STG = TGT + rejected.
- All rows in the rejects table are counted, so it must hold **only the rows rejected by the load being checked**. If it keeps history from earlier loads, RAW vs STG will fail from the second load on. It only counts rows. It does not check that the rejected rows were really bad or that they are absent from STG or TGT.
- **Key: RAW vs TGT** does not subtract rejected keys, so rejected RAW keys show up as "in RAW only". The `detail` column notes this when `rejects` is set.

## `columns` (the mapping)
A list with one object per **TGT column**. It drives the column, data type, length, nullability, default, full row and aggregate checks.

STG values are cast to the TGT column's type before they are compared with TGT (values that do not fit become null), so a text STG column holding `"1.5"` matches a `decimal(10,2)` TGT column holding `1.50`.

```json
"columns": [
  {"stg": "customer_id", "tgt": "customer_id", "type": "bigint", "nullable": false},
  {"stg": "customer_name", "tgt": "customer_name", "type": "varchar(100)", "nullable": false,
   "expr": "upper(trim(customer_name))"},
  {"stg": "status", "tgt": "status", "type": "string", "default": "A"},
  {"stg": "credit_limit", "tgt": "credit_limit", "type": "decimal(10,2)", "agg": true}
]
```

| Field | Required | Meaning |
|---|---|---|
| `tgt` | Yes | The TGT column name |
| `stg` | No | The STG source column. Leave out if the column is derived in TGT |
| `type` | No | Expected TGT data type |
| `length` | No | Expected maximum length |
| `nullable` | No | `false` means mandatory. `true` means nulls are allowed. Must be a real `true` or `false`, not text such as `"N"` |
| `default` | No | Expected default value |
| `recon` | No | `false` leaves the column out of the full row comparison |
| `expr` | No | Spark SQL on STG columns giving the expected TGT value |
| `agg` | No | `true` compares sum, min, max and non-null count |

Column names are matched without regard to case. `nullable`, `recon` and `agg` must be a real `true` or `false`; text such as `"N"` or `"false"` is a config error, because it would otherwise count as true.

A mapped column that is missing from TGT (or from STG) is reported once, by *Columns present*. The other checks leave that column out, so a renamed column does not also turn the run red.

### `type`
Compared with the TGT column's type in `information_schema`.
- Case and spaces are ignored. These aliases are treated as equal: `integer`=`int`, `long`=`bigint`, `short`=`smallint`, `byte`=`tinyint`, `numeric`=`decimal` (so `numeric(10,2)` matches `decimal(10,2)`), `real`=`float`.
- `varchar(n)` and `char(n)` are treated as `string`. The length is checked separately.
- Give the full type for decimals, for example `decimal(10,2)`. A plain `decimal` will not match `decimal(10,2)`.

### `length`
- Give it as `"length": 100`, or write it in the type, for example `"type": "varchar(100)"`.
- Two checks run: the declared length in the catalog must equal the mapped length, and the longest value in TGT must not exceed it.
- `STRING` columns have no declared length, and the catalog reports 0 or nothing. In that case you get a warning, *Length declared in catalog*, and only the data is checked. The data check also runs when the catalog returns nothing for the table at all.

### `nullable`
- `false` (mandatory): TGT must contain no nulls in the column. This is a **failing** check (*Mandatory columns populated*).
- The mapping is also compared with what the catalog declares (`NOT NULL` or nullable). A mismatch is only a **warning** (*Declared nullability vs mapping*), because Delta columns are nullable unless someone declared otherwise.
- Leave it out to skip both checks for that column.

### `default`
- If the catalog has a declared default for the column, it must match (quotes and case are ignored, and numbers are compared as numbers, so `0` matches `0.00`).
- Data check: with `stg` and keys on both layers, rows with a null STG value must hold the default in TGT. Without `stg` or the keys, the column must contain no nulls. The default is cast to the TGT type first. A default that does not fit the type, such as `"N/A"` on a number column, cannot match, so rows that should hold it are reported.
- A defaulted column also takes part in the full row comparison, where a null STG value will not equal the default in TGT. Give the column an `expr` such as `coalesce(status, 'A')`, or set `recon` to `false`.

### `expr`
Use it when a column is transformed between STG and TGT, so the comparison uses the expected result and not the raw STG value.
- It is Spark SQL over STG column names, for example `upper(trim(customer_name))` or `concat(first_name, ' ', last_name)`.
- It is used in the **full row** comparison and in `agg`.
- With `expr`, `stg` can be left out.

### `recon`
Set `"recon": false` for columns you expect to differ and that have no `expr`, such as a column filled by a sequence in TGT. They stay in the other checks.

### `agg`
Compares `sum`, `min`, `max` and the non-null count of the STG column (or `expr`) with the TGT column. STG values are cast to the TGT type first. The sum is only used for number columns; dates, timestamps and text get `min`, `max` and the count. The values must be equal, except that `float` and `double` results allow a tiny tolerance.

### What the mapping enables
- **Columns present:** every mapped TGT column exists in TGT, and every mapped STG column exists in STG.
- **Unmapped TGT columns (warning):** TGT columns that are neither mapped nor listed in `audit_cols`.
- **Data types** (only when at least one column has a `type`), **Lengths**, **Declared nullability vs mapping**, **Mandatory columns populated**, **Default values**.
- **Full row: STG vs TGT:** all mapped columns that have `stg` or `expr`, and `recon` not set to `false`. Values are cast to strings and compared in both directions with duplicates counted. Nulls compare equal.
- **Aggregates: STG vs TGT:** columns with `agg`.

## `audit_cols`
A list of TGT audit columns.

```json
"audit_cols": ["created_ts", "updated_ts", "batch_id"]
```

Each must exist in TGT and contain no nulls. Columns of type `timestamp`, `timestamp_ntz` or `date` must also not hold values in the future, judged against the notebook's clock. Audit columns are not part of the full row comparison, and they do not need to appear in `columns`.

## `dq` (data quality rules)
A list of rules. Every rule can have:
- `name`: shown in the results. Defaults to `rule1`, `rule2` and so on.
- `layer`: `raw`, `stg` or `tgt`. Defaults to `tgt`.
- `max_violations`: how many bad rows are allowed. Defaults to 0.

Each rule also needs **one** of these:

| Rule | Example | Violation |
|---|---|---|
| `sql` | `{"sql": "SELECT * FROM {t} WHERE amount < 0"}` | Every row the query returns. `{t}` stands for the layer's table |
| `accepted_values` | `{"col": "status", "accepted_values": ["A","I","C"]}` | A non-null value not in the list |
| `regex` | `{"col": "email", "regex": "^[^@]+@[^@]+$"}` | A non-null value that does not match |
| `no_leading_trailing_space` | `{"cols": ["name","city"], "no_leading_trailing_space": true}` | A value starting or ending with whitespace. Nulls pass |
| `not_empty` | `{"cols": ["name"], "not_empty": true}` | A null, empty or blank value |
| `compare` | `{"compare": ["start_date","<","end_date"], "nulls": "ignore"}` | Rows where the comparison is false |
| `range` | `{"col": "age", "range": [0, 120]}` | A non-null value outside the range, inclusive |

Notes:
- `cols` takes a list of columns. `col` takes one. Either works for `no_leading_trailing_space` and `not_empty`.
- `compare` operators: `<`, `<=`, `>`, `>=`, `=`, `<>`. With `"nulls": "ignore"` (the default), rows where either column is null are skipped. With `"nulls": "fail"`, those rows count as violations.
- For `sql` rules, write a query that returns the **bad** rows. A query that returns nothing passes. It must be one `SELECT`; a trailing `;` is removed.
- `regex` finds a match anywhere in the value, so `[0-9]{5}` also accepts `ab12345cd`. Anchor the pattern with `^` and `$` to match the whole value.
- For `range`, use `null` for an open end: `[0, null]` means "0 or more".
- The result shows `expected <= max_violations` and the number found. The first 20 violating rows of a failed rule are shown in the notebook and written to the Excel file.
- If a rule itself errors (for example it names a column that does not exist, uses an unknown `compare` operator, or has no rule type), that one rule is reported under Setup and the other rules still run.

## Which settings switch on which checks
| Check category | Needs |
|---|---|
| Record Count | `raw`, `stg`, `tgt` (and `rejects` if rows are rejected) |
| Primary Key | `keys` (any layers listed) |
| Alternate Key | `alt_keys` |
| Reconciliation: Key RAW vs TGT | `keys.raw` and `keys.tgt` |
| Reconciliation: Full row STG vs TGT | `columns` with `stg` or `expr` |
| Reconciliation: Aggregates | `columns` with `agg` |
| Column Mapping | `columns` (`type` and `length` for types and lengths) |
| Nullability and Defaults | `columns` with `nullable` or `default` |
| Data Quality | `dq` |
| Audit Columns | `audit_cols` |

## Common mistakes
- **Keys in a different order across layers.** `raw`, `stg` and `tgt` key lists are matched by position, so a composite key listed in a different order compares the wrong columns.
- **Backticks or brackets in names.** Write plain names.
- **A plain `decimal` type** against a `decimal(10,2)` column.
- **Mapping a transformed column with no `expr`.** The full row check will report every row as different. Add `expr` or set `recon` to `false`.
- **Setting a length on a non-text column.** The length is checked against the text form of the value, so a decimal such as `12345678.90` with `length: 10` fails.
- **A JSON syntax error.** One missing comma stops the whole file from loading, so validate it before a run.
- **Case.** Entity names are case-sensitive in the `entities` widget, and a name that is not in the config is reported as a Setup failure. Column names are not case-sensitive.
- **A misspelt table name**, including one inside a `raw` query. It looks the same as a table that does not exist yet, so the entity is reported as `SKIPPED`, not failed. Check the `detail` of any skipped entity you expected to run.
- **A wrong `sql_file` path.** This is reported as a Setup failure (*Load tables*), not a skip, with the reason the file could not be opened.
- **A defaulted column with no `expr`.** Rows where STG is null and TGT holds the default show up in the full row comparison.
- **Unquoted `yes`, `no`, `on`, `off` in YAML.** YAML turns them into booleans, so write `default: "NO"` with quotes.

## Not supported yet
These are planned for incremental loads: batch filtering, history (SCD2) checks, and freshness checks. Referential integrity (foreign key) checks are out of scope for now.
