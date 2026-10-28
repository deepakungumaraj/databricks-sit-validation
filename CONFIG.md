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
| `raw`, `stg`, `tgt` | Yes | Full table name in each layer, as `catalog.schema.table` |
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
- The user running the notebook needs read access to all three tables, and to `information_schema` in the TGT catalog.

## Entities whose tables do not exist yet
You can add an entity to the config before its tables are created. If its RAW, STG or TGT table, or a table named in `rejects`, does not exist (the table, its schema or its catalog cannot be found), the entity is **skipped**:
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
| `nullable` | No | `false` means mandatory. `true` means nulls are allowed |
| `default` | No | Expected default value |
| `recon` | No | `false` leaves the column out of the full row comparison |
| `expr` | No | Spark SQL on STG columns giving the expected TGT value |
| `agg` | No | `true` compares sum, min, max and non-null count |

Column names are matched without regard to case.

### `type`
Compared with the TGT column's type in `information_schema`.
- Case and spaces are ignored. These aliases are treated as equal: `integer`=`int`, `long`=`bigint`, `short`=`smallint`, `byte`=`tinyint`, `numeric`=`decimal`, `real`=`float`.
- `varchar(n)` and `char(n)` are treated as `string`. The length is checked separately.
- Give the full type for decimals, for example `decimal(10,2)`. A plain `decimal` will not match `decimal(10,2)`.

### `length`
- Give it as `"length": 100`, or write it in the type, for example `"type": "varchar(100)"`.
- Two checks run: the declared length in the catalog must equal the mapped length, and the longest value in TGT must not exceed it.
- `STRING` columns have no declared length, and the catalog reports 0 or nothing. In that case you get a warning, *Length declared in catalog*, and only the data is checked.

### `nullable`
- `false` (mandatory): TGT must contain no nulls in the column. This is a **failing** check (*Mandatory columns populated*).
- The mapping is also compared with what the catalog declares (`NOT NULL` or nullable). A mismatch is only a **warning** (*Declared nullability vs mapping*), because Delta columns are nullable unless someone declared otherwise.
- Leave it out to skip both checks for that column.

### `default`
- If the catalog has a declared default for the column, it must match (quotes and case are ignored).
- Data check: with `stg` and keys on both layers, rows with a null STG value must hold the default in TGT. Without `stg` or the keys, the column must contain no nulls. Numeric and boolean defaults are compared after casting to the TGT type, so `0` matches `0.00` and `false` matches `false`.
- A defaulted column also takes part in the full row comparison, where a null STG value will not equal the default in TGT. Give the column an `expr` such as `coalesce(status, 'A')`, or set `recon` to `false`.

### `expr`
Use it when a column is transformed between STG and TGT, so the comparison uses the expected result and not the raw STG value.
- It is Spark SQL over STG column names, for example `upper(trim(customer_name))` or `concat(first_name, ' ', last_name)`.
- It is used in the **full row** comparison and in `agg`.
- With `expr`, `stg` can be left out.

### `recon`
Set `"recon": false` for columns you expect to differ and that have no `expr`, such as a column filled by a sequence in TGT. They stay in the other checks.

### `agg`
Compares `sum`, `min`, `max` and the non-null count of the STG column (or `expr`) with the TGT column. STG values are cast to the TGT type first. The values must be equal, except that `float` and `double` results allow a tiny tolerance.

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
- For `sql` rules, write a query that returns the **bad** rows. A query that returns nothing passes.
- The result shows `expected <= max_violations` and the number found. The first 20 violating rows of a failed rule are shown in the notebook and written to the Excel file.
- If a rule itself errors (for example it names a column that does not exist), that one rule is reported under Setup and the other rules still run.

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
- **A misspelt table name.** It looks the same as a table that does not exist yet, so the entity is reported as `SKIPPED`, not failed. Check the `detail` of any skipped entity you expected to run.
- **A defaulted column with no `expr`.** Rows where STG is null and TGT holds the default show up in the full row comparison.
- **Unquoted `yes`, `no`, `on`, `off` in YAML.** YAML turns them into booleans, so write `default: "NO"` with quotes.

## Not supported yet
These are planned for incremental loads: batch filtering, history (SCD2) checks, and freshness checks. Referential integrity (foreign key) checks are out of scope for now.
