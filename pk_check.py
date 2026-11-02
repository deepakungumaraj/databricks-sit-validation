# Databricks notebook source
# MAGIC %md
# MAGIC # Primary key check: not null and unique
# MAGIC Give a table and its primary key columns (one or several). The notebook counts the rows with a null in any key column and the key values that appear more than once, and shows PASS or FAIL for each.
# MAGIC
# MAGIC A failed check does not fail the run. Only a real error (a missing table or column, a bad input) does. Nothing is saved. Details: `PK_CHECK.md`.

# COMMAND ----------

dbutils.widgets.text("table", "")         # catalog.schema.table
dbutils.widgets.text("key_columns", "")   # comma separated, e.g. order_id,line_no
dbutils.widgets.text("filter", "")        # optional SQL condition, e.g. is_current = true
dbutils.widgets.text("config_path", "")   # optional .json file listing several tables; replaces the three widgets above

TABLE = dbutils.widgets.get("table").strip()
KEY_COLUMNS = dbutils.widgets.get("key_columns")
FILTER = dbutils.widgets.get("filter").strip()
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

# COMMAND ----------

import json

import pandas as pd
from pyspark.sql import functions as F

# What to check: a list of {"table", "key_columns", "filter"}; filter is optional.
# key_columns is a list or a comma separated string.
if CONFIG_PATH:
    with open(CONFIG_PATH, encoding="utf-8-sig") as f:
        TARGETS = json.load(f)
    if not isinstance(TARGETS, list) or not TARGETS:
        raise ValueError("config_path must hold a non-empty JSON list of {\"table\", \"key_columns\", \"filter\"} objects")
else:
    TARGETS = [{"table": TABLE, "key_columns": KEY_COLUMNS, "filter": FILTER}]


def q(name):
    return f"`{name}`"


def parse_columns(value):
    cols = value.split(",") if isinstance(value, str) else list(value or [])
    return [str(c).strip().strip("`").strip() for c in cols if str(c).strip().strip("`").strip()]


def check_table(target):
    """Returns (summary row, per column null rows, sample duplicate keys as (table, pandas)). Raises on bad input."""
    if not isinstance(target, dict):
        raise ValueError(f"expected an object with table and key_columns, found {target!r}")
    table = str(target.get("table") or "").strip()
    parts = [p.strip().strip("`") for p in table.split(".")]
    if len(parts) != 3 or not all(parts):
        raise ValueError(f"table must be catalog.schema.table, found {table!r}")
    wanted = parse_columns(target.get("key_columns"))
    if not wanted:
        raise ValueError("key_columns is empty")
    flt = str(target.get("filter") or "").strip()

    df = spark.table(".".join(q(p) for p in parts))
    by_lower = {c.lower(): c for c in df.columns}
    missing = [c for c in wanted if c.lower() not in by_lower]
    if missing:
        raise ValueError(f"key column(s) not found in {table}: {', '.join(missing)}. Columns of the table: {', '.join(df.columns)}")
    keys = [by_lower[c.lower()] for c in wanted]
    if len(set(keys)) != len(keys):
        raise ValueError(f"key_columns lists the same column twice: {', '.join(wanted)}")
    bad = [f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields
           if f.name in keys and (f.dataType.typeName() in ("map", "variant") or f.dataType.simpleString() == "variant")]
    if bad:
        raise ValueError(f"key column type cannot be grouped for the uniqueness check in {table}: {', '.join(bad)}")
    if flt:
        df = df.filter(F.expr(flt))
    df = df.select(*[F.col(q(k)) for k in keys])

    # Not null: one pass over the table.
    any_null = F.lit(False)
    for k in keys:
        any_null = any_null | F.col(q(k)).isNull()
    counts = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.coalesce(F.sum(F.when(any_null, 1).otherwise(0)), F.lit(0)).alias("null_rows"),
        *[F.coalesce(F.sum(F.when(F.col(q(k)).isNull(), 1).otherwise(0)), F.lit(0)).alias(f"n{i}") for i, k in enumerate(keys)],
    ).first()
    rows, null_rows = int(counts["rows"]), int(counts["null_rows"])
    column_rows = [{"table": table, "key_column": k, "null_rows": int(counts[f"n{i}"])} for i, k in enumerate(keys)]

    # Unique: the combination of all key columns, over rows with no null in the key.
    non_null = df.filter(~any_null)   # not dropna: it would also drop NaN, which is a value here
    dups = non_null.groupBy(*[F.col(q(k)) for k in keys]).agg(F.count(F.lit(1)).alias("__n")).filter(F.col("__n") > 1)
    d = dups.agg(F.count(F.lit(1)).alias("keys"), F.coalesce(F.sum(F.col("__n") - 1), F.lit(0)).alias("extra")).first()
    dup_keys, extra_rows = int(d["keys"]), int(d["extra"])

    sample = pd.DataFrame()
    if dup_keys:
        sample = (dups.orderBy(F.col("__n").desc(), *[F.col(q(k)) for k in keys]).limit(20)
                  .select(*[F.col(q(k)).cast("string").alias(k) for k in keys], F.col("__n").alias("rows_with_this_key"))
                  .toPandas().astype(str))

    summary = {"table": table, "key_columns": ", ".join(keys), "filter": flt, "rows_checked": rows,
               "not_null": "PASS" if null_rows == 0 else "FAIL", "rows_with_null_key": null_rows,
               "unique": "PASS" if dup_keys == 0 else "FAIL", "duplicate_keys": dup_keys, "extra_rows": extra_rows}
    return summary, column_rows, (table, sample)


# COMMAND ----------

summary_rows, column_rows, sample_frames, errors = [], [], [], []
for target in TARGETS:
    name = target.get("table", "") if isinstance(target, dict) else str(target)
    try:
        s, c, smp = check_table(target)
        summary_rows.append(s)
        column_rows += c
        if len(smp[1]):
            sample_frames.append(smp)
    except Exception as e:
        errors.append(f"{name}: {str(e)[:2000]}")

# COMMAND ----------

if summary_rows:
    display(pd.DataFrame(summary_rows))
    display(pd.DataFrame(column_rows))
for table, smp in sample_frames:
    print(f"Sample duplicate keys for {table} (up to 20, most repeated first)")
    display(smp)
for e in errors:
    print(f"ERROR {e}")

# COMMAND ----------

n_failed = sum(1 for s in summary_rows if "FAIL" in (s["not_null"], s["unique"]))
summary_text = (f"PK check completed: {len(summary_rows)} table(s) checked, {len(summary_rows) - n_failed} passed, "
                f"{n_failed} failed, {len(errors)} error(s)")

# A failed check is a finding and does not fail the run. Only a real error (missing table or column, bad input) does.
if errors:
    raise Exception(f"{summary_text}. {' | '.join(errors)}")
dbutils.notebook.exit(summary_text)
