# Databricks notebook source
# MAGIC %md
# MAGIC # SIT validation: RAW -> STG
# MAGIC Run by hand after the RAW -> STG load. Results are shown at the end of the notebook.
# MAGIC The notebook raises an error if any check fails, so a Workflow task built on it would fail too.

# COMMAND ----------

dbutils.widgets.text("raw_catalog", "RAW")
dbutils.widgets.text("stg_catalog", "STG")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("tables", "")          # comma separated; blank = every table in CONFIG
dbutils.widgets.text("batch_col", "")       # optional, e.g. _batch_id; applied to RAW and STG
dbutils.widgets.text("batch_value", "")     # optional

RAW = dbutils.widgets.get("raw_catalog").strip()
STG = dbutils.widgets.get("stg_catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
TABLES = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
BATCH_COL = dbutils.widgets.get("batch_col").strip()
BATCH_VALUE = dbutils.widgets.get("batch_value").strip()

# COMMAND ----------

# Per-table settings. Every key is optional. A table missing here still gets the generic checks
# (row count, schema). Add the table name to CONFIG to switch on the others.
#   pk              columns that must be unique and not null
#   not_null        other columns that must not contain nulls
#   measures        numeric columns whose sum/min/max must match RAW
#   load_ts_col     STG column holding the load timestamp
#   max_age_hours   freshness limit for load_ts_col
#   rejected_table  fully qualified table of rejected rows (counted in the row-count check)
#   tgt_table       STG table name, if different from the RAW name
CONFIG = {
    # "customer": {
    #     "pk": ["customer_id"],
    #     "not_null": ["customer_name"],
    #     "measures": ["credit_limit"],
    #     "load_ts_col": "load_ts",
    #     "max_age_hours": 24,
    #     "rejected_table": "STG.crm.customer_rejects",
    # },
}

# COMMAND ----------

from datetime import datetime, timedelta
from pyspark.sql import functions as F

results = []


def record(table, check, passed, expected="", actual="", detail=""):
    results.append({
        "table": table, "check": check, "status": "PASS" if passed else "FAIL",
        "expected": str(expected), "actual": str(actual), "detail": detail,
    })


def load(catalog, table, apply_batch=True):
    df = spark.table(f"{catalog}.{SCHEMA}.{table}")
    if apply_batch and BATCH_COL and BATCH_VALUE:
        df = df.filter(F.col(BATCH_COL) == BATCH_VALUE)
    return df


def check_row_count(table, raw, stg, cfg):
    raw_n, stg_n = raw.count(), stg.count()
    rej_n = 0
    if cfg.get("rejected_table"):
        rej = spark.table(cfg["rejected_table"])
        if BATCH_COL and BATCH_VALUE and BATCH_COL in rej.columns:
            rej = rej.filter(F.col(BATCH_COL) == BATCH_VALUE)
        rej_n = rej.count()
    record(table, "row_count", raw_n == stg_n + rej_n,
           expected=f"RAW {raw_n} = STG + rejected", actual=f"STG {stg_n} + rejected {rej_n}")


def check_schema(table, raw, stg, cfg):
    raw_cols = {f.name.lower(): f.dataType.simpleString() for f in raw.schema.fields}
    stg_cols = {f.name.lower(): f.dataType.simpleString() for f in stg.schema.fields}
    missing = sorted(set(raw_cols) - set(stg_cols))
    extra = sorted(set(stg_cols) - set(raw_cols))
    changed = sorted(c for c in raw_cols.keys() & stg_cols.keys() if raw_cols[c] != stg_cols[c])
    detail = "; ".join(filter(None, [
        f"missing in STG: {missing}" if missing else "",
        f"extra in STG: {extra}" if extra else "",
        f"type changed: {[(c, raw_cols[c], stg_cols[c]) for c in changed]}" if changed else "",
    ]))
    record(table, "schema", not detail, detail=detail)


def check_pk(table, raw, stg, cfg):
    pk = cfg.get("pk")
    if not pk:
        return
    nulls = stg.filter(" OR ".join(f"`{c}` IS NULL" for c in pk)).count()
    dups = stg.groupBy(*pk).count().filter("count > 1").count()
    record(table, "pk_not_null", nulls == 0, expected=0, actual=nulls)
    record(table, "pk_unique", dups == 0, expected=0, actual=dups, detail=f"keys: {pk}")


def check_not_null(table, raw, stg, cfg):
    for c in cfg.get("not_null", []):
        n = stg.filter(F.col(f"`{c}`").isNull()).count()
        record(table, f"not_null:{c}", n == 0, expected=0, actual=n)


def check_measures(table, raw, stg, cfg):
    for c in cfg.get("measures", []):
        def agg(df):
            return df.agg(F.sum(f"`{c}`"), F.min(f"`{c}`"), F.max(f"`{c}`")).first()
        r, s = agg(raw), agg(stg)
        rej_note = " (a rejected-table difference can explain a mismatch)" if cfg.get("rejected_table") else ""
        record(table, f"aggregate:{c}", tuple(r) == tuple(s),
               expected=f"sum/min/max {tuple(r)}", actual=tuple(s), detail=rej_note.strip())


def check_freshness(table, raw, stg, cfg):
    col = cfg.get("load_ts_col")
    if not col:
        return
    limit = cfg.get("max_age_hours", 24)
    latest = stg.agg(F.max(f"`{col}`")).first()[0]
    ok = latest is not None and latest >= datetime.now() - timedelta(hours=limit)
    record(table, "freshness", ok, expected=f"{col} within {limit}h", actual=latest)


CHECKS = [check_row_count, check_schema, check_pk, check_not_null, check_measures, check_freshness]

# COMMAND ----------

assert SCHEMA, "Set the 'schema' widget"
to_run = TABLES or list(CONFIG)
assert to_run, "Set the 'tables' widget or add tables to CONFIG"

for table in to_run:
    cfg = CONFIG.get(table, {})
    try:
        raw = load(RAW, table)
        stg = load(STG, cfg.get("tgt_table", table))
    except Exception as e:  # table missing or no access
        record(table, "load_tables", False, detail=str(e)[:300])
        continue
    for check in CHECKS:
        try:
            check(table, raw, stg, cfg)
        except Exception as e:
            record(table, check.__name__.replace("check_", ""), False, detail=f"ERROR: {str(e)[:300]}")

# COMMAND ----------

summary = spark.createDataFrame(results)
display(summary.select("table", "check", "status", "expected", "actual", "detail")
        .orderBy(F.col("status"), "table", "check"))

failed = [r for r in results if r["status"] == "FAIL"]
print(f"{len(results) - len(failed)} passed, {len(failed)} failed across {len(to_run)} table(s)")
if failed:
    raise Exception(f"SIT validation failed: {len(failed)} check(s). See the table above.")
