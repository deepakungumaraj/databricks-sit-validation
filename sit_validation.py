# Databricks notebook source
# MAGIC %md
# MAGIC # SIT validation: RAW -> STG -> TGT (CNFZ)
# MAGIC Run by hand after the load. Results are shown at the end of the notebook, and the notebook raises an error if any check fails.
# MAGIC
# MAGIC Checks: record count, PK, alternate key, STG vs TGT full row, RAW vs TGT key, column presence / data type / length vs mapping, default values, nullability, data quality, audit columns.

# COMMAND ----------

dbutils.widgets.text("entities", "")       # comma separated names from CONFIG; blank = all
dbutils.widgets.text("batch_value", "")    # optional; used with the batch_col settings in CONFIG
dbutils.widgets.text("config_path", "")    # optional .json/.yaml/.yml file; replaces CONFIG below

ENTITIES = [e.strip() for e in dbutils.widgets.get("entities").split(",") if e.strip()]
BATCH_VALUE = dbutils.widgets.get("batch_value").strip()
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

# COMMAND ----------

# One entry per entity. Catalog.schema.table is different in each layer, so each layer is given in full.
# Everything except raw/stg/tgt is optional: a check is skipped when its setting is missing.
#
#   raw, stg, tgt   "catalog.schema.table"
#   filters         optional SQL filter per layer, e.g. {"tgt": "is_current = true"} for SCD2
#   batch_col       optional batch column per layer; used with the batch_value widget
#   keys            primary key columns per layer (same order, so raw[0] corresponds to stg[0] and tgt[0])
#   alt_keys        alternate keys per layer: {"tgt": [["email"], ["acct_no", "region"]]}
#   rejects         rejected-row tables: {"stg": "cat.sch.tbl"} (RAW -> STG), {"tgt": "cat.sch.tbl"} (STG -> TGT)
#   columns         the STG -> TGT mapping; one dict per TGT column:
#                     stg       source column in STG (None if derived in TGT)
#                     tgt       target column
#                     type      expected TGT type, e.g. "bigint", "decimal(10,2)", "varchar(50)"
#                     length    expected max length (or give it in type)
#                     nullable  False = mandatory
#                     default   expected default value
#                     recon     False to leave the column out of the full row comparison
#   audit_cols      TGT audit columns, must exist and not be null; timestamps must not be in the future
#   dq              data quality rules, each with name, layer (default tgt) and one of:
#                     {"sql": "SELECT * FROM {t} WHERE amount < 0"}   rows returned = violations
#                     {"col": "status", "accepted_values": ["A", "I"]}
#                     {"col": "email", "regex": "^[^@]+@[^@]+$"}
CONFIG = {
    # "customer": {
    #     "raw": "RAW.crm.customer",
    #     "stg": "STG.crm.customer_stg",
    #     "tgt": "CNFZ.core.dim_customer",
    #     "filters": {"tgt": "is_current = true"},
    #     "batch_col": {"raw": "_batch_id", "stg": "batch_id", "tgt": "batch_id"},
    #     "keys": {"raw": ["cust_id"], "stg": ["customer_id"], "tgt": ["customer_id"]},
    #     "alt_keys": {"tgt": [["email"]]},
    #     "rejects": {"stg": "STG.crm.customer_rejects"},
    #     "columns": [
    #         {"stg": "customer_id", "tgt": "customer_id", "type": "bigint", "nullable": False},
    #         {"stg": "customer_name", "tgt": "customer_name", "type": "varchar(100)", "nullable": False},
    #         {"stg": "status", "tgt": "status", "type": "string", "default": "A"},
    #     ],
    #     "audit_cols": ["created_ts", "updated_ts", "batch_id"],
    #     "dq": [{"name": "status_valid", "col": "status", "accepted_values": ["A", "I", "C"]}],
    # },
}

# COMMAND ----------

import json
import re
from datetime import datetime

from pyspark.sql import functions as F

if CONFIG_PATH:
    with open(CONFIG_PATH) as f:
        if CONFIG_PATH.lower().endswith((".yaml", ".yml")):
            import yaml
            CONFIG = yaml.safe_load(f)
        else:
            CONFIG = json.load(f)

LAYERS = ["raw", "stg", "tgt"]
results = []
samples = []


def record(entity, check, ok, expected="", actual="", detail=""):
    """ok is True, False, or "WARN" (reported but does not fail the run)."""
    status = "WARN" if ok == "WARN" else ("PASS" if ok else "FAIL")
    results.append({"entity": entity, "check": check, "status": status,
                    "expected": str(expected), "actual": str(actual), "detail": str(detail)[:500]})


def add_sample(entity, check, df):
    samples.append((entity, check, df.limit(20)))


def q(name):
    return f"`{name}`"


def load(cfg, layer):
    df = spark.table(".".join(q(p) for p in cfg[layer].split(".")))
    flt = cfg.get("filters", {}).get(layer)
    if flt:
        df = df.filter(flt)
    bcol = cfg.get("batch_col", {}).get(layer)
    if bcol and BATCH_VALUE:
        df = df.filter(F.col(q(bcol)) == BATCH_VALUE)
    return df


def norm_type(t):
    t = re.sub(r"\s+", "", str(t).lower())
    t = re.sub(r"^(varchar|char)\(\d+\)$", "string", t)
    return {"integer": "int", "long": "bigint", "short": "smallint", "byte": "tinyint",
            "numeric": "decimal", "real": "float"}.get(t, t)


def map_length(m):
    if m.get("length"):
        return int(m["length"])
    hit = re.match(r"^\s*(?:var)?char\((\d+)\)\s*$", str(m.get("type", "")), re.I)
    return int(hit.group(1)) if hit else None


def norm_default(v):
    return None if v is None else str(v).strip().strip("'\"").lower()


def table_info(fqname):
    """Column metadata from the catalog's information_schema, keyed by lower-case column name."""
    cat, sch, tbl = fqname.split(".")
    rows = spark.sql(
        f"SELECT column_name, full_data_type, character_maximum_length, is_nullable, column_default "
        f"FROM {q(cat)}.information_schema.columns "
        f"WHERE lower(table_schema) = lower('{sch}') AND lower(table_name) = lower('{tbl}')"
    ).collect()
    info = {}
    for r in rows:
        length = r["character_maximum_length"]
        if length is not None and (length <= 0 or length >= 2**31 - 1):
            length = None  # STRING reports 0 (or a huge placeholder): no declared length
        if length is None:
            hit = re.match(r"^(?:var)?char\((\d+)\)$", str(r["full_data_type"]).lower())
            length = int(hit.group(1)) if hit else None
        info[r["column_name"].lower()] = {
            "type": r["full_data_type"], "length": length,
            "nullable": str(r["is_nullable"]).upper() == "YES", "default": r["column_default"]}
    return info


def as_str(df, cols, aliases):
    return df.select(*[F.col(q(c)).cast("string").alias(a) for c, a in zip(cols, aliases)])

# COMMAND ----------

# Each check takes (entity, cfg, ctx). ctx holds the layer DataFrames in ctx["df"] and the TGT column metadata in ctx["info"].

def check_record_count(entity, cfg, ctx):
    n = {l: ctx["df"][l].count() for l in LAYERS}
    rej = {}
    for layer in ("stg", "tgt"):
        t = cfg.get("rejects", {}).get(layer)
        rej[layer] = 0
        if t:
            r = spark.table(".".join(q(p) for p in t.split(".")))
            bcol = cfg.get("batch_col", {}).get(layer)
            if bcol and BATCH_VALUE and bcol in r.columns:
                r = r.filter(F.col(q(bcol)) == BATCH_VALUE)
            rej[layer] = r.count()
    record(entity, "record_count RAW vs STG", n["raw"] == n["stg"] + rej["stg"],
           expected=f"RAW {n['raw']}", actual=f"STG {n['stg']} + rejected {rej['stg']}")
    record(entity, "record_count STG vs TGT", n["stg"] == n["tgt"] + rej["tgt"],
           expected=f"STG {n['stg']}", actual=f"TGT {n['tgt']} + rejected {rej['tgt']}")


def check_primary_key(entity, cfg, ctx):
    for layer, cols in cfg.get("keys", {}).items():
        df = ctx["df"][layer]
        nulls = df.filter(" OR ".join(f"{q(c)} IS NULL" for c in cols)).count()
        dups = df.groupBy(*[q(c) for c in cols]).count().filter("count > 1").count()
        record(entity, f"pk_not_null {layer.upper()}", nulls == 0, expected=0, actual=nulls, detail=f"key {cols}")
        record(entity, f"pk_unique {layer.upper()}", dups == 0, expected=0, actual=dups, detail=f"key {cols}")
        if dups:
            add_sample(entity, f"duplicate PK in {layer.upper()}",
                       df.groupBy(*[q(c) for c in cols]).count().filter("count > 1"))


def check_alternate_key(entity, cfg, ctx):
    for layer, keys in cfg.get("alt_keys", {}).items():
        df = ctx["df"][layer]
        for cols in keys:
            nn = df.filter(" AND ".join(f"{q(c)} IS NOT NULL" for c in cols))
            dups = nn.groupBy(*[q(c) for c in cols]).count().filter("count > 1").count()
            nulls = df.count() - nn.count()
            record(entity, f"alt_key_unique {layer.upper()}", dups == 0, expected=0, actual=dups,
                   detail=f"key {cols}; rows with a null in the key: {nulls}")


def check_full_row_stg_vs_tgt(entity, cfg, ctx):
    cols = [m for m in cfg.get("columns", []) if m.get("stg") and m.get("recon", True)]
    if not cols:
        return
    names = [m["tgt"] for m in cols]
    s = as_str(ctx["df"]["stg"], [m["stg"] for m in cols], names)
    t = as_str(ctx["df"]["tgt"], names, names)
    only_stg, only_tgt = s.exceptAll(t), t.exceptAll(s)
    a, b = only_stg.count(), only_tgt.count()
    record(entity, "full_row STG vs TGT", a == 0 and b == 0, expected="0 / 0",
           actual=f"in STG only {a} / in TGT only {b}", detail=f"{len(names)} mapped columns compared as strings")
    if a:
        add_sample(entity, "rows in STG not in TGT", only_stg)
    if b:
        add_sample(entity, "rows in TGT not in STG", only_tgt)


def check_key_raw_vs_tgt(entity, cfg, ctx):
    keys = cfg.get("keys", {})
    if not (keys.get("raw") and keys.get("tgt")):
        return
    alias = [f"k{i}" for i in range(len(keys["raw"]))]
    r = as_str(ctx["df"]["raw"], keys["raw"], alias).distinct()
    t = as_str(ctx["df"]["tgt"], keys["tgt"], alias).distinct()
    missing, extra = r.exceptAll(t), t.exceptAll(r)
    a, b = missing.count(), extra.count()
    note = "; some RAW keys may be rejected rows" if cfg.get("rejects") else ""
    record(entity, "key RAW vs TGT", a == 0 and b == 0, expected="0 / 0",
           actual=f"in RAW only {a} / in TGT only {b}", detail=f"RAW {keys['raw']} vs TGT {keys['tgt']}{note}")
    if a:
        add_sample(entity, "keys in RAW not in TGT", missing)
    if b:
        add_sample(entity, "keys in TGT not in RAW", extra)


def check_column_presence(entity, cfg, ctx):
    cols = cfg.get("columns", [])
    if not cols:
        return
    tgt_cols = {c.lower() for c in ctx["df"]["tgt"].columns}
    stg_cols = {c.lower() for c in ctx["df"]["stg"].columns}
    miss_tgt = [m["tgt"] for m in cols if m["tgt"].lower() not in tgt_cols]
    miss_stg = [m["stg"] for m in cols if m.get("stg") and m["stg"].lower() not in stg_cols]
    record(entity, "columns present vs mapping", not miss_tgt and not miss_stg,
           detail="; ".join(filter(None, [f"missing in TGT: {miss_tgt}" if miss_tgt else "",
                                          f"missing in STG: {miss_stg}" if miss_stg else ""])))
    known = {m["tgt"].lower() for m in cols} | {c.lower() for c in cfg.get("audit_cols", [])}
    unmapped = sorted(tgt_cols - known)
    if unmapped:
        record(entity, "TGT columns not in mapping", "WARN", detail=unmapped)


def check_data_types(entity, cfg, ctx):
    bad = []
    for m in cfg.get("columns", []):
        i = ctx["info"].get(m["tgt"].lower())
        if m.get("type") and i and norm_type(m["type"]) != norm_type(i["type"]):
            bad.append(f"{m['tgt']}: expected {m['type']}, found {i['type']}")
    record(entity, "data_types vs mapping", not bad, detail="; ".join(bad))


def check_lengths(entity, cfg, ctx):
    bad, undeclared, data_cols = [], [], []
    for m in cfg.get("columns", []):
        want = map_length(m)
        i = ctx["info"].get(m["tgt"].lower())
        if want is None or not i:
            continue
        if i["length"] is None:
            undeclared.append(m["tgt"])
        elif i["length"] != want:
            bad.append(f"{m['tgt']}: expected {want}, declared {i['length']}")
        data_cols.append((m["tgt"], want))
    if data_cols:
        row = ctx["df"]["tgt"].agg(*[F.max(F.length(F.col(q(c)))).alias(c) for c, _ in data_cols]).first()
        for c, want in data_cols:
            if row[c] is not None and row[c] > want:
                bad.append(f"{c}: longest value {row[c]} exceeds {want}")
    if data_cols:
        record(entity, "column_length vs mapping", not bad, detail="; ".join(bad))
    if undeclared:
        record(entity, "column_length declared in catalog", "WARN",
               detail=f"no length in information_schema, only the data was checked: {undeclared}")


def check_defaults(entity, cfg, ctx):
    cols = [m for m in cfg.get("columns", []) if m.get("default") is not None]
    if not cols:
        return
    bad_decl, bad_data = [], []
    keys = cfg.get("keys", {})
    can_join = bool(keys.get("stg") and keys.get("tgt"))
    for m in cols:
        i = ctx["info"].get(m["tgt"].lower())
        if i and i["default"] is not None and norm_default(i["default"]) != norm_default(m["default"]):
            bad_decl.append(f"{m['tgt']}: expected {m['default']}, declared {i['default']}")
        want = str(m["default"]).strip("'\"")
        if m.get("stg") and can_join:
            alias = [f"k{n}" for n in range(len(keys["stg"]))]
            s = ctx["df"]["stg"].select(*[F.col(q(c)).cast("string").alias(a) for c, a in zip(keys["stg"], alias)],
                                        F.col(q(m["stg"])).alias("sv"))
            t = ctx["df"]["tgt"].select(*[F.col(q(c)).cast("string").alias(a) for c, a in zip(keys["tgt"], alias)],
                                        F.col(q(m["tgt"])).cast("string").alias("tv"))
            n = s.join(t, alias).filter(F.col("sv").isNull() & ~F.col("tv").eqNullSafe(F.lit(want))).count()
            if n:
                bad_data.append(f"{m['tgt']}: {n} rows with a null source value do not hold the default '{want}'")
        else:
            n = ctx["df"]["tgt"].filter(F.col(q(m["tgt"])).isNull()).count()
            if n:
                bad_data.append(f"{m['tgt']}: {n} null values in a column with a default")
    record(entity, "default_values vs mapping", not bad_decl and not bad_data, detail="; ".join(bad_decl + bad_data))


def check_nullability(entity, cfg, ctx):
    cols = [m for m in cfg.get("columns", []) if m.get("nullable") is not None]
    if not cols:
        return
    bad = []
    for m in cols:
        i = ctx["info"].get(m["tgt"].lower())
        if i and i["nullable"] != bool(m["nullable"]):
            bad.append(f"{m['tgt']}: expected {'nullable' if m['nullable'] else 'NOT NULL'}, "
                       f"declared {'nullable' if i['nullable'] else 'NOT NULL'}")
    mandatory = [m["tgt"] for m in cols if not m["nullable"]]
    if mandatory:
        row = ctx["df"]["tgt"].agg(*[F.sum(F.col(q(c)).isNull().cast("int")).alias(c) for c in mandatory]).first()
        bad += [f"{c}: {row[c]} nulls in a mandatory column" for c in mandatory if row[c]]
    record(entity, "nullability / mandatory vs mapping", not bad, detail="; ".join(bad))


def check_data_quality(entity, cfg, ctx):
    for n, rule in enumerate(cfg.get("dq", [])):
        layer = rule.get("layer", "tgt")
        df = ctx["df"][layer]
        name = rule.get("name", f"rule{n + 1}")
        if "sql" in rule:
            view = f"_sit_{layer}"
            df.createOrReplaceTempView(view)
            bad = spark.sql(rule["sql"].replace("{t}", view))
        elif "accepted_values" in rule:
            c = F.col(q(rule["col"]))
            bad = df.filter(c.isNotNull() & ~c.isin(rule["accepted_values"]))
        elif "regex" in rule:
            c = F.col(q(rule["col"])).cast("string")
            bad = df.filter(c.isNotNull() & ~c.rlike(rule["regex"]))
        else:
            record(entity, f"dq:{name}", False, detail="rule needs sql, accepted_values or regex")
            continue
        cnt = bad.count()
        record(entity, f"dq:{name} {layer.upper()}", cnt <= rule.get("max_violations", 0), expected=0, actual=cnt)
        if cnt:
            add_sample(entity, f"dq:{name}", bad)


def check_audit_columns(entity, cfg, ctx):
    cols = cfg.get("audit_cols", [])
    if not cols:
        return
    df = ctx["df"]["tgt"]
    have = {f.name.lower(): f for f in df.schema.fields}
    missing = [c for c in cols if c.lower() not in have]
    present = [c for c in cols if c.lower() in have]
    bad = [f"missing: {missing}"] if missing else []
    if present:
        aggs = [F.sum(F.col(q(c)).isNull().cast("int")).alias(f"n_{c}") for c in present]
        ts = [c for c in present if have[c.lower()].dataType.simpleString() in ("timestamp", "timestamp_ntz", "date")]
        aggs += [F.sum((F.col(q(c)) > F.lit(datetime.now())).cast("int")).alias(f"f_{c}") for c in ts]
        row = df.agg(*aggs).first()
        bad += [f"{c}: {row[f'n_{c}']} nulls" for c in present if row[f"n_{c}"]]
        bad += [f"{c}: {row[f'f_{c}']} values in the future" for c in ts if row[f"f_{c}"]]
    record(entity, "audit_columns", not bad, detail="; ".join(bad))


CHECKS = [check_record_count, check_primary_key, check_alternate_key, check_full_row_stg_vs_tgt,
          check_key_raw_vs_tgt, check_column_presence, check_data_types, check_lengths,
          check_defaults, check_nullability, check_data_quality, check_audit_columns]

# COMMAND ----------

to_run = ENTITIES or list(CONFIG)
assert to_run, "Add an entity to CONFIG (or set config_path), or fill the 'entities' widget"

for entity in to_run:
    cfg = CONFIG[entity]
    try:
        ctx = {"df": {l: load(cfg, l) for l in LAYERS}, "info": table_info(cfg["tgt"])}
        if not ctx["info"]:
            record(entity, "tgt metadata", "WARN", detail="no rows in information_schema.columns for TGT; "
                   "type, length, nullability and default checks compare nothing")
    except Exception as e:  # table missing or no access
        record(entity, "load_tables", False, detail=e)
        continue
    for check in CHECKS:
        try:
            check(entity, cfg, ctx)
        except Exception as e:
            record(entity, check.__name__.replace("check_", ""), False, detail=f"ERROR: {e}")

# COMMAND ----------

order = F.when(F.col("status") == "FAIL", 0).when(F.col("status") == "WARN", 1).otherwise(2)
display(spark.createDataFrame(results).select("entity", "check", "status", "expected", "actual", "detail")
        .orderBy(order, "entity", "check"))

failed = [r for r in results if r["status"] == "FAIL"]
warned = [r for r in results if r["status"] == "WARN"]
print(f"{len(results) - len(failed) - len(warned)} passed, {len(failed)} failed, {len(warned)} warnings "
      f"across {len(to_run)} entity(ies)")

# COMMAND ----------

# Sample rows for the checks that failed (up to 20 each)
for entity, check, df in samples:
    print(f"{entity}: {check}")
    display(df)

# COMMAND ----------

if failed:
    raise Exception(f"SIT validation failed: {len(failed)} check(s). See the results table above.")
