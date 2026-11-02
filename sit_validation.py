# Databricks notebook source
# MAGIC %md
# MAGIC # SIT validation: RAW -> STG -> TGT (CNFZ)
# MAGIC Run by hand after the load. Results are shown at the end of the notebook, and the `fail_run_on` widget decides whether failed checks mark the run as failed.
# MAGIC
# MAGIC Checks: record count, PK, alternate key, STG vs TGT full row, RAW vs TGT key, column presence / data type / length vs mapping, default values, nullability, data quality, audit columns, aggregates.

# COMMAND ----------

dbutils.widgets.text("entities", "")       # comma separated names from CONFIG; blank = all
dbutils.widgets.text("config_path", "")    # optional .json/.yaml/.yml file; replaces CONFIG below
dbutils.widgets.text("output_dir", "")     # optional folder for Excel and CSV copies of the results, e.g. /Workspace/Users/<you>/sit_results
dbutils.widgets.dropdown("fail_run_on", "setup", ["never", "setup", "any"])   # when to mark the run as failed

ENTITIES = [e.strip() for e in dbutils.widgets.get("entities").split(",") if e.strip()]
OUTPUT_DIR = dbutils.widgets.get("output_dir").strip()
FAIL_RUN_ON = dbutils.widgets.get("fail_run_on").strip().lower()
assert FAIL_RUN_ON in ("never", "setup", "any"), f"fail_run_on must be never, setup or any, found {FAIL_RUN_ON!r}"
CONFIG_PATH = dbutils.widgets.get("config_path").strip()

# COMMAND ----------

# One entry per entity. Catalog.schema.table is different in each layer, so each layer is given in full.
# Everything except raw/stg/tgt is optional: a check is skipped when its setting is missing.
# Full reference: CONFIG.md
#
#   raw, stg, tgt   "catalog.schema.table"
#                   raw can also be a query when STG is built by joining several RAW tables:
#                     {"sql_file": "/Workspace/Users/<you>/sit_sql/orders.sql", "params": {"env": "dev"}}
#                   sql_file is a SELECT (or WITH) query uploaded to the workspace or a volume; ${env} in it is
#                   replaced by the params value. {"sql": "SELECT ..."} puts the query in the config instead.
#                   keys.raw and filters.raw then refer to the columns the query returns.
#   filters         optional SQL filter per layer, e.g. {"tgt": "is_current = true"} for SCD2
#   keys            primary key columns per layer (same order, so raw[0] corresponds to stg[0] and tgt[0]);
#                   RAW and STG values are cast to the TGT key type before they are compared
#   alt_keys        alternate keys per layer: {"tgt": [["email"], ["acct_no", "region"]]}
#   rejects         rejected-row tables: {"stg": "cat.sch.tbl"} (RAW -> STG), {"tgt": "cat.sch.tbl"} (STG -> TGT);
#                   each must hold only the rows rejected by the load being checked
#   columns         the STG -> TGT mapping; one dict per TGT column:
#                     stg       source column in STG (None if derived in TGT)
#                     tgt       target column
#                     type      expected TGT type, e.g. "bigint", "decimal(10,2)", "varchar(50)"
#                     length    expected max length (or give it in type)
#                     nullable  False = mandatory
#                     default   expected default value
#                     recon     False to leave the column out of the full row comparison
#                     expr      Spark SQL on STG columns that gives the expected TGT value, e.g. "upper(trim(name))"
#                               or "coalesce(status, 'A')" for a defaulted column; used instead of the stg column
#                               in the full row comparison
#                     agg       True to compare sum/min/max/non-null count of STG (or expr) vs TGT
#                   STG values are cast to the TGT column's type before they are compared with TGT.
#   audit_cols      TGT audit columns, must exist and not be null; timestamps must not be in the future
#   dq              data quality rules, each with name, layer (default tgt) and one of:
#                     {"sql": "SELECT * FROM {t} WHERE amount < 0"}   rows returned = violations
#                     {"col": "status", "accepted_values": ["A", "I"]}
#                     {"col": "email", "regex": "^[^@]+@[^@]+$"}
#                     {"cols": ["name", "city"], "no_leading_trailing_space": True}
#                     {"cols": ["name"], "not_empty": True}               null or blank values fail
#                     {"compare": ["start_date", "<", "end_date"], "nulls": "ignore"}   or "fail"
#                     {"col": "age", "range": [0, 120]}                   null values are ignored
CONFIG = {
    # "customer": {
    #     "raw": "RAW.crm.customer",
    #     "stg": "STG.crm.customer_stg",
    #     "tgt": "CNFZ.core.dim_customer",
    #     "filters": {"tgt": "is_current = true"},
    #     "keys": {"raw": ["cust_id"], "stg": ["customer_id"], "tgt": ["customer_id"]},
    #     "alt_keys": {"tgt": [["email"]]},
    #     "rejects": {"stg": "STG.crm.customer_rejects"},
    #     "columns": [
    #         {"stg": "customer_id", "tgt": "customer_id", "type": "bigint", "nullable": False},
    #         {"stg": "customer_name", "tgt": "customer_name", "type": "varchar(100)", "nullable": False},
    #         {"stg": "status", "tgt": "status", "type": "string", "default": "A", "expr": "coalesce(status, 'A')"},
    #     ],
    #     "audit_cols": ["created_ts", "updated_ts", "batch_id"],
    #     "dq": [{"name": "status_valid", "col": "status", "accepted_values": ["A", "I", "C"]}],
    # },
}

# COMMAND ----------

import json
import math
import os
import re
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

if CONFIG_PATH:
    with open(CONFIG_PATH) as f:
        if CONFIG_PATH.lower().endswith((".yaml", ".yml")):
            import yaml
            CONFIG = yaml.safe_load(f)
        else:
            CONFIG = json.load(f)

LAYERS = ["raw", "stg", "tgt"]
KNOWN_SETTINGS = {"raw", "stg", "tgt", "filters", "keys", "alt_keys", "rejects", "columns", "audit_cols", "dq"}
results = []
samples = []


# Categories group related checks in the results, in this order.
CATEGORIES = ["Setup", "Record Count", "Primary Key", "Alternate Key", "Reconciliation", "Column Mapping",
              "Nullability and Defaults", "Data Quality", "Audit Columns"]


def record(entity, category, check, ok, expected="", actual="", detail=""):
    """ok is True, False, "WARN" (reported but does not fail the run) or "SKIP" (entity not checked, does not fail the run)."""
    status = "WARN" if ok == "WARN" else ("SKIPPED" if ok == "SKIP" else ("PASS" if ok else "FAIL"))
    text = re.sub(r"\s*\n\s*", " ", str(detail))[:500]
    results.append({"entity": entity, "category": category, "check": check, "status": status,
                    "expected": str(expected), "actual": str(actual), "detail": text})


@contextmanager
def guard(entity, category, label):
    """Record an error as a Setup failure so one bad rule, layer or column does not stop the rest."""
    try:
        yield
    except Exception as e:
        record(entity, "Setup", f"Error in {category}: {label}", False, detail=f"ERROR: {e}")


def add_sample(entity, check, df):
    """Keep up to 20 rows as pandas now, so later checks (or temp views) cannot change what is shown."""
    try:
        samples.append((entity, check, df.limit(20).toPandas().astype(str)))
    except Exception as e:
        print(f"Could not capture sample rows for {entity}: {check} ({str(e)[:200]})")


def q(name):
    return f"`{name}`"


def full_name(fq):
    return ".".join(q(p) for p in fq.split("."))


def raw_query(src):
    """SQL for a RAW layer built from several tables: inline 'sql', or 'sql_file' (uploaded to the workspace or a volume)."""
    if "sql_file" in src:
        try:
            with open(src["sql_file"], encoding="utf-8-sig") as f:
                text = f.read()
        except OSError as e:
            raise ValueError(f"SQL file could not be opened: {e}")
        except UnicodeDecodeError:
            raise ValueError("SQL file is not UTF-8 text, save it as UTF-8")
    else:
        text = src["sql"]
    for k, v in (src.get("params") or {}).items():
        text = text.replace("${" + k + "}", str(v))
    left = sorted(set(re.findall(r"\$\{\w+\}", text)))
    if left:
        raise ValueError(f"no value in 'params' for {left}")
    # drop trailing blank lines, whole-line comments and semicolons (a sub-query cannot end with ";")
    lines = text.splitlines()
    while True:
        while lines and (not lines[-1].strip() or lines[-1].strip().startswith("--")):
            lines.pop()
        if lines and lines[-1].rstrip().endswith(";"):
            lines[-1] = lines[-1].rstrip().rstrip(";")
            continue
        break
    text = "\n".join(lines)
    head = re.sub(r"\A(?:\s+|--[^\n]*|/\*.*?\*/)+", "", text, flags=re.S)
    if not re.match(r"(?:select|with|values|from|table)\b|\(", head, re.I):
        raise ValueError(f"the file must hold one SELECT (or WITH ... SELECT) query but it starts with "
                         f"{head[:40]!r}; upload a copy with only the SELECT part")
    return text


def source_name(v):
    """How a layer's source is named in messages."""
    if isinstance(v, str):
        return v
    return f"query in {v['sql_file']}" if "sql_file" in v else "inline query"


def load(cfg, layer):
    src = cfg[layer]
    if isinstance(src, dict):
        # Wrapped as a sub-query, so anything that is not a SELECT fails to parse instead of running
        df = spark.sql(f"SELECT * FROM (\n{raw_query(src)}\n) AS raw_query")
    else:
        df = spark.table(full_name(src))
    flt = cfg.get("filters", {}).get(layer)
    if flt:
        df = df.filter(flt)
    df.schema  # resolve now, so a missing table or bad filter fails here and not in a later check
    return df


NOT_FOUND = re.compile(r"TABLE_OR_VIEW_NOT_FOUND|SCHEMA_NOT_FOUND|CATALOG_NOT_FOUND|NO_SUCH_CATALOG"
                       r"|cannot be found|does not exist", re.I)
NO_ACCESS = re.compile(r"PERMISSION_DENIED|INSUFFICIENT_PERMISSIONS", re.I)


def is_not_found(e):
    """True when the error only says a table, schema or catalog does not exist (yet). Access errors are not."""
    msg = str(e).split("== SQL")[0]  # Spark echoes the query after this marker; its text must not count
    return bool(NOT_FOUND.search(msg)) and not NO_ACCESS.search(msg)


def lower_cols(df):
    return {c.lower() for c in df.columns}


def tgt_type(df, col):
    return df.select(q(col)).schema.fields[0].dataType


def typed(sql_expr, dtype):
    """Cast to the TGT column's type, giving null (not an error) when a value does not fit."""
    return F.expr(f"try_cast({sql_expr} AS {dtype.simpleString()})")


def norm_type(t):
    t = re.sub(r"\s+", "", str(t).lower())
    t = re.sub(r"^(varchar|char)\(\d+\)$", "string", t)
    t = re.sub(r"^numeric", "decimal", t)
    return {"integer": "int", "long": "bigint", "short": "smallint", "byte": "tinyint", "real": "float"}.get(t, t)


def map_length(m):
    if m.get("length"):
        return int(m["length"])
    hit = re.match(r"^\s*(?:var)?char\((\d+)\)\s*$", str(m.get("type", "")), re.I)
    return int(hit.group(1)) if hit else None


def norm_default(v):
    return None if v is None else str(v).strip().strip("'\"").lower()


def same_default(a, b):
    """Defaults match as text, or as numbers (so 0 matches 0.00)."""
    x, y = norm_default(a), norm_default(b)
    try:
        return float(x) == float(y)
    except (TypeError, ValueError):
        return x == y


def same_values(a, b):
    """Row comparison for aggregates: floats use a small tolerance, everything else must be equal."""
    for x, y in zip(a, b):
        if x is None or y is None:
            if x is not y:
                return False
        elif isinstance(x, float) or isinstance(y, float):
            if not math.isclose(float(x), float(y), rel_tol=1e-9, abs_tol=1e-9):
                return False
        elif x != y:
            return False
    return True


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


def validate_config(cfg):
    """Returns (problems, warnings). Problems stop the entity from running."""
    problems, warnings = [], []
    if not isinstance(cfg, dict):
        return ["the entity settings must be an object"], warnings
    for layer in LAYERS:
        v = cfg.get(layer)
        if layer == "raw" and isinstance(v, dict):
            given = [k for k in ("sql", "sql_file") if k in v]
            if len(given) != 1 or not isinstance(v[given[0]], str):
                problems.append("'raw' as a query needs exactly one of 'sql' or 'sql_file', as text")
            if not isinstance(v.get("params") or {}, dict):
                problems.append("'raw' 'params' must be an object")
            if set(v) - {"sql", "sql_file", "params"}:
                problems.append(f"'raw' has unknown settings {sorted(set(v) - {'sql', 'sql_file', 'params'})}")
        elif not isinstance(v, str) or len(v.split(".")) != 3:
            problems.append(f"'{layer}' must be catalog.schema.table"
                            f"{' (or a query with sql / sql_file)' if layer == 'raw' else ''}, found {v!r}")
    def shape(name, ok):
        """The setting must be an object keyed by layer, with values that pass ok(). Returns True when it does."""
        v = cfg.get(name)
        if v is None:
            return False
        if isinstance(v, dict) and set(v) <= set(LAYERS) and all(ok(x) for x in v.values()):
            return True
        problems.append(f"'{name}' has the wrong shape (see CONFIG.md), found {v!r}")
        return False

    def cols_ok(x):
        return isinstance(x, list) and bool(x) and all(isinstance(c, str) for c in x)

    keys_ok = shape("keys", cols_ok)
    shape("alt_keys", lambda x: isinstance(x, list) and bool(x) and all(cols_ok(k) for k in x))
    shape("rejects", lambda x: not x or (isinstance(x, str) and len(x.split(".")) == 3))
    shape("filters", lambda x: isinstance(x, str))
    lengths = {l: len(c) for l, c in cfg["keys"].items()} if keys_ok else {}
    if len(set(lengths.values())) > 1:
        problems.append(f"'keys' lists must have the same number of columns in every layer, found {lengths}")
    columns = cfg.get("columns", [])
    if not isinstance(columns, list):
        problems.append(f"'columns' must be a list, found {columns!r}")
        columns = []
    for m in columns:
        if not isinstance(m, dict) or not m.get("tgt"):
            problems.append(f"every entry in 'columns' needs a 'tgt', found {m!r}")
            continue
        for flag in ("nullable", "recon", "agg"):
            if m.get(flag) is not None and not isinstance(m[flag], bool):
                problems.append(f"'{flag}' for {m['tgt']!r} must be true or false, not text, found {m[flag]!r}")
    unknown = sorted(set(cfg) - KNOWN_SETTINGS)
    if unknown:
        warnings.append(f"unknown settings ignored: {unknown}")
    return problems, warnings

# COMMAND ----------

# Each check takes (entity, cfg, ctx). ctx holds the layer DataFrames in ctx["df"] and the TGT column metadata in ctx["info"].

def check_record_count(entity, cfg, ctx):
    n = {l: ctx["df"][l].count() for l in LAYERS}
    rej = {}
    for layer in ("stg", "tgt"):
        t = cfg.get("rejects", {}).get(layer)
        rej[layer] = spark.table(full_name(t)).count() if t else 0
    record(entity, "Record Count", "RAW vs STG", n["raw"] == n["stg"] + rej["stg"],
           expected=f"RAW {n['raw']}", actual=f"STG {n['stg']} + rejected {rej['stg']}")
    record(entity, "Record Count", "STG vs TGT", n["stg"] == n["tgt"] + rej["tgt"],
           expected=f"STG {n['stg']}", actual=f"TGT {n['tgt']} + rejected {rej['tgt']}")


def check_primary_key(entity, cfg, ctx):
    for layer, cols in cfg.get("keys", {}).items():
        with guard(entity, "Primary Key", layer.upper()):
            df = ctx["df"][layer]
            nulls = df.filter(" OR ".join(f"{q(c)} IS NULL" for c in cols)).count()
            dup_keys = df.groupBy(*[q(c) for c in cols]).agg(F.count(F.lit(1)).alias("_n")).filter("_n > 1")
            dups = dup_keys.count()
            record(entity, "Primary Key", f"Not null ({layer.upper()})", nulls == 0, expected=0, actual=nulls, detail=f"key {cols}")
            record(entity, "Primary Key", f"Unique ({layer.upper()})", dups == 0, expected=0, actual=dups, detail=f"key {cols}")
            if dups:
                add_sample(entity, f"duplicate PK in {layer.upper()}", dup_keys)


def check_alternate_key(entity, cfg, ctx):
    for layer, keys in cfg.get("alt_keys", {}).items():
        total = None
        for cols in keys:
            with guard(entity, "Alternate Key", f"{layer.upper()} {cols}"):
                df = ctx["df"][layer]
                total = df.count() if total is None else total
                nn = df.filter(" AND ".join(f"{q(c)} IS NOT NULL" for c in cols))
                dups = nn.groupBy(*[q(c) for c in cols]).agg(F.count(F.lit(1)).alias("_n")).filter("_n > 1").count()
                nulls = total - nn.count()
                record(entity, "Alternate Key", f"Unique ({layer.upper()}) {cols}", dups == 0, expected=0, actual=dups,
                       detail=f"key {cols}; rows with a null in the key: {nulls}")


def check_full_row_stg_vs_tgt(entity, cfg, ctx):
    tgt = ctx["df"]["tgt"]
    tgt_cols, stg_cols = lower_cols(tgt), lower_cols(ctx["df"]["stg"])
    # a column missing from a layer is already reported by Columns present, so it is left out here
    cols = [m for m in cfg.get("columns", [])
            if (m.get("stg") or m.get("expr")) and m.get("recon", True) and m["tgt"].lower() in tgt_cols
            and (m.get("expr") or m["stg"].lower() in stg_cols)]
    if not cols:
        return
    names = [m["tgt"] for m in cols]
    s = ctx["df"]["stg"].select(*[
        typed(m["expr"] if m.get("expr") else q(m["stg"]), tgt_type(tgt, m["tgt"])).cast("string").alias(m["tgt"])
        for m in cols])
    t = as_str(tgt, names, names)
    only_stg, only_tgt = s.exceptAll(t), t.exceptAll(s)
    a, b = only_stg.count(), only_tgt.count()
    record(entity, "Reconciliation", "Full row: STG vs TGT", a == 0 and b == 0, expected="0 / 0",
           actual=f"in STG only {a} / in TGT only {b}",
           detail=f"{len(names)} mapped columns; STG values cast to the TGT type, then compared as text")
    if a:
        add_sample(entity, "rows in STG not in TGT", only_stg)
    if b:
        add_sample(entity, "rows in TGT not in STG", only_tgt)


def check_key_raw_vs_tgt(entity, cfg, ctx):
    keys = cfg.get("keys", {})
    if not (keys.get("raw") and keys.get("tgt")):
        return
    tgt = ctx["df"]["tgt"]
    alias = [f"k{i}" for i in range(len(keys["raw"]))]
    r = ctx["df"]["raw"].select(*[typed(q(c), tgt_type(tgt, tc)).alias(a)
                                  for c, tc, a in zip(keys["raw"], keys["tgt"], alias)]).distinct()
    t = tgt.select(*[F.col(q(tc)).alias(a) for tc, a in zip(keys["tgt"], alias)]).distinct()
    missing, extra = r.exceptAll(t), t.exceptAll(r)
    a, b = missing.count(), extra.count()
    note = "; some RAW keys may be rejected rows" if cfg.get("rejects") else ""
    record(entity, "Reconciliation", "Key: RAW vs TGT", a == 0 and b == 0, expected="0 / 0",
           actual=f"in RAW only {a} / in TGT only {b}",
           detail=f"RAW {keys['raw']} (cast to the TGT type) vs TGT {keys['tgt']}{note}")
    if a:
        add_sample(entity, "keys in RAW not in TGT", missing)
    if b:
        add_sample(entity, "keys in TGT not in RAW", extra)


def check_aggregates(entity, cfg, ctx):
    tgt = ctx["df"]["tgt"]
    tgt_cols, stg_cols = lower_cols(tgt), lower_cols(ctx["df"]["stg"])
    cols = [m for m in cfg.get("columns", []) if m.get("agg") and m["tgt"].lower() in tgt_cols
            and (m.get("expr") or (m.get("stg") and m["stg"].lower() in stg_cols))]
    if not cols:
        return
    bad, done = [], 0
    for m in cols:
        with guard(entity, "Reconciliation", f"aggregate {m['tgt']}"):
            dtype = tgt_type(tgt, m["tgt"])
            s = typed(m["expr"] if m.get("expr") else q(m["stg"]), dtype)
            t = F.col(q(m["tgt"]))
            numeric = isinstance(dtype, NumericType)   # a sum only makes sense for numbers

            def aggs(c):
                return [F.sum(c) if numeric else F.lit(None), F.min(c), F.max(c), F.count(c)]

            a = ctx["df"]["stg"].agg(*aggs(s)).first()
            b = tgt.agg(*aggs(t)).first()
            if not same_values(tuple(a), tuple(b)):
                bad.append(f"{m['tgt']}: STG (sum, min, max, count) {tuple(a)} vs TGT {tuple(b)}")
            done += 1
    if done:
        record(entity, "Reconciliation", "Aggregates: STG vs TGT", not bad, detail="; ".join(bad))


def check_column_presence(entity, cfg, ctx):
    cols = cfg.get("columns", [])
    if not cols:
        return
    tgt_cols = {c.lower() for c in ctx["df"]["tgt"].columns}
    stg_cols = {c.lower() for c in ctx["df"]["stg"].columns}
    miss_tgt = [m["tgt"] for m in cols if m["tgt"].lower() not in tgt_cols]
    miss_stg = [m["stg"] for m in cols if m.get("stg") and m["stg"].lower() not in stg_cols]
    record(entity, "Column Mapping", "Columns present", not miss_tgt and not miss_stg,
           detail="; ".join(filter(None, [f"missing in TGT: {miss_tgt}" if miss_tgt else "",
                                          f"missing in STG: {miss_stg}" if miss_stg else ""])))
    known = {m["tgt"].lower() for m in cols} | {c.lower() for c in cfg.get("audit_cols", [])}
    unmapped = sorted(tgt_cols - known)
    if unmapped:
        record(entity, "Column Mapping", "Unmapped TGT columns", "WARN", detail=unmapped)


def check_data_types(entity, cfg, ctx):
    bad, compared = [], 0
    for m in cfg.get("columns", []):
        i = ctx["info"].get(m["tgt"].lower())
        if m.get("type") and i:
            compared += 1
            if norm_type(m["type"]) != norm_type(i["type"]):
                bad.append(f"{m['tgt']}: expected {m['type']}, found {i['type']}")
    if compared:
        record(entity, "Column Mapping", "Data types", not bad, detail="; ".join(bad))


def check_lengths(entity, cfg, ctx):
    bad, undeclared, data_cols = [], [], []
    tgt_cols = lower_cols(ctx["df"]["tgt"])
    for m in cfg.get("columns", []):
        want = map_length(m)
        i = ctx["info"].get(m["tgt"].lower())
        if want is None or m["tgt"].lower() not in tgt_cols:
            continue
        if i and i["length"] is None:
            undeclared.append(m["tgt"])
        elif i and i["length"] != want:
            bad.append(f"{m['tgt']}: expected {want}, declared {i['length']}")
        data_cols.append((m["tgt"], want))   # the data is checked even when the catalog has nothing to compare
    if data_cols:
        row = ctx["df"]["tgt"].agg(*[F.max(F.length(F.col(q(c)))).alias(c) for c, _ in data_cols]).first()
        for c, want in data_cols:
            if row[c] is not None and row[c] > want:
                bad.append(f"{c}: longest value {row[c]} exceeds {want}")
        record(entity, "Column Mapping", "Lengths", not bad, detail="; ".join(bad))
    if undeclared:
        record(entity, "Column Mapping", "Length declared in catalog", "WARN",
               detail=f"no length in information_schema, only the data was checked: {undeclared}")


def check_defaults(entity, cfg, ctx):
    tgt = ctx["df"]["tgt"]
    tgt_cols, stg_cols = lower_cols(tgt), lower_cols(ctx["df"]["stg"])
    cols = [m for m in cfg.get("columns", []) if m.get("default") is not None and m["tgt"].lower() in tgt_cols]
    if not cols:
        return
    bad_decl, bad_data, done = [], [], 0
    keys = cfg.get("keys", {})
    can_join = bool(keys.get("stg") and keys.get("tgt"))
    for m in cols:
        with guard(entity, "Default values", m["tgt"]):
            i = ctx["info"].get(m["tgt"].lower())
            if i and i["default"] is not None and not same_default(i["default"], m["default"]):
                bad_decl.append(f"{m['tgt']}: expected {m['default']}, declared {i['default']}")
            want = m["default"].strip("'\"") if isinstance(m["default"], str) else m["default"]
            dtype = tgt_type(tgt, m["tgt"])
            # the default as a SQL literal, cast with try_cast so a default that does not fit the type is a finding, not an error
            literal = "'" + str(want).replace("\\", "\\\\").replace("'", "''") + "'"
            if m.get("stg") and can_join and m["stg"].lower() in stg_cols:
                alias = [f"k{n}" for n in range(len(keys["stg"]))]
                s = ctx["df"]["stg"].select(
                    *[typed(q(sk), tgt_type(tgt, tk)).alias(a) for sk, tk, a in zip(keys["stg"], keys["tgt"], alias)],
                    F.col(q(m["stg"])).alias("sv"))
                t = tgt.select(*[F.col(q(tk)).alias(a) for tk, a in zip(keys["tgt"], alias)],
                               F.col(q(m["tgt"])).alias("tv"))
                n = s.join(t, alias).filter(
                    F.col("sv").isNull() & ~F.col("tv").eqNullSafe(typed(literal, dtype))).count()
                if n:
                    bad_data.append(f"{m['tgt']}: {n} rows with a null source value do not hold the default '{want}'")
            else:
                n = tgt.filter(F.col(q(m["tgt"])).isNull()).count()
                if n:
                    bad_data.append(f"{m['tgt']}: {n} null values in a column with a default")
            done += 1
    if done:
        record(entity, "Nullability and Defaults", "Default values", not bad_decl and not bad_data,
               detail="; ".join(bad_decl + bad_data))


def check_nullability(entity, cfg, ctx):
    cols = [m for m in cfg.get("columns", []) if m.get("nullable") is not None]
    if not cols:
        return
    bad, compared = [], 0
    for m in cols:
        i = ctx["info"].get(m["tgt"].lower())
        if i:
            compared += 1
            if i["nullable"] != m["nullable"]:
                bad.append(f"{m['tgt']}: mapping says {'nullable' if m['nullable'] else 'NOT NULL'}, "
                           f"catalog declares {'nullable' if i['nullable'] else 'NOT NULL'}")
    if compared:
        record(entity, "Nullability and Defaults", "Declared nullability vs mapping", "WARN" if bad else True,
               detail="; ".join(bad))
    tgt_cols = lower_cols(ctx["df"]["tgt"])
    mandatory = [m["tgt"] for m in cols if not m["nullable"] and m["tgt"].lower() in tgt_cols]
    if mandatory:
        row = ctx["df"]["tgt"].agg(*[F.sum(F.col(q(c)).isNull().cast("int")).alias(c) for c in mandatory]).first()
        nulls = [f"{c}: {row[c]} nulls" for c in mandatory if row[c]]
        record(entity, "Nullability and Defaults", "Mandatory columns populated", not nulls, detail="; ".join(nulls))


def check_data_quality(entity, cfg, ctx):
    for n, rule in enumerate(cfg.get("dq", [])):
        layer = rule.get("layer", "tgt")
        name = rule.get("name", f"rule{n + 1}")
        with guard(entity, "Data Quality", name):
            df = ctx["df"][layer]
            if "sql" in rule:
                view = f"_sit_{layer}"
                df.createOrReplaceTempView(view)
                body = re.sub(r"[;\s]+$", "", rule["sql"].replace("{t}", view))
                # wrapped as a sub-query, so anything that is not a SELECT fails to parse instead of running
                bad = spark.sql(f"SELECT * FROM (\n{body}\n) AS dq_rows")
            elif "accepted_values" in rule:
                c = F.col(q(rule["col"]))
                bad = df.filter(c.isNotNull() & ~c.isin(rule["accepted_values"]))
            elif "regex" in rule:
                c = F.col(q(rule["col"])).cast("string")
                bad = df.filter(c.isNotNull() & ~c.rlike(rule["regex"]))
            elif rule.get("no_leading_trailing_space") or rule.get("not_empty"):
                cs = [F.col(q(c)).cast("string") for c in (rule.get("cols") or [rule["col"]])]
                if rule.get("no_leading_trailing_space"):
                    conds = [c.rlike(r"^\s|\s$") for c in cs]
                else:
                    conds = [c.isNull() | c.rlike(r"^\s*$") for c in cs]
                bad = df.filter(reduce(lambda x, y: x | y, conds))
            elif "compare" in rule:
                a, op, b = rule["compare"]
                if op not in ("<", "<=", ">", ">=", "=", "<>"):
                    record(entity, "Setup", f"Error in Data Quality: {name}", False, detail=f"unknown operator {op}")
                    continue
                ca, cb = F.col(q(a)), F.col(q(b))
                viol = ca.isNotNull() & cb.isNotNull() & ~F.expr(f"{q(a)} {op} {q(b)}")
                if rule.get("nulls", "ignore") == "fail":
                    viol = viol | ca.isNull() | cb.isNull()
                bad = df.filter(viol)
            elif "range" in rule:
                c, (lo, hi) = F.col(q(rule["col"])), rule["range"]
                out = ([c < lo] if lo is not None else []) + ([c > hi] if hi is not None else [])   # null = no bound
                bad = df.filter(c.isNotNull() & reduce(lambda x, y: x | y, out)) if out else df.limit(0)
            else:
                record(entity, "Setup", f"Error in Data Quality: {name}", False, detail="rule needs sql, "
                       "accepted_values, regex, no_leading_trailing_space, not_empty, compare or range")
                continue
            cnt = bad.count()
            allowed = rule.get("max_violations", 0)
            record(entity, "Data Quality", f"{name} ({layer.upper()})", cnt <= allowed,
                   expected=f"<= {allowed}", actual=cnt)
            if cnt > allowed:
                add_sample(entity, f"dq: {name}", bad)


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
        now = {"timestamp": F.current_timestamp(), "timestamp_ntz": F.expr("localtimestamp()"),
               "date": F.current_date()}
        types = {c: have[c.lower()].dataType.simpleString() for c in present}
        ts = [c for c in present if types[c] in now]
        aggs = [F.sum(F.col(q(c)).isNull().cast("int")).alias(f"n_{c}") for c in present]
        aggs += [F.sum((F.col(q(c)) > now[types[c]]).cast("int")).alias(f"f_{c}") for c in ts]
        row = df.agg(*aggs).first()
        bad += [f"{c}: {row[f'n_{c}']} nulls" for c in present if row[f"n_{c}"]]
        bad += [f"{c}: {row[f'f_{c}']} values in the future" for c in ts if row[f"f_{c}"]]
    record(entity, "Audit Columns", "Present and populated", not bad, detail="; ".join(bad))


CHECKS = [check_record_count, check_primary_key, check_alternate_key, check_full_row_stg_vs_tgt,
          check_key_raw_vs_tgt, check_column_presence, check_data_types, check_lengths,
          check_defaults, check_nullability, check_data_quality, check_audit_columns,
          check_aggregates]

# COMMAND ----------

to_run = ENTITIES or list(CONFIG)
assert to_run, "Add an entity to CONFIG (or set config_path), or fill the 'entities' widget"

def run_entity(entity):
    cfg = CONFIG.get(entity)
    if cfg is None:
        record(entity, "Setup", "Unknown entity", False, detail=f"not in the config; available: {sorted(CONFIG)}")
        return
    problems, warnings = validate_config(cfg)
    for w in warnings:
        record(entity, "Setup", "Config settings", "WARN", detail=w)
    if problems:
        record(entity, "Setup", "Config settings", False, detail="; ".join(problems))
        return
    # A table that does not exist yet (entity added before its tables) skips the entity; any other error fails it.
    dfs, found, missing, errors = {}, [], [], []
    tables = [(l, l.upper(), source_name(cfg[l])) for l in LAYERS]
    tables += [(None, f"{l.upper()} rejects", t) for l, t in cfg.get("rejects", {}).items() if t]
    for layer, label, name in tables:
        try:
            if layer:
                dfs[layer] = load(cfg, layer)
            else:
                spark.table(full_name(name)).schema
            found.append(label)
        except Exception as e:  # table missing, no access, or a bad filter
            if is_not_found(e):
                # for a query the error names the missing table
                missing.append(f"{label} {name}" + (f" ({str(e)[:200]})" if isinstance(cfg.get(layer), dict) else ""))
            else:
                errors.append(f"{label} {name}: {str(e)[:300]}")
    gone = [f"not found: {', '.join(missing)}"] if missing else []
    if errors:
        record(entity, "Setup", "Load tables", False, detail="; ".join(errors + gone))
        return
    if missing:
        record(entity, "Setup", "Skipped: tables not found", "SKIP",
               detail=f"{gone[0]}; found: {', '.join(found) or 'none'}; no checks were run for this entity")
        return
    if isinstance(cfg["raw"], dict):
        # an uploaded file can go stale, so show which file ran and when it was last changed
        note = "inline query from the config"
        if "sql_file" in cfg["raw"]:
            try:
                changed = datetime.fromtimestamp(os.path.getmtime(cfg["raw"]["sql_file"]), timezone.utc)
                note = f"{cfg['raw']['sql_file']} (last changed {changed:%Y-%m-%d %H:%M} UTC)"
            except OSError:
                note = f"{cfg['raw']['sql_file']} (last changed unknown)"
        record(entity, "Setup", "RAW source", True, detail=note)
    try:
        info = table_info(cfg["tgt"])
        info_note = "no rows in information_schema.columns for TGT" if not info else ""
    except Exception as e:
        info, info_note = {}, f"could not read information_schema for TGT: {e}"
    if info_note:
        record(entity, "Setup", "TGT column metadata", "WARN",
               detail=f"{info_note}; type, length, declared nullability and declared default are not compared")
    ctx = {"df": dfs, "info": info}
    for check in CHECKS:
        try:
            check(entity, cfg, ctx)
        except Exception as e:
            record(entity, "Setup", "Error in " + check.__name__[6:].replace("_", " "), False, detail=e)


for entity in to_run:
    try:
        run_entity(entity)
    except Exception as e:  # whatever goes wrong in one entity must not stop the others or the results below
        record(entity, "Setup", "Unexpected error", False, detail=f"ERROR: {e}")

# COMMAND ----------

STATUS_ORDER = {"FAIL": 0, "WARN": 1, "SKIPPED": 2, "PASS": 3}
run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
run_by = spark.sql("SELECT current_user()").first()[0]

failed = [r for r in results if r["status"] == "FAIL"]
warned = [r for r in results if r["status"] == "WARN"]
skipped = [r for r in results if r["status"] == "SKIPPED"]

# One row per entity and category, worst status shown
rollup = {}
for r in results:
    row = rollup.setdefault((r["entity"], r["category"]), {
        "entity": r["entity"], "category": r["category"], "checks": 0, "passed": 0, "failed": 0, "warnings": 0,
        "skipped": 0})
    row["checks"] += 1
    row[{"PASS": "passed", "FAIL": "failed", "WARN": "warnings", "SKIPPED": "skipped"}[r["status"]]] += 1
summary_rows = list(rollup.values())
for row in summary_rows:
    row["status"] = ("FAIL" if row["failed"] else "WARN" if row["warnings"] else
                     "SKIPPED" if row["skipped"] else "PASS")
summary_rows.sort(key=lambda r: (STATUS_ORDER[r["status"]], r["entity"], CATEGORIES.index(r["category"])))
SUMMARY_COLS = ["entity", "category", "status", "checks", "passed", "failed", "warnings", "skipped"]

# One row per check, failures first
detail_rows = sorted(results, key=lambda r: (STATUS_ORDER[r["status"]], r["entity"],
                                              CATEGORIES.index(r["category"]), r["check"]))
DETAIL_COLS = ["entity", "category", "check", "status", "expected", "actual", "detail"]

print("Summary by category")
display(spark.createDataFrame(summary_rows).select(*SUMMARY_COLS))
print("All checks")
display(spark.createDataFrame(detail_rows).select(*DETAIL_COLS))
print(f"{len(results) - len(failed) - len(warned) - len(skipped)} passed, {len(failed)} failed, "
      f"{len(warned)} warnings, {len(skipped)} skipped across {len(to_run)} entity(ies)")

# COMMAND ----------

# Sample rows for the checks that failed (up to 20 each)
for entity, check, pdf in samples:
    print(f"{entity}: {check}")
    display(pdf)

# COMMAND ----------

# Copies of the results for the tracker, written as files (not tables) before the run can be marked failed (see fail_run_on below):
#   sit_results_<time>.csv    every check, with run_time_utc and run_by columns (one file per run)
#   sit_results_latest.csv    the same, overwritten each run, so Excel can point at one file name
#   sit_results_<time>.xlsx   Run, Summary and Results sheets, plus one sheet per set of sample rows
# A problem writing the files is reported below but does not stop the run.
export_note = ""
if OUTPUT_DIR:
    try:
        import shutil
        import tempfile
        import pandas as pd

        os.makedirs(OUTPUT_DIR, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        detail = pd.DataFrame(detail_rows)[DETAIL_COLS]
        detail.insert(0, "run_by", run_by)
        detail.insert(0, "run_time_utc", run_time)
        for name in (f"sit_results_{stamp}.csv", "sit_results_latest.csv"):
            detail.to_csv(os.path.join(OUTPUT_DIR, name), index=False, encoding="utf-8-sig")
        written = [f"sit_results_{stamp}.csv", "sit_results_latest.csv"]

        try:
            run_info = pd.DataFrame([{
                "run_time_utc": run_time, "run_by": run_by, "entities": ", ".join(to_run), "checks": len(results),
                "passed": len(results) - len(failed) - len(warned) - len(skipped), "failed": len(failed),
                "warnings": len(warned), "skipped": len(skipped)}])
            xlsx = f"sit_results_{stamp}.xlsx"
            tmp = os.path.join(tempfile.mkdtemp(), xlsx)  # Excel needs random-access writes, so build it locally first
            with pd.ExcelWriter(tmp) as xl:
                run_info.to_excel(xl, sheet_name="Run", index=False)
                pd.DataFrame(summary_rows)[SUMMARY_COLS].to_excel(xl, sheet_name="Summary", index=False)
                pd.DataFrame(detail_rows)[DETAIL_COLS].to_excel(xl, sheet_name="Results", index=False)
                for n, (entity, check, pdf) in enumerate(samples, 1):
                    name = re.sub(r"[^A-Za-z0-9 _-]", "", f"{n} {entity} {check}")[:31]
                    pdf.to_excel(xl, sheet_name=name, index=False)
            shutil.copy(tmp, os.path.join(OUTPUT_DIR, xlsx))
            written.append(xlsx)
        except Exception as e:
            export_note = f" (Excel file not written: {str(e)[:200]})"
        print(f"Written to {OUTPUT_DIR}: {', '.join(written)}{export_note}")
    except Exception as e:
        export_note = f" (output files not written: {str(e)[:200]})"
        print(f"Output files not written{export_note}")

# COMMAND ----------

passed = len(results) - len(failed) - len(warned) - len(skipped)
setup_failed = [r for r in failed if r["category"] == "Setup"]
summary_text = (f"SIT completed: {passed} passed, {len(failed)} failed, {len(warned)} warnings, "
                f"{len(skipped)} skipped across {len(to_run)} entity(ies){export_note}")

# fail_run_on decides whether the job run is marked failed:
#   never  the run succeeds whatever the checks found (read the results)
#   setup  the run fails only if something could not run at all (Setup category), not for failed checks
#   any    the run fails if any check failed
# SKIPPED entities (tables not created yet) never fail the run.
if (FAIL_RUN_ON == "any" and failed) or (FAIL_RUN_ON == "setup" and setup_failed):
    raise Exception(f"{summary_text}. See the results tables above.")
dbutils.notebook.exit(summary_text)
