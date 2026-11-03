"""Render one Databricks SQL load script per table from config/tables.yaml + templates/.

A script is an ordered list of steps. Each step is one of:
  - "name"           -> templates/steps/<name>.sql.j2
  - "name" + variant -> templates/steps/<name>/<variant>.sql.j2, variant taken from `variants: {name: variant}`
  - {sql: "..."}     -> inline SQL (Jinja-rendered, so {{ name }}, {{ tgt_fqn }} etc. work)

Usage: python generate.py [--config config/tables.yaml] [--out out] [--only table_01 ...]
"""
import argparse
import pathlib
import sys
import textwrap

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined, TemplateNotFound

ROOT = pathlib.Path(__file__).parent
REQUIRED = ["name", "pk", "watermark_column"]
DEEP_KEYS = ("variants", "params")

env = Environment(loader=FileSystemLoader(ROOT / "templates"), undefined=StrictUndefined,
                  trim_blocks=True, lstrip_blocks=True, keep_trailing_newline=True)


def merge(defaults, table):
    ctx = {**defaults, **table}
    for k in DEEP_KEYS:
        ctx[k] = {**defaults.get(k, {}), **table.get(k, {})}
    return ctx


def derive(ctx):
    name, pk, cols = ctx["name"], ctx.get("pk", []), ctx.get("columns")
    ctx["src_fqn"] = f"{ctx['catalog']}.{ctx['source_schema']}.{ctx.get('source_name', name)}"
    ctx["tgt_fqn"] = f"{ctx['catalog']}.{ctx['target_schema']}.{name}"
    ctx["control_table"] = f"{ctx['catalog']}.{ctx['etl_schema']}.{ctx['control_table_name']}"
    ctx["watermark_table"] = f"{ctx['catalog']}.{ctx['etl_schema']}.{ctx['watermark_table_name']}"
    ctx["pk_join"] = " AND ".join(f"t.{c} = s.{c}" for c in pk)
    if cols:   # explicit column list
        ctx["col_select"] = ", ".join(cols)
        ctx["update_set"] = ", ".join(f"{c} = s.{c}" for c in cols if c not in pk)
        ctx["insert_clause"] = f"({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)})"
    else:      # schemas match: let Delta expand *
        ctx["col_select"], ctx["update_set"], ctx["insert_clause"] = "*", "*", "*"
    return ctx


def step_template(step, ctx):
    variant = ctx["variants"].get(step)
    return f"steps/{step}/{variant}.sql.j2" if variant else f"steps/{step}.sql.j2"


def validate(ctx):
    errs = [f"missing '{k}'" for k in REQUIRED if not ctx.get(k)]
    if not (ROOT / "templates" / "strategies" / f"{ctx.get('watermark_strategy')}.j2").exists():
        errs.append(f"unknown watermark_strategy '{ctx.get('watermark_strategy')}'")
    for s in ctx["steps"]:
        if isinstance(s, dict):
            if "sql" not in s:
                errs.append(f"inline step needs 'sql': {s}")
        elif not (ROOT / "templates" / step_template(s, ctx)).exists():
            errs.append(f"step '{s}' has no template {step_template(s, ctx)}")
    return errs


def render_steps(ctx):
    parts, names = [], []
    for s in ctx["steps"]:
        if isinstance(s, dict):
            parts.append(env.from_string(s["sql"]).render(**ctx).rstrip() + "\n")
            names.append(s.get("label", "custom"))
        else:
            parts.append(env.get_template(step_template(s, ctx)).render(**ctx))
            names.append(s)
    body = "\n".join(p.rstrip() + "\n" for p in parts)
    return textwrap.indent(body, "  "), names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=ROOT / "config" / "tables.yaml")
    ap.add_argument("--out", default=ROOT / "out")
    ap.add_argument("--only", nargs="*")
    a = ap.parse_args()

    cfg = yaml.safe_load(open(a.config, encoding="utf-8"))
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    main_tpl = env.get_template("main.sql.j2")

    failed, seen = False, set()
    for t in cfg["tables"]:
        if t.get("name") in seen:
            print(f"ERROR duplicate table name {t['name']}", file=sys.stderr)
            failed = True
        seen.add(t.get("name"))
        if a.only and t.get("name") not in a.only:
            continue
        ctx = derive(merge(cfg["defaults"], t))
        errs = validate(ctx)
        if errs:
            failed = True
            print(f"ERROR {t.get('name', '?')}: " + "; ".join(errs), file=sys.stderr)
            continue
        body, names = render_steps(ctx)
        (out / f"{ctx['name']}.sql").write_text(
            main_tpl.render(**ctx, body=body, step_names=names), encoding="utf-8")
        print(f"generated {ctx['name']}.sql")

    if not failed:
        sctx = derive(merge(cfg["defaults"], {"name": "_setup", "pk": ["x"], "watermark_column": "x"}))
        (out / "_setup.sql").write_text(env.get_template("setup.sql.j2").render(**sctx), encoding="utf-8")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
