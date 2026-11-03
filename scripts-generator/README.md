# scripts-generator

Generates one Databricks SQL incremental-load script per table (control record, incremental MERGE, watermark, validation, close) from `config/tables.yaml` and Jinja templates.

```bash
pip install jinja2 pyyaml
python generate.py                  # all tables -> out/ (plus out/_setup.sql)
python generate.py --only table_03
```

- `config/tables.yaml`: one entry per table; names are placeholders. Defaults, step order, and variants are set here.
- `templates/steps/`: one file per step; `steps/<step>/<variant>.sql.j2` for variants.
- `templates/strategies/`: watermark strategies (`max_ts`, `max_id`).
- Per-table overrides: `steps:` (order), `variants:`, or inline `{sql: ...}` steps.

Generated SQL is untested on Databricks; requires a runtime with SQL scripting support. Run `out/_setup.sql` once first.
