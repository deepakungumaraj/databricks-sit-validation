# Databricks SIT validation (RAW -> STG -> TGT)

A single Databricks notebook, `sit_validation.py`, that runs by hand after the load and compares the RAW, STG and TGT (CNFZ) layers. Results are shown at the end of the notebook, and it raises an error if any check fails.

Each layer has its own catalog, schema and table, given in full in the `CONFIG` dict (or a JSON/YAML file via the `config_path` widget).

Checks: record count, primary key, alternate key, STG vs TGT full row, RAW vs TGT key, column presence / data type / length vs mapping, default values, nullability, data quality, audit columns.

Usage: import the file into Databricks, fill in `CONFIG`, set the `entities` widget (optional), and run all cells.
