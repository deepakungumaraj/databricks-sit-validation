# Databricks SIT validation (RAW -> STG)

A single Databricks notebook, `sit_validation.py`, that runs by hand after the RAW -> STG load and shows pass/fail results at the end of the notebook. It raises an error if any check fails.

Checks: row count (RAW = STG + rejected), schema, and optionally primary key not null/unique, not-null columns, sum/min/max of measures, and freshness. Per-table options go in the `CONFIG` dict.

Usage: import the file into Databricks, set the `schema` and `tables` widgets, and run all cells.
