-- Run in a Databricks SQL editor or SQL notebook.
-- Requires permission to create a catalog. If you already have a permitted
-- catalog, replace 'fleetpulse' in ALL subsequent files with its name.
CREATE CATALOG IF NOT EXISTS fleetpulse;
CREATE SCHEMA IF NOT EXISTS fleetpulse.bronze;
CREATE SCHEMA IF NOT EXISTS fleetpulse.silver;
CREATE SCHEMA IF NOT EXISTS fleetpulse.gold;
CREATE SCHEMA IF NOT EXISTS fleetpulse.ops;
CREATE VOLUME IF NOT EXISTS fleetpulse.ops.checkpoints;
