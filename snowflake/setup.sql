-- ============================================================
-- Snowflake Setup for GitHub Analytics Pipeline
-- Run these statements as ACCOUNTADMIN (or a role with SYSADMIN + SECURITYADMIN)
-- ============================================================

-- 1. Create a dedicated virtual warehouse for Airbyte loads
CREATE WAREHOUSE IF NOT EXISTS AIRBYTE_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'Dedicated warehouse for Airbyte ingestion jobs';

-- 2. Create the unified GitHub database
CREATE DATABASE IF NOT EXISTS GITHUB
  COMMENT = 'GitHub Analytics Pipeline — raw, staging, and marts';

-- 3. Create the three schemas
CREATE SCHEMA IF NOT EXISTS GITHUB.RAW
  COMMENT = 'Airbyte landing zone - one table per stream';

CREATE SCHEMA IF NOT EXISTS GITHUB.STAGING
  COMMENT = 'dbt staging models - typed and cleaned raw data';

CREATE SCHEMA IF NOT EXISTS GITHUB.MARTS
  COMMENT = 'dbt mart models - business-level aggregations for Preset';

-- 4. Create a dedicated Airbyte role
CREATE ROLE IF NOT EXISTS AIRBYTE_ROLE;

-- 5. Grant warehouse usage to the role
GRANT USAGE ON WAREHOUSE AIRBYTE_WH TO ROLE AIRBYTE_ROLE;

-- 6. Grant database-level privileges
GRANT USAGE ON DATABASE GITHUB TO ROLE AIRBYTE_ROLE;
GRANT CREATE SCHEMA ON DATABASE GITHUB TO ROLE AIRBYTE_ROLE;

-- 7. Grant full privileges on all three schemas
GRANT ALL PRIVILEGES ON SCHEMA GITHUB.RAW TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA GITHUB.RAW TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA GITHUB.RAW TO ROLE AIRBYTE_ROLE;

GRANT ALL PRIVILEGES ON SCHEMA GITHUB.STAGING TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA GITHUB.STAGING TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA GITHUB.STAGING TO ROLE AIRBYTE_ROLE;

GRANT ALL PRIVILEGES ON SCHEMA GITHUB.MARTS TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA GITHUB.MARTS TO ROLE AIRBYTE_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA GITHUB.MARTS TO ROLE AIRBYTE_ROLE;

-- 8. Create a dedicated service user for Airbyte
--    Replace <STRONG_PASSWORD> with a secure generated password before running
CREATE USER IF NOT EXISTS AIRBYTE_USER
  PASSWORD = '<STRONG_PASSWORD>'
  DEFAULT_ROLE = AIRBYTE_ROLE
  DEFAULT_WAREHOUSE = AIRBYTE_WH
  DEFAULT_NAMESPACE = GITHUB.RAW
  COMMENT = 'Service account for Airbyte Cloud connector';

-- 9. Bind the role to the user
GRANT ROLE AIRBYTE_ROLE TO USER AIRBYTE_USER;

-- ============================================================
-- Verification queries (run after setup to confirm access)
-- ============================================================
-- SHOW GRANTS TO ROLE AIRBYTE_ROLE;
-- SHOW GRANTS TO USER AIRBYTE_USER;
-- SHOW WAREHOUSES LIKE 'AIRBYTE_WH';
-- SHOW SCHEMAS IN DATABASE GITHUB;
