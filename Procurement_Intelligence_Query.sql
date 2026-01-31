-- ==========================================================
-- PROCUREMENT INTELLIGENCE PLATFORM
-- Final, Professional, Submission-Ready SQL Server Script
-- Author: Varun Gupta
-- ==========================================================

/*
ARCHITECTURE OVERVIEW
---------------------
This script implements a layered procurement analytics system:

1. Database & Schemas
2. Governance / Audit Logging
3. Staging (Raw Ingestion Layer)
4. Vendor Normalization (Master Data)
5. Data Quality Gate (Clean vs Rejects)
6. Analytics Fact Table (Business Intelligence Layer)
7. Reporting Views (BI Consumption Layer)
8. Indexing & Performance Optimization
9. Project Metadata
10. Pipeline Run Auditing

DESIGN PRINCIPLES
-----------------
- Idempotent (safe to run multiple times)
- Layered architecture (staging → analytics → reporting)
- Business-rule driven data quality
- BI-ready output
*/

-- ==========================================================
-- 1. DATABASE SETUP
-- ==========================================================

IF DB_ID('Procurement_Intelligence') IS NULL
BEGIN
    CREATE DATABASE Procurement_Intelligence;
END
GO

USE Procurement_Intelligence;
GO

-- ==========================================================
-- 2. SCHEMA ARCHITECTURE
-- ==========================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
    EXEC('CREATE SCHEMA analytics');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'reporting')
    EXEC('CREATE SCHEMA reporting');
GO

-- ==========================================================
-- 3. GOVERNANCE / AUDIT LOG
-- ==========================================================

IF OBJECT_ID('analytics.project_log','U') IS NULL
BEGIN
    CREATE TABLE analytics.project_log (
        log_id INT IDENTITY(1,1) PRIMARY KEY,
        action_type VARCHAR(100) NOT NULL,
        action_description VARCHAR(255) NOT NULL,
        action_time DATETIME DEFAULT GETDATE()
    );
END
GO

INSERT INTO analytics.project_log (action_type, action_description)
VALUES ('SETUP', 'Initialized Procurement Intelligence platform and schema architecture');
GO

-- ==========================================================
-- 4. STAGING LAYER (RAW INGESTION)
-- ==========================================================

IF OBJECT_ID('staging.stg_procurement_raw','U') IS NULL
BEGIN
    CREATE TABLE staging.stg_procurement_raw (
        Purchase_ID VARCHAR(50) NOT NULL,
        Vendor_Name VARCHAR(255) NOT NULL,
        Category VARCHAR(100),
        Sub_Category VARCHAR(100),
        Spend_Amount_INR DECIMAL(18,2) NOT NULL,
        Purchase_Date DATE NOT NULL,
        Region VARCHAR(50),
        Payment_Terms VARCHAR(50),
        Delivery_Time_Days INT,
        Quality_Score INT,
        Vendor_Score INT
    );
END
GO

-- Enforce uniqueness on business key
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'uq_staging_purchase'
      AND object_id = OBJECT_ID('staging.stg_procurement_raw')
)
BEGIN
    CREATE UNIQUE INDEX uq_staging_purchase
    ON staging.stg_procurement_raw (Purchase_ID);
END
GO

-- ==========================================================
-- 5. VENDOR NORMALIZATION MAP (MASTER DATA)
-- ==========================================================

IF OBJECT_ID('analytics.vendor_normalization_map','U') IS NULL
BEGIN
    CREATE TABLE analytics.vendor_normalization_map (
        raw_vendor_name VARCHAR(255) PRIMARY KEY,
        clean_vendor_name VARCHAR(255) NOT NULL,
        normalization_rule VARCHAR(255),
        created_at DATETIME DEFAULT GETDATE()
    );
END
GO

-- ==========================================================
-- 6. DATA QUALITY GATE (CLEAN vs REJECTS)
-- ==========================================================

IF OBJECT_ID('analytics.procurement_clean_gate','U') IS NULL
BEGIN
    CREATE TABLE analytics.procurement_clean_gate (
        Purchase_ID VARCHAR(50) PRIMARY KEY,
        Vendor_Name VARCHAR(255),
        Category VARCHAR(100),
        Sub_Category VARCHAR(100),
        Spend_Amount_INR DECIMAL(18,2),
        Purchase_Date DATE,
        Region VARCHAR(50),
        Payment_Terms VARCHAR(50),
        Delivery_Time_Days INT,
        Quality_Score INT,
        Vendor_Score INT,
        Quality_Flag VARCHAR(50),
        Load_Timestamp DATETIME DEFAULT GETDATE()
    );
END
GO

IF OBJECT_ID('analytics.procurement_rejects','U') IS NULL
BEGIN
    CREATE TABLE analytics.procurement_rejects (
        Purchase_ID VARCHAR(50),
        Reject_Reason VARCHAR(255),
        Reject_Time DATETIME DEFAULT GETDATE()
    );
END
GO

-- ==========================================================
-- 7. ANALYTICS FACT TABLE (BUSINESS INTELLIGENCE LAYER)
-- ==========================================================

IF OBJECT_ID('analytics.fact_procurement_spend','U') IS NULL
BEGIN
    CREATE TABLE analytics.fact_procurement_spend (
        Purchase_ID VARCHAR(50) PRIMARY KEY,
        Clean_Vendor_Name VARCHAR(255),
        Category VARCHAR(100),
        Sub_Category VARCHAR(100),
        Spend_Amount_INR DECIMAL(18,2),
        Purchase_Date DATE,
        Purchase_Month VARCHAR(7),
        Region VARCHAR(50),
        Payment_Terms VARCHAR(50),
        Delivery_Time_Days INT,
        Quality_Score INT,
        Vendor_Score INT,
        Contract_Status VARCHAR(20),
        Risk_Level VARCHAR(20),
        Outlier_Flag VARCHAR(10),
        Load_Timestamp DATETIME DEFAULT GETDATE()
    );
END
GO

-- ==========================================================
-- 8. REPORTING LAYER (BI VIEWS)
-- ==========================================================

CREATE OR ALTER VIEW reporting.vw_monthly_procurement_kpis AS
SELECT
    Purchase_Month,
    SUM(Spend_Amount_INR) AS Total_Spend_INR,
    COUNT(DISTINCT Clean_Vendor_Name) AS Active_Vendors,
    SUM(CASE WHEN Risk_Level = 'HIGH' THEN 1 ELSE 0 END) AS High_Risk_Orders,
    AVG(Delivery_Time_Days) AS Avg_Delivery_Days
FROM analytics.fact_procurement_spend
GROUP BY Purchase_Month;
GO

-- ==========================================================
-- 9. INDEXING & PERFORMANCE
-- ==========================================================

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'idx_staging_vendor'
      AND object_id = OBJECT_ID('staging.stg_procurement_raw')
)
BEGIN
    CREATE INDEX idx_staging_vendor
    ON staging.stg_procurement_raw (Vendor_Name);
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'idx_fact_month'
      AND object_id = OBJECT_ID('analytics.fact_procurement_spend')
)
BEGIN
    CREATE INDEX idx_fact_month
    ON analytics.fact_procurement_spend (Purchase_Month);
END
GO

-- ==========================================================
-- 10. PROJECT METADATA
-- ==========================================================

IF OBJECT_ID('analytics.project_metadata','U') IS NULL
BEGIN
    CREATE TABLE analytics.project_metadata (
        project_name VARCHAR(100),
        author_name VARCHAR(100),
        created_on DATETIME DEFAULT GETDATE(),
        description VARCHAR(255)
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM analytics.project_metadata)
BEGIN
    INSERT INTO analytics.project_metadata (
        project_name,
        author_name,
        description
    )
    VALUES (
        'Procurement Intelligence System',
        'Varun Gupta',
        'End-to-end procurement analytics platform with staging, normalization, data quality, risk intelligence, and BI-ready reporting views'
    );
END
GO

-- ==========================================================
-- 11. PIPELINE EXECUTION (FULL REFRESH)
-- ==========================================================

-- 11.1 Refresh Vendor Normalization Map
TRUNCATE TABLE analytics.vendor_normalization_map;
GO

INSERT INTO analytics.vendor_normalization_map (
    raw_vendor_name,
    clean_vendor_name,
    normalization_rule
)
SELECT DISTINCT
    Vendor_Name,
    LTRIM(RTRIM(
        REPLACE(
        REPLACE(
        REPLACE(
        REPLACE(
        REPLACE(UPPER(Vendor_Name), ' PVT.', ''),
        ' PVT', ''),
        ' LTD.', ''),
        ' LTD', ''),
        ' INC', '')
    )),
    'UPPER + TRIM + REMOVE LEGAL SUFFIXES'
FROM staging.stg_procurement_raw;
GO

-- 11.2 Clear Previous Analytics Output
TRUNCATE TABLE analytics.procurement_clean_gate;
TRUNCATE TABLE analytics.procurement_rejects;
TRUNCATE TABLE analytics.fact_procurement_spend;
GO

-- 11.3 Load Clean Records
INSERT INTO analytics.procurement_clean_gate (
    Purchase_ID,
    Vendor_Name,
    Category,
    Sub_Category,
    Spend_Amount_INR,
    Purchase_Date,
    Region,
    Payment_Terms,
    Delivery_Time_Days,
    Quality_Score,
    Vendor_Score,
    Quality_Flag
)
SELECT
    Purchase_ID,
    Vendor_Name,
    Category,
    Sub_Category,
    Spend_Amount_INR,
    Purchase_Date,
    Region,
    Payment_Terms,
    Delivery_Time_Days,
    Quality_Score,
    Vendor_Score,
    'VALID'
FROM staging.stg_procurement_raw
WHERE
    Purchase_ID IS NOT NULL
    AND Vendor_Name IS NOT NULL
    AND Spend_Amount_INR > 0
    AND Purchase_Date IS NOT NULL
    AND (Quality_Score BETWEEN 1 AND 10 OR Quality_Score IS NULL)
    AND (Vendor_Score BETWEEN 1 AND 100 OR Vendor_Score IS NULL);
GO

-- 11.4 Load Rejected Records
INSERT INTO analytics.procurement_rejects (
    Purchase_ID,
    Reject_Reason
)
SELECT
    Purchase_ID,
    'FAILED DATA QUALITY RULES'
FROM staging.stg_procurement_raw
WHERE
    Purchase_ID IS NULL
    OR Vendor_Name IS NULL
    OR Spend_Amount_INR <= 0
    OR Purchase_Date IS NULL
    OR Quality_Score NOT BETWEEN 1 AND 10
    OR Vendor_Score NOT BETWEEN 1 AND 100;
GO

-- 11.5 Build Analytics Fact Table
WITH stats AS (
    SELECT
        AVG(Spend_Amount_INR) AS avg_spend,
        STDEV(Spend_Amount_INR) AS std_spend
    FROM analytics.procurement_clean_gate
)
INSERT INTO analytics.fact_procurement_spend (
    Purchase_ID,
    Clean_Vendor_Name,
    Category,
    Sub_Category,
    Spend_Amount_INR,
    Purchase_Date,
    Purchase_Month,
    Region,
    Payment_Terms,
    Delivery_Time_Days,
    Quality_Score,
    Vendor_Score,
    Contract_Status,
    Risk_Level,
    Outlier_Flag
)
SELECT
    c.Purchase_ID,
    m.clean_vendor_name,
    c.Category,
    c.Sub_Category,
    c.Spend_Amount_INR,
    c.Purchase_Date,
    FORMAT(c.Purchase_Date, 'yyyy-MM'),
    c.Region,
    c.Payment_Terms,
    c.Delivery_Time_Days,
    c.Quality_Score,
    c.Vendor_Score,

    CASE
        WHEN c.Vendor_Score >= 75 AND c.Quality_Score >= 7 THEN 'Contract'
        ELSE 'Non-Contract'
    END,

    CASE
        WHEN c.Vendor_Score < 50 OR c.Quality_Score < 5 THEN 'HIGH'
        WHEN c.Vendor_Score BETWEEN 50 AND 70 THEN 'MEDIUM'
        ELSE 'LOW'
    END,

    CASE
        WHEN c.Spend_Amount_INR > (s.avg_spend + 2 * s.std_spend)
        THEN 'YES'
        ELSE 'NO'
    END
FROM analytics.procurement_clean_gate c
LEFT JOIN analytics.vendor_normalization_map m
    ON c.Vendor_Name = m.raw_vendor_name
CROSS JOIN stats s;
GO

-- ==========================================================
-- 12. PIPELINE RUN AUDIT
-- ==========================================================

INSERT INTO analytics.project_log (action_type, action_description)
VALUES (
    'PIPELINE_RUN',
    CONCAT(
        'Staging Rows: ',
        (SELECT COUNT(*) FROM staging.stg_procurement_raw),
        ' | Fact Rows: ',
        (SELECT COUNT(*) FROM analytics.fact_procurement_spend)
    )
);
GO

-- ==========================================================
-- 13. EXECUTIVE VALIDATION QUERIES
-- ==========================================================

SELECT
    (SELECT COUNT(*) FROM staging.stg_procurement_raw) AS Total_Rows,
    (SELECT COUNT(*) FROM analytics.procurement_clean_gate) AS Clean_Rows,
    (SELECT COUNT(*) FROM analytics.procurement_rejects) AS Rejected_Rows,
    (SELECT COUNT(*) FROM analytics.fact_procurement_spend) AS Fact_Rows;
GO

SELECT
    Purchase_Month,
    SUM(Spend_Amount_INR) AS Total_Spend_INR,
    COUNT(DISTINCT Clean_Vendor_Name) AS Active_Vendors,
    SUM(CASE WHEN Risk_Level = 'HIGH' THEN 1 ELSE 0 END) AS High_Risk_Orders
FROM analytics.fact_procurement_spend
GROUP BY Purchase_Month
ORDER BY Purchase_Month;
GO

-- ==========================================================
-- END OF SCRIPT
-- ==========================================================
