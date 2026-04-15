# Data Warehouse Project (PostgreSQL + Docker)

## Overview
This project implements a complete **Data Warehouse pipeline** using **PostgreSQL and Docker**, following the modern **Bronze → Silver → Gold architecture**.

It is inspired by the **DataWithBaraa** YouTube series and adapted to PostgreSQL with production-style practices.

---

## Architecture

![ETL Architecture](docs/data_architecture.png)

###  Bronze Layer (Raw Ingestion)
- Source: CSV files (CRM & ERP systems)
- No transformations (raw data only)
- Fast loading using `COPY`
- Tables mirror source structure

---

### Silver Layer (Data Cleaning & Transformation)
- Data standardization & type casting
- Deduplication
- Handling nulls and inconsistencies
- Adds metadata:
  - `dwh_create_date`

---

### Gold Layer (Business Layer)
- Star schema (Dimensions + Facts)
- Optimized for analytics & BI tools
- Clean, enriched, and business-ready data

---

##  Project Structure
```
datawarehouse-project/
├── datasets/
│   ├── source_crm/
│   └── source_erp/
├── docs/
│   ├── ETL.png
│   ├── data_flow.png
│   ├── data_model.png
│   └── naming_conventions.md
├── scripts/
│   ├── bronze_layer/
│   ├── silver_layer/
│   ├── gold_layer/
│   └── init_database.sql
```

---


---


##  Tech Stack

- **PostgreSQL (Dockerized)**
- **PL/pgSQL (Stored Procedures)**
- **DBeaver (DB client)**
- **CSV data sources (CRM + ERP)**
- **BI-ready (Tableau / Power BI)**

---

## Key Skills & Concepts Applied

### Data Warehouse Architecture
- Designed **multi-layer architecture (Bronze → Silver → Gold)**
- Applied **separation of concerns**
- Built scalable and maintainable pipeline

### ETL Pipeline (SQL-based)

#### Bronze Layer
- Raw ingestion using `COPY`
- No transformations
- Mirrors source systems

#### Silver Layer
- Data cleaning & normalization
- Deduplication using:
  ```sql
  ROW_NUMBER() OVER (PARTITION BY ...)
  ```
  - Standardization using CASE
- Handling missing values with COALESCE
- Data validation and correction

#### Gold Layer
- Star schema modeling
- Business-ready views
- Dimension enrichment
  
---

## Challenges Faced
- Migrating SQL Server logic to PostgreSQL
- Docker container conflicts and permissions
- File ingestion issues (`COPY` vs paths)
- Understanding window functions and SCD logic


## How to Run

1. Start Docker:
```
docker run --name postgres-dwh ...
```

2. Load Bronze data:
```
CALL bronze.load_bronze();
```

3. Transform to Silver:
```
CALL silver.load_silver();
```

4. Query Gold layer:
```
SELECT * FROM gold.fact_sales;
```

---

## Tableau Usage

Connect Tableau to PostgreSQL:
- Host: localhost
- Port: 5432
- Database: postgres

Use:
- `gold.dim_customers`
- `gold.dim_products`
- `gold.fact_sales`

---

## Author
Mariia Shmidt

Inspired by: DataWithBaraa

