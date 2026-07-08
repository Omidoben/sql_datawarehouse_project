
# Data WareHouse & Data Analytics Project

###  Project Overview
This project involved the design, development, and implementation of a **Microsoft SQL Server** data warehouse, integrating business data from CRM (Customer Relationship Management) and ERP (Enterprise Resource Planning) systems. The primary objective was to consolidate these data sources into a unified, clean, and analytical-ready repository to enable comprehensive reporting, advanced analytics, and strategic decision-making.

###  Project Planning & Management
The entire project lifecycle, from initial ideation to deployment, was planned and tracked using Notion. This involved breaking down the project into six key epics, each with its own set of detailed tasks (sub-epics):

**Requirements Analysis:** Defining and documenting business needs and data requirements.

**Design Data Architecture:** Conceptualizing the data warehouse structure and modeling.

**Project Initialization:** Setting up the foundational environment and tools.

**Build Bronze Layer:** Implementing the raw data ingestion stage.

**Build Silver Layer:** Developing data cleansing, standardization, and integration processes.

**Build Gold Layer:** Creating the final business-ready dimensional models and analytical views.

### 🔗 Data Sources
Data for this project was extracted from the following simulated operational systems:

**CRM System:** Provided core customer information, product details, and sales transaction data.

`crm_cust_info (Customer demographics)`

`crm_prd_info (Product details)`

`crm_sales_details (Sales orders, quantities, prices)`

**ERP System:** Contributed supplementary customer and product classification data.

`erp_loc_a101 (Customer country information)`

`erp_cust_az12 (Customer birthdate and gender)`

`erp_px_cat_g1v2 (Product categories and subcategories)`

###  Data Warehouse Architecture  ETL Process
This data warehouse project implemented a Medallion Architecture pattern (Bronze, Silver, Gold layers) within a Microsoft SQL Server environment. This approach ensures progressive data quality improvement and logical separation of concerns:

**Bronze Layer:** Stores exact, immutable copies of source data.

**Silver Layer:** Holds cleansed, standardized, and conformed data, ready for enterprise-wide consumption.

**Gold Layer:** Contains highly optimized, aggregated, and business-friendly data structured for direct analytical consumption (Star Schema).

`The Extract, Transform, Load (ETL) process for this data warehouse was developed using T-SQL stored procedures within SQL Server. The process is executed in distinct stages, mirroring the Medallion Architecture.`


###  Exploratory Data Analysis (EDA)

After the Gold layer, Exploratory Data Analysis (EDA) was performed to ensure data integrity, consistency, and to gain initial insights.

**Tools Used:**

SQL Server Management Studio (SSMS) for direct SQL queries.

**Key Areas of Exploration:**

**Dimension Analysis:** Handling missing values (e.g., gender fallback from ERP)

**Date Range Checks:** Validating temporal coverage of data

**Measure Aggregation:** Summarizing sales trends, volumes

**Ranking & Magnitude Analysis:** Identifying top-performing products, customers, and regions

###  Advanced Analytics

Building upon the insights from EDA, a series of advanced analytical techniques were applied to uncover deeper patterns and provide actionable intelligence.

**1. Change Over Time Analysis:**

`Purpose:` To track trends, growth, and changes in key metrics (e.g., sales, quantity sold) over time.

**2. Cumulative Analysis:**

`Purpose:` To calculate running totals or moving averages for key metrics, tracking performance over time cumulatively.

**3. Performance Analysis (Year-over-Year, Month-over-Month):**

`Purpose:` To measure the performance of products, customers, or regions against previous periods.

**4. Data Segmentation:**

`Purpose:` To group data into meaningful categories based on specific criteria for targeted insights.

**5. Part-to-Whole Analysis:**

`Purpose:` To analyze how an individual part (e.g., a specific product category) is performing compared to the overall business, understanding its proportional impact.

###  Reporting & Insights
The results of the advanced analytics were consolidated into two comprehensive reports, designed to provide business users with readily consumable insights.

**i) Customer Report:** Consolidates key customer metrics and behaviors to understand customer spending and engagement.

**ii) Product Report:** Consolidates information about products and their key performance metrics.

###  Power BI Dashboards

Two Power BI dashboards were built on top of the Gold layer to visualize the customer and sales insights described above.

**Customer Performance Dashboard**
![Customer Performance Dashboard](PowerBi%20Dashboards/CustomerPerformanceDashboard.jpg)

**Sales Performance Dashboard**
![Sales Performance Dashboard](PowerBi%20Dashboards/SalesPerformanceDashboard.jpg)

📂 Repository Structure

```bash
.
├── data-warehouse-project/             # Main data warehouse development project
│   ├── datasets/                       # Raw datasets used for the project (ERP and CRM data)
│   │   ├── crm_cust_info.csv
│   │   ├── crm_prd_info.csv
│   │   ├── crm_sales_details.csv
│   │   ├── erp_loc_a101.csv
│   │   ├── erp_cust_az12.csv
│   │   └── erp_px_cat_g1v2.csv
│   │
│   ├── docs/                           # Project documentation and architecture details
│   │   ├── data_architecture.drawio
│   │   ├── data_catalog.md
│   │   ├── data_flow.drawio
│   │   ├── data_integration.drawio
│   │   ├── data_mart(data model).drawio
│   │
│   ├── scripts/                        # SQL scripts for ETL and transformations
│   │   ├── 00_database_setup/
│   │   │   └── create_database_schemas.sql
│   │   ├── bronze/
│   │   │   ├── ddl_bronze.sql
│   │   │   └── proc_load_bronze.sql
│   │   ├── silver/
│   │   │   ├── ddl_silver.sql
│   │   │   └── proc_load_silver.sql
│   │   └── gold/
│   │       └── ddl_gold.sql
│   │
│   ├── tests/                          # Test scripts and quality checks
│   │   ├── quality_checks_silver_layer.sql
│   │   └── quality_checks_gold_layer.sql
│   │
│   ├── README.md                       # Project overview and instructions
│   └── LICENSE                         # License information
│
└── DataAnalytics/                      # Project for EDA, Analytics & Reporting
    ├── EDA/                            # Exploratory Data Analysis files
    ├── AdvancedAnalytics/              # Advanced analytics SQL scripts
    └── Reports/                        # Final consolidated reports
        ├── customer_report.sql
        └── product_report.sql
└── PowerBi Dashboards/                 # Power BI dashboard screenshots
    ├── CustomerPerformanceDashboard.jpg
    └── SalesPerformanceDashboard.jpg
