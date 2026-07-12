# RetailFlow Data Engineering Capstone

## Project Overview
An end-to-end retail data engineering pipeline built across 10 phases.

## Project Structure

- Phase 0 – Environmet Setup
- Phase 1 – Synthetic Data Generation & Profiling(Colab)
- Phase 2 – Boto3 Ingestion Utility(VS Code)
- Phase 3 – Data Quality
- Phase 4 – Data Transformation
- Phase 5 – Data Warehouse
- Phase 6 – Analytics
- Phase 7 – Dashboard
- Phase 8 – Orchestration
- Phase 9 – Final Delivery

## Technologies

- Python
- Pandas
- NumPy
- Faker
- SQL
- Apache Spark
- Power BI
- Git


retailflow-capstone/
│
├── README.md
├── requirements.txt
├── .gitignore
│
├── phase-01-data-generation/
│   ├── 01_data_generation.ipynb
│   ├── README.md
│   ├── data_profile_report.md
│   │
│   ├── data/
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   ├── orders_day1.json
│   │   ├── order_items_day1.json
│   │   ├── orders_day2.json
│   │   └── order_items_day2.json
│   │
│   └── charts/
│       ├── orders_per_day.png
│       ├── revenue_distribution.png
│       ├── top_categories.png
│       └── null_heatmap.png
│
├── phase-02-ingestion/
│   ├── 02_data_ingestion.ipynb
│   ├── README.md
│   ├── ingestion_report.md
│   └── screenshots/
│
├── phase-03-data-quality/
│   ├── 03_data_quality.ipynb
│   ├── README.md
│   └── quality_report.md
│
├── phase-04-transformations/
│   ├── 04_transformations.ipynb
│   ├── README.md
│   └── transformation_report.md
│
├── phase-05-data-warehouse/
│   ├── 05_data_warehouse.ipynb
│   ├── README.md
│   └── warehouse_design.md
│
├── phase-06-analytics/
│   ├── 06_analytics.ipynb
│   ├── README.md
│   └── analysis_report.md
│
├── phase-07-dashboard/
│   ├── README.md
│   ├── dashboard.pbix
│   ├── dashboard.pdf
│   └── screenshots/
│
├── phase-08-orchestration/
│   ├── 08_orchestration.ipynb
│   ├── README.md
│   └── workflow_diagrams/
│
└── phase-09-final-delivery/
    ├── README.md
    ├── final_report.pdf
    ├── presentation.pptx
    └── architecture.png