# Financial DataOps ETL Pipeline (Bronze–Silver–Gold Architecture)

## Overview

This project implements an end-to-end **DataOps ETL pipeline** using AWS services and Infrastructure as Code.
The pipeline ingests financial transaction data from Amazon S3, processes it through a **Bronze → Silver → Gold** data lake architecture using AWS Glue (PySpark), and orchestrates workflows using AWS Step Functions.

Infrastructure is fully automated using Terraform and deployed through a CI/CD pipeline with GitHub Actions.

---

## Architecture

The pipeline follows a modern **DataOps architecture**:

1. Raw financial transaction data is stored in Amazon S3.
2. AWS Step Functions orchestrates ETL jobs.
3. AWS Glue jobs perform transformations using PySpark.
4. Processed data is stored in Bronze, Silver, and Gold layers.
5. EventBridge schedules the workflow daily.
6. Terraform provisions all infrastructure.
7. GitHub Actions performs CI/CD deployment.

---

## AWS Services Used

* **Amazon S3** – Data lake storage
* **AWS Glue** – ETL processing with PySpark
* **AWS Step Functions** – Workflow orchestration
* **Amazon EventBridge** – Cron scheduling
* **AWS IAM** – Access control
* **Terraform** – Infrastructure as Code
* **GitHub Actions** – CI/CD pipeline

---

## Data Lake Architecture

### Bronze Layer

* Raw data ingestion
* Data stored as-is from source
* Minimal transformation

### Silver Layer

* Data cleaning
* Schema standardization
* Null handling and filtering

### Gold Layer

* Aggregated business-ready datasets
* Analytics and reporting ready

---

## Project Structure

```
financial-dataops-pipeline
│
├── terraform
│   ├── provider.tf
│   ├── iam.tf
│   ├── glue_jobs.tf
│   ├── step_function.tf
│   ├── scheduler.tf
│   └── variables.tf
│
├── etl_scripts
│   ├── bronze_etl.py
│   ├── silver_etl.py
│   └── gold_etl.py
│
├── .github/workflows
│   └── deploy.yml
│
└── README.md
```

---

## ETL Workflow

The Step Function orchestrates the pipeline in the following order:

```
Bronze Glue Job
      ↓
Silver Glue Job
      ↓
Gold Glue Job
```

Each job reads data from the previous layer and writes processed data to the next layer.

---


## CI/CD Pipeline

The CI/CD workflow is implemented using **GitHub Actions**.

### Pipeline Steps

1. Code pushed to GitHub repository
2. GitHub Actions workflow triggers
3. Terraform initializes infrastructure
4. Terraform plans changes
5. Terraform applies updates
6. AWS resources are provisioned or updated
7. Step Function workflow becomes available

---

## Terraform Resources Provisioned

Terraform automatically provisions:

* S3 buckets for data storage
* AWS Glue jobs for Bronze, Silver, and Gold layers
* AWS Step Functions workflow
* EventBridge cron scheduler
* IAM roles and policies
* Logging permissions

---


