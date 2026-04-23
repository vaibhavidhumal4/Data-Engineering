# Air Quality ETL Pipeline 

## Overview
A robust, scalable Data Engineering pipeline designed to extract, transform, and load (ETL) multi-year air quality data from an Indian government API into **Google BigQuery**. Built with Python, this project demonstrates production-ready data engineering workflows, including asynchronous data extraction, complex transformation logic using staging tables, and cloud-native data warehousing.

## Architecture & Workflow
1. **Extract:** Utilizes Python's asynchronous capabilities (`asyncio`) to efficiently pull high-volume, historical, and real-time data from the government API, drastically reducing data ingestion time.
2. **Transform:** Processes raw JSON responses, handles missing or malformed values, normalizes data structures, and prepares staging tables. Ensures data integrity and strict schema compliance before the final load.
3. **Load:** Securely upserts the cleaned, structured data into **Google BigQuery**, establishing a reliable single source of truth for scalable querying and downstream analytics.

## Tech Stack
* **Language:** Python 3.x (Asyncio, Pandas, Requests)
* **Cloud Platform:** Google Cloud Platform (GCP)
* **Data Warehouse:** Google BigQuery
* **Database/Querying:** SQL 
* **Design Patterns:** Asynchronous Processing, Staging Tables, REST API Integration

## Key Features
* **High-Performance Extraction:** Leverages asynchronous API calls to process multi-year datasets in a fraction of the time compared to synchronous scripts.
* **Resilient Data Transformation:** Implements comprehensive data cleaning and staging logic to prevent dirty data from corrupting the production warehouse.
* **Cloud-Native Storage:** Utilizes BigQuery for highly scalable, serverless enterprise data warehousing.
* **Modular Codebase:** Code is structured for maintainability, allowing for seamless updates to API endpoints, transformation rules, or destination schemas.

## Prerequisites
* Python 3.8+
* Google Cloud Platform (GCP) Account with BigQuery enabled
* GCP Service Account Key (`.json` file) with BigQuery Editor permissions
* Access Key/Credentials for the source API
