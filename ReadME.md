
# Week 9 Project: Bikeshare Analytics Pipeline — From Batch to Real-Time

## **Problem Statement**
You've been hired by a micromobility company in the United States to help analyze rider behavior and improve operational efficiency. You've been given real bikeshare trip data from **Capital Bikeshare** for **December 2022**, https://s3.amazonaws.com/capitalbikeshare-data/202212-capitalbikeshare-tripdata.zip

This data contains over **175,000 trips**, and your task is to build a data pipeline that powers both **weekly performance reports** and **real-time monitoring** for high-priority activity. Operations teams want alerts when:
- A casual rider starts a trip at midnight
- Any ride lasts longer than 45 minutes
---
## **Dataset columns**:
- Duration – Duration of trip
- Start Date – Includes start date and time
- End Date – Includes end date and time
- Start Station – Includes starting station name and number
- End Station – Includes ending station name and number
- Bike Number – Includes ID number of bike used for the trip
- Member Type – Indicates whether user was a "registered" member (Annual Member, - 30-Day Member or Day Key Member) or a "casual" rider (Single Trip, 24-Hour - Pass, 3-Day Pass or 5-Day Pass)
- This data has been processed to remove trips that are taken by staff as they service and inspect the system, trips that are taken to/from any of our “test” stations at our warehouses and any trips lasting less than 60 seconds (potentially false starts or users trying to re-dock a bike to ensure it's secure).
---

## Expected Outcome
A pipeline that:
- Ingests and cleans the trip data
- Stores weekly aggregates in partitioned Parquet format
- Streams "flagged" rides in real time
- Is fully containerized with Docker

---

## What You Must Do

### 1. Define Your Tech Stack
You should use the following technologies:

| **Purpose**                    | **Required Tool**                                                                 |
|-------------------------------|------------------------------------------------------------------------------------|
| Containerization            | **Docker** — Package and deploy your pipeline components as isolated containers.  |
| Workflow Orchestration      | **Apache Airflow** — Schedule your batch ETL job to run every Monday at 10am UTC. |
| Batch Processing            | **Pandas** — Read, clean, and transform the bikeshare dataset efficiently.        |
| Storage Format              | **Parquet** — Save transformed data in a partitioned, columnar format.            |
| Real-Time Streaming (Simulated) | **Python generator** (Kafka-style simulation) — Stream and log flagged events.     |
| Deployment Management       | **Docker Compose** — Run and manage multi-container apps during testing.          |

Be ready to explain:
- Why each tool fits its role
- How you containerized and orchestrated your components

### 2. Share a High-Level Architecture Diagram
Include a visual overview showing:
- Data ingestion ➝ cleaning ➝ Parquet output
- Real-time stream processor ➝ logs/alerts
- Docker containers for modularity

### 3. Deliverables
- Cleaned dataset stored in partitioned Parquet format (e.g., by user type and week)
- ETL logic using Airflow DAG
- Real-time processor that logs:
  - Trips > 45 minutes
  - Casual riders starting at midnight
- A `Dockerfile` and `docker-compose.yml` to containerize the pipeline
- Airflow batch job scheduled every Monday at 10am UTC. (Ideally, this job would ingest new real-time data each week. However, for the purpose of this project, it will repeatedly ingest and process the same December 2022 dataset at the scheduled time.)

---

## Bonus Challenge
Data Visualization: Produce a usage heatmap using `start_lat` and `start_lng`.

---

## Submission
- Create a repository in https://github.com/Data-Epic, then create a `dev` branch and add your codes there.
- Include a README and screenshots of logs or DAGs

---

## Deadline
**Thursday, May 1st, 2025 — 7:59 PM WAT**

https://github.com/andrejnevesjr/airflow-spark-minio-postgres/blob/master/docker-compose.yml


## Resources that helps me to correct some common errors when setting up all my applications
https://www.youtube.com/watch?v=tRlEctAwkk8
https://stackoverflow.com/questions/77355287/adding-local-minio-host-to-mc-configuration-failed-to-add-temporary-minio-ser
https://stackoverflow.com/questions/60193781/postgres-with-docker-compose-gives-fatal-role-root-does-not-exist-error
https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
https://stackoverflow.com/questions/72222070/postgres-and-docker-compose-password-authentication-fails-and-role-postgres-d
Setup_Airflow python depencies : https://www.youtube.com/watch?v=0UepvC9X4HY&t=300s, command=docker build . -t dev_apache_airflow:2.8.4 
https://www.youtube.com/watch?v=WglsTKda6Gc


s3 - coonection https://www.youtube.com/watch?v=sVNvAtIZWdQ