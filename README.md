### Project Overview: Airflow ETL Pipeline with NASA Asteroids & Postgres

[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8%2B-blue)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.12-green)](https://python.org)
[![Postgres](https://img.shields.io/badge/PostgreSQL-13-blue)](https://postgresql.org)

This project builds a **production-grade ETL pipeline** using **Apache Airflow** to extract **NASA Near-Earth Object (NeoWs) data**, transform nested JSON, and load it into **PostgreSQL** — all orchestrated daily.


---

## Key Features

| Component | Description |
|---------|-------------|
| **Extract** | `HttpOperator` fetches today’s asteroid close approaches from NASA NeoWs API |
| **Transform** | Flattens nested JSON (diameters, velocities, hazards) using `@task` |
| **Load** | Bulk insert into Postgres with **idempotency** (`ON CONFLICT DO NOTHING`) |
| **Orchestration** | Airflow DAG with `@daily` schedule, error handling, and dependencies |
| **Environment** | Dockerized with `docker-compose.yml` (Postgres + Airflow via Astro CLI) |

---

## Why This Pipeline Stands Out

- **Real API Challenges**: Handled outages, rate limits, and nested data.
- **Variable Row Count**: Unlike APOD (1 row/day), NeoWs returns **N asteroids/day** → learned to process lists.
- **Idempotent Design**: Safe re-runs with unique constraints on `(asteroid_name, close_approach_date)`.
- **Modern Airflow**: Uses `HttpOperator`, `PostgresHook`, and TaskFlow API (Airflow 2.8+).

---

## Tech Stack

- **Apache Airflow** (via Astro CLI)
- **Python 3.12**
- **PostgreSQL 13**
- **NASA NeoWs API** (`https://api.nasa.gov`)
- **Docker & Docker Compose**

---

## Architecture & Workflow

The ETL pipeline is orchestrated in Airflow using a DAG (Directed Acyclic Graph). The pipeline consists of the following stages:

1. **Extract (E):**
The SimpleHttpOperator is used to make HTTP GET requests to NASA’s NeoWs API.
The response is in JSON format, containing fields like the title of the picture, the explanation, and the URL to the image.

2. **Transform (T):**
The extracted JSON data is processed in the transform task using Airflow’s TaskFlow API (with the @task decorator).
This stage involves extracting relevant fields like title, explanation, url, and date and ensuring they are in the correct format for the database.

3. **Load (L):**
The transformed data is loaded into a Postgres table using PostgresHook.
If the target table doesn’t exist in the Postgres database, it is created automatically as part of the DAG using a create table task.
