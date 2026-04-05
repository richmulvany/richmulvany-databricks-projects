HR Analytics Chatbot (Databricks + LangChain)

Overview

This project demonstrates the design and implementation of a modern data product built on Databricks, exposed via an AI-powered chatbot interface using LangChain.

The goal is to simulate a real-world enterprise scenario:

«Transform raw HR data into a governed, trusted data product, and enable natural language access via an external AI service.»

The solution follows a Medallion Architecture (Bronze → Silver → Gold) and integrates:

- Data contracts & schema governance
- Data quality enforcement
- Metadata & lineage considerations
- External API access via a containerised backend
- Natural language querying using LLMs

---

Architecture

                ┌────────────────────────────┐
                │      Raw HR Dataset        │
                └────────────┬───────────────┘
                             ↓
                    ┌─────────────────┐
                    │     Bronze      │
                    │ Ingestion Layer │
                    └────────┬────────┘
                             ↓
                    ┌─────────────────┐
                    │     Silver      │
                    │ Cleaned + Typed │
                    └────────┬────────┘
                             ↓
                    ┌─────────────────┐
                    │      Gold       │
                    │ Data Products   │
                    └────────┬────────┘
                             ↓
              ┌──────────────────────────────┐
              │  FastAPI + LangChain Backend │
              └────────────┬─────────────────┘
                           ↓
                  Natural Language Queries

---

Key Features

Data Engineering

- Medallion architecture implemented in Databricks
- Automated ingestion and transformation pipelines
- Schema enforcement and type casting
- Derived feature engineering (e.g. tenure ratios, income metrics)

Data Contracts & Governance

- YAML-based data contract
- Versioned schema with evolution tracking
- Contract validation at ingestion (Bronze)
- Schema enforcement at transformation (Silver)
- Column-level metadata:
  - descriptions
  - sensitivity classification
  - allowed values
- Schema hashing for drift detection

Data Quality

- Validation checks:
  - Missing columns
  - Unexpected columns
  - Nullability violations
  - Domain constraints
  - Primary key uniqueness
- Centralised contract violations logging

Metadata & Observability

- Ingestion metadata:
  - "_ingestion_timestamp"
  - "_source_file"
  - "_contract_version"
  - "_load_id"
- Automatic column documentation applied to tables
- Generated markdown documentation for data products

Gold Layer (Data Products)

Designed specifically for analytics and AI consumption:

- "employees" – full enriched dataset
- "department_summary" – aggregated department metrics
- "job_summary" – role-level insights
- "kpi_metrics" – global KPIs

All metrics are rounded and structured for usability.

---

AI / Chatbot Integration

The project exposes the data product via an external backend:

- Built with FastAPI
- Uses LangChain to translate natural language → SQL
- Connects to Databricks SQL Warehouse
- Returns results directly from governed Gold tables

Example queries:

- “How many employees are in each department?”
- “What is the average salary in R&D?”
- “Which roles have the highest attrition?”

---

Tech Stack

- Databricks (Delta Lake, Unity Catalog)
- Python
- PySpark
- LangChain
- FastAPI
- Docker
- SQLAlchemy + Databricks SQL Connector

---

Running the Backend (Docker)

Build the container

docker build -t hr-chatbot .

Run the container

docker run --env-file .env -p 8000:8000 hr-chatbot

Access API

http://localhost:8000/docs

---

Environment Variables

Create a ".env" file:

OPENAI_API_KEY=...
DATABRICKS_HOST=...
DATABRICKS_TOKEN=...
DATABRICKS_WAREHOUSE_ID=...
DATABRICKS_CATALOG=...
DATABRICKS_SCHEMA=...

---

Repository Structure

hr-chatbot/
│
├── data-pipeline/        # Databricks notebooks (Bronze/Silver/Gold)
├── contracts/            # Data contracts (YAML)
├── backend/              # FastAPI + LangChain service
├── docs/                 # Generated dataset documentation
├── Dockerfile
├── requirements.txt
└── README.md

---

Key Design Decisions

Separate Gold Tables for AI Consumption

Instead of a single wide table, the Gold layer is split into:

- Entity-level data ("employees")
- Aggregated summaries ("department", "job")
- KPI layer ("metrics")

This improves:

- Query performance
- LLM reasoning
- Semantic clarity

---

External API Instead of Direct Notebook Querying

The chatbot is implemented as an external service, reflecting real enterprise patterns:

- Decouples compute from serving
- Enables scaling and deployment
- Demonstrates production-ready architecture

---

Contract-Driven Development

The pipeline is driven by a data contract, not just code:

- Prevents silent schema drift
- Documents expectations explicitly
- Enables governance-first data engineering

---

Future Improvements

- Row/column-level security & masking (PII handling)
- Vector search / RAG for richer context
- Frontend UI (Streamlit or React chatbot)
- CI/CD pipeline for automated deployment
- Data quality scoring (DQX integration)

---

What This Project Demonstrates

This project is designed to showcase:

- End-to-end data engineering capability
- Understanding of data governance and contracts
- Ability to build production-style data products
- Integration of modern AI tooling into data platforms

---

Author

Richard Mulvany
Data Engineer | Data Science MSc | Interested in Data Platforms & AI Integration