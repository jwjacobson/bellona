# bellona - a universal data ontology platform
![Tests](https://github.com/jwjacobson/bellona/actions/workflows/tests.yaml/badge.svg)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/python-3.14-blue)](https://www.python.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Bellona is a universal data ontology platform inspired by tools like Palantir -- built for developers who want to ingest heterogeneous data, map it to a dynamic semantic layer, and query it meaningfully. You define an ontology at runtime (not in code), AI agents propose how your data maps to it, and you confirm or reject those proposals before anything is stored. The current implementation is built with FastAPI, SQLAlchemy (async), PostgreSQL with JSONB, and Agno for multi-agent orchestration, with an htmx/Alpine.js frontend planned for a future phase.

## Setup
### Prerequisites
- PostgreSQL running locally
- Python 3.14+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed
- A valid [Claude API key](https://platform.claude.com/dashboard)

### Installation
Download bellona and install dependencies:
```bash
git clone git@github.com:jwjacobson/bellona.git
cd bellona
uv sync
```

Set up your environment variables:
```bash
cp .env.example .env
# Adjust defaults as needed and add your API key
```

Set up the database:
```bash
psql -U postgres -c "CREATE USER bellona WITH PASSWORD 'bellona';"
psql -U postgres -c "CREATE DATABASE bellona OWNER bellona;"
uv run alembic upgrade head
```

## Manually calling the endpoints

Currently, Bellona has been built through Phase 3 of the [development roadmap](SPEC.md#10-development-roadmap), which means two things:
1. You can test its current functionality by uploading a csv and transforming it into queryable data; but
2. You have to call each endpoint manually because there's no UI yet.

To try it out (using the provided dummy `companies.csv` as input):

### 0. Start the server
```bash
uv run fastapi dev src/bellona/main.py
# Or, if you have just installed, `just run`
```

Then, in another terminal:
### 1. Upload a CSV
```bash
curl -X POST http://localhost:8000/api/v1/connectors/csv/upload \
  -F "file=@companies.csv" \
  -F "name=companies"
```
The `id` in this response is your **connector ID**.

### 2. Propose a schema
Ask the Schema Agent to analyze the CSV and propose an entity type:
```bash
curl -X POST http://localhost:8000/api/v1/schema/propose \
  -H "Content-Type: application/json" \
  -d '{"connector_id": "{CONNECTOR_ID}"}'
```
The agent returns a proposed entity type with property definitions, data types, and a confidence score. The `id` in this response is your **proposal ID**. 

### 3. Confirm the schema proposal
```bash
curl -X POST http://localhost:8000/api/v1/proposals/{PROPOSAL_ID}/confirm
```
This creates the entity type in the database. The `id` in this response is the **entity type ID**.

### 4. Propose field mappings
Ask the Mapper Agent how the CSV columns map to the new entity type's properties:
```bash
curl -X POST http://localhost:8000/api/v1/mappings/propose \
  -H "Content-Type: application/json" \
  -d '{
    "connector_id": "{CONNECTOR_ID}",
    "entity_type_id": "{ENTITY_TYPE_ID}"
  }'
```
The agent returns per-field mappings with confidence scores and reasoning. The `id` in this response is a *new* **proposal ID**: the ID of the mapping proposal.

### 5. Confirm the mapping proposal
```bash
curl -X POST http://localhost:8000/api/v1/proposals/{PROPOSAL_ID}/confirm
```

### 6. Ingest the data
Trigger a sync job to load the CSV rows through the confirmed mapping:
```bash
curl -X POST http://localhost:8000/api/v1/connectors/{CONNECTOR_ID}/sync
```
The `id` in the response is the **job ID**.

### 7. Verify the data
Check the ingestion job status:
```bash
curl http://localhost:8000/api/v1/ingestion-jobs/{JOB_ID}
```
Query the data directly in PostgreSQL:
```bash
psql -U bellona -d bellona
```
Then:
```sql
SELECT id, properties->>'name' AS name, properties->>'industry' AS industry,
       (properties->>'employee_count')::int AS employees
FROM entities
WHERE entity_type_id = '{ENTITY_TYPE_ID}'
ORDER BY (properties->>'employee_count')::int DESC;
```


## License

Bellona is free software released under the [GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0.en.html). You are free to use, modify, and distribute it under the same terms.