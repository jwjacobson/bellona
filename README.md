# bellona - a universal data ontology platform
![Tests](https://github.com/jwjacobson/bellona/actions/workflows/tests.yaml/badge.svg)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/python-3.14-blue)](https://www.python.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

Bellona is a universal data ontology platform inspired by tools like Palantir -- built for developers who want to ingest heterogeneous data, map it to a dynamic semantic layer, and query it meaningfully. You define an ontology at runtime (not in code), AI agents propose how your data maps to it, and you confirm or reject those proposals before anything is stored. It is built with FastAPI, SQLAlchemy (async), PostgreSQL with JSONB, and Agno for multi-agent orchestration, with an htmx/Alpine.js frontend.

## Setup
At present, Bellona is not deployed to the web, so you will have to run it locally.

### Prerequisites
- PostgreSQL running locally
- Python 3.14+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed (not strictly necessary, but these instructions assume it)
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

## Running
Start the server:
```bash
uv run fastapi dev src/bellona/main.py

# Or, if you have just installed:
just run
```

### Using the web interface
In a web browser, navigate to `http://127.0.0.1:8000`. This will take you to the **Ontology** page, which will be empty if you haven't discovered any data.

#### Parts of the app

| |  |
|-|-|
| **Ontology** | Browse discovered entity types, field definitions, and relationships. |
| **Explorer** | View ingested entities in tabular form, organized by type. |
| **Connectors** | Configure data sources (CSV or REST API). Start here if you're new to Bellona. |
| **Proposals** | Review and confirm proposals from the Discovery, Schema, and Mapping agents. |
| **Query** | Ask questions about your data in natural language. |
| **Graph** | Visualize entities and their relationships graphically. |

#### Ingesting data
Bellona can currently ingest data from CSVs and REST APIs. To create a connector, upload a CSV or enter an API base url in the appropriate form on the **Connectors** page.

##### CSV connector
After you upload a CSV, you will be taken to the new connector's details page.

1. Click `Propose schema`; once a proposal has been made, follow the link to the Proposals page.
2. `Confirm` the schema proposal; you will be taken back to the connector details page.
3. Select the appropriate schema and click `Propose mapping`; once a proposal has been made, follow the link to the Proposals page.
4. `Confirm` the mapping proposal; you will be taken back to the connector details page.
5. Click `Trigger sync` to ingest the data from the file. You can now explore your data in the Explorer.

##### REST API connector
1. Enter the API's base url in the `API BASE URL` field, press `Discover API`, and wait for the Discovery agent to do its work.

> [!NOTE]
> The Discovery agent takes a long time (~2 minutes on the Star Wars API); if you know the configuration details of your desired API, you may wish to configure the connector manually in the dropdown below the Discover API button. Manual configuration skips the Discovery agent step.

2. Follow the link to the Proposals page and confirm the Discovery proposal; you will be redirected to the **Connectors** page.
3. Follow the `details` link for the connector of your choice. From here, the procedure continues from step 1 in the CSV connector instructions above.

### Manually calling the endpoints

You can also call the endpoints manually, though the process is rather involved.

To try it out (using `curl` and the provided dummy `companies.csv` as input):

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