# bellona - a universal data ontology platform
![Tests](https://github.com/jwjacobson/bellona/actions/workflows/tests.yaml/badge.svg)
[![FastAPI](https://img.shields.io/badge/FastAPI-005571)](https://fastapi.tiangolo.com/)
[![Python](https://img.shields.io/badge/python-3.14-blue)](https://www.python.org/)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![License: GPL v3](https://img.shields.io/badge/license-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

## Manually calling the endpoints

Currently, Bellona has been built through Phase 3 of the [development roadmap](SPEC.md#10-development-roadmap), which means two things:
1. You can test its current functionality by uploading a csv and transforming it into queryable data; but
2. You have to call each endpoint manually because there's no UI yet.

To try it out (using the provided dummy `companies.csv` as input):

### 0. Migrate the databse and start the server
```bash
uv run alembic upgrade head
uv run fastapi dev src/bellona/main.py # Or, if you have just installed, just run
```

### 1. Upload a CSV
```bash
curl -X POST http://localhost:8000/api/v1/connectors/csv/upload \
  -F "file=@companies.csv" \
  -F "name=companies"
```
#Note the `id` from the response — this is your **connector ID**.

### 2. Propose a schema
Ask the Schema Agent to analyze the CSV and propose an entity type:
```bash
curl -X POST http://localhost:8000/api/v1/schema/propose \
  -H "Content-Type: application/json" \
  -d '{"connector_id": ""}'
```
The agent returns a proposed entity type with property definitions, data types, and a confidence score. Note the proposal `id` and the `entity_type_id` after confirmation.

### 3. Confirm the schema proposal
```bash
curl -X POST http://localhost:8000/api/v1/proposals//confirm
```
This creates the entity type in the database.

### 4. Propose field mappings
Ask the Mapper Agent how the CSV columns map to the new entity type's properties:
```bash
curl -X POST http://localhost:8000/api/v1/mappings/propose \
  -H "Content-Type: application/json" \
  -d '{
    "connector_id": "",
    "entity_type_id": ""
  }'
```
The agent returns per-field mappings with confidence scores and reasoning.

### 5. Confirm the mapping proposal
```bash
curl -X POST http://localhost:8000/api/v1/proposals//confirm
```

### 6. Ingest the data
Trigger a sync job to load the CSV rows through the confirmed mapping:
```bash
curl -X POST http://localhost:8000/api/v1/connectors//sync
```

### 7. Verify the data
Check the ingestion job status:
```bash
curl http://localhost:8000/api/v1/ingestion-jobs/
```
Query the data directly in PostgreSQL:
```sql
SELECT id, properties->>'name' AS name, properties->>'industry' AS industry,
       (properties->>'employee_count')::int AS employees
FROM entities
WHERE entity_type_id = ''
ORDER BY (properties->>'employee_count')::int DESC;
```

### Other useful endpoints
Check data quality for an entity type:
```bash
curl -X POST http://localhost:8000/api/v1/quality/check/
```
List pending proposals:
```bash
curl http://localhost:8000/api/v1/proposals
```
Reject a proposal:
```bash
curl -X POST http://localhost:8000/api/v1/proposals//reject
```