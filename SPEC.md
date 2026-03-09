# Bellona: Universal Data Ontology Platform

**Technical Specification v1.0**

Jeff Jacobson — March 2026

---

## 1. Executive Summary

Bellona is a universal data ontology platform that ingests data from heterogeneous sources, maps it through a dynamic ontology layer, and stores it in a queryable database. The system uses AI agents to assist with schema inference, data mapping, and natural language querying.

### 1.1 Core Value Proposition

Organizations deal with data scattered across dozens of systems in incompatible formats. Bellona provides a single platform that can connect to any data source, understand its structure, map it to a unified ontology, and make it queryable through both programmatic APIs and natural language. This is the core capability that platforms like Palantir Foundry provide to enterprise customers.

### 1.2 Design Principles

- **Dynamic over static:** The ontology is defined at runtime, not in code. The system adapts to new data shapes without redeployment.
- **Agents advise, humans decide:** AI agents suggest mappings and schemas, but a human confirms before data is written (v1).
- **Connectors are pluggable:** New data sources are added by implementing a connector interface, not by modifying core logic.
- **Local-first, cloud-ready:** Runs on a single machine for development and demo, but architected for cloud deployment.

### 1.3 Key Decisions

| Decision | Choice |
|----------|--------|
| Framework | FastAPI + Pydantic + SQLAlchemy (async) |
| Database | PostgreSQL with JSONB for dynamic entity properties |
| Agent Framework | Agno (multi-agent pattern) |
| Frontend | htmx + Alpine.js (secondary priority) |
| Auth | Single-user for v1, architected for multi-tenant |
| Deployment | Local-first, designed for cloud migration |

---

## 2. System Architecture

Bellona is organized as a four-stage pipeline with an AI agent layer that spans across stages. Data flows from connectors through the ontology layer into storage, then out through the query layer.

### 2.1 Architecture Overview

The system comprises five major components:

1. **Connectors** pull raw data from external sources and normalize it into a common intermediate format.
2. **Agent Layer** uses specialized AI agents to assist with schema inference, data mapping, quality checking, and natural language querying.
3. **Ontology Layer** validates and transforms incoming data against user-defined entity types, properties, and relationships.
4. **Storage Layer** persists ontology metadata relationally and entity data as validated JSONB in PostgreSQL.
5. **Query/API Layer** exposes a REST API, natural language query interface, and data explorer UI.

### 2.2 Data Flow

- **Ingest:** A connector pulls raw data from an external source (CSV file, REST API) and emits records in a normalized intermediate format (list of dictionaries with metadata).
- **Analyze:** The Mapper Agent examines the incoming data schema and proposes how fields map to existing ontology entity types and properties. If no suitable type exists, the Schema Agent proposes new entity types.
- **Confirm:** The user reviews and approves or modifies the agent's proposed mappings via the UI or API.
- **Validate:** The ontology layer validates each record against the confirmed mapping, applying type coercion, required-field checks, and constraint validation.
- **Store:** Valid entities are written to PostgreSQL. Ontology metadata goes into relational tables; entity data goes into the entities table with a JSONB properties column.
- **Index:** The system creates or updates targeted GIN indexes on frequently queried JSONB paths.
- **Query:** The data is now available through the REST API, the natural language agent, and the UI explorer.

---

## 3. Connector System

Connectors are pluggable adapters that handle the specifics of communicating with external data sources. Each connector implements a common interface, allowing the rest of the system to treat all data sources uniformly.

### 3.1 Connector Interface

Every connector must implement the following contract:

- **connect():** Establish a connection or validate access to the data source. Returns a connection status object.
- **discover_schema():** Introspect the data source and return a description of available fields, types, and structure. This is what the Mapper Agent uses to propose ontology mappings.
- **fetch_records(options):** Pull data from the source and yield records in the normalized intermediate format. Supports pagination, filtering, and incremental fetches where the source allows.
- **get_metadata():** Return connector-level metadata including source name, last sync timestamp, record count, and connection health.

### 3.2 Intermediate Record Format

All connectors emit records in a common format, regardless of the source. This decouples source-specific logic from the ontology layer:

- Each record is a flat or shallow-nested dictionary of field names to values.
- Records include a `_source` metadata block with connector ID, source identifier, and fetch timestamp.
- Field values are Python-native types (str, int, float, bool, datetime, None). Type coercion to ontology property types happens downstream.
- For sources with nested or hierarchical data (e.g., JSON APIs), the connector is responsible for flattening or splitting into multiple record types, documented in its schema discovery output.

### 3.3 v1 Connectors

#### 3.3.1 CSV File Connector

Handles upload and parsing of CSV files. This is the simplest connector and the primary demo path.

- Accepts file upload via the API (multipart form data).
- Uses Python's csv module with dialect sniffing for delimiter detection.
- Schema discovery reads the header row and samples the first N rows to infer field types (string, numeric, date, boolean).
- Handles common edge cases: BOM markers, mixed line endings, quoted fields with embedded delimiters, encoding detection via chardet.

#### 3.3.2 REST API Connector

A configurable connector for pulling data from external REST APIs. The user provides connection configuration, and the system handles fetching and normalization.

- Configuration includes: base URL, authentication method (API key, OAuth2, bearer token), pagination strategy (offset, cursor, link-header), rate limiting parameters, and a JSONPath expression for extracting records from the response.
- Supports scheduled fetches (cron-style) and on-demand pulls.
- Schema discovery makes a sample request and infers structure from the response.
- Handles nested JSON responses by flattening to dot-notation keys (e.g., address.city) or splitting into separate record types based on user configuration.

### 3.4 Future Connectors

The connector interface is designed to accommodate additional sources without modifying core system code. Planned connectors include:

- **PostgreSQL connector:** Direct database-to-database ingestion via SQL queries or full table replication.
- **Webhook listener:** Receives pushed data from external systems in real-time.
- **S3/object storage:** Pulls files (CSV, JSON, Parquet) from cloud storage buckets.
- **GraphQL connector:** For APIs that expose a GraphQL endpoint rather than REST.

---

## 4. Ontology Layer

The ontology layer is the core of Bellona. It provides a dynamic, runtime-defined schema that describes entity types, their properties, and their relationships. The ontology is itself stored as data in the database, not as code, which means it can evolve without redeployment.

### 4.1 Core Concepts

#### 4.1.1 Entity Types

An entity type is a named category of things in the ontology (e.g., Company, Person, Transaction). Each entity type has a unique name, an optional description, and a set of property definitions. Entity types are created at runtime by users or proposed by the Schema Agent.

#### 4.1.2 Property Definitions

Each entity type has a list of property definitions that describe the shape of its data. A property definition includes:

- **name:** The property identifier (e.g., "founded_date", "employee_count").
- **data_type:** One of: string, integer, float, boolean, date, datetime, enum, json. The ontology layer coerces incoming values to the target type during validation.
- **required:** Whether this property must be present on every entity of this type.
- **constraints:** Optional validation rules such as min/max values, regex patterns, enum allowed values, or uniqueness.
- **description:** Human-readable description of what this property represents.

#### 4.1.3 Relationship Types

Relationships connect entities to each other. A relationship type defines:

- **name:** The relationship identifier (e.g., "employed_by", "acquired").
- **source_entity_type:** The entity type on the "from" side.
- **target_entity_type:** The entity type on the "to" side.
- **cardinality:** One of: one-to-one, one-to-many, many-to-many.
- **properties:** Optional properties on the relationship itself (e.g., "start_date" on an employment relationship).

### 4.2 Ontology Operations

The ontology layer exposes the following operations through the API:

- **Create entity type:** Define a new entity type with its property definitions.
- **Modify entity type:** Add, remove, or update property definitions. Modifications to existing properties must be backward-compatible (e.g., you can make a required field optional but not vice versa without a migration).
- **Create relationship type:** Define a new relationship between two entity types.
- **Validate record:** Check an incoming record against an entity type definition. Returns validation results with specific errors per field.
- **Map record:** Apply a confirmed field mapping to transform an intermediate record into a validated entity.

### 4.3 Schema Versioning

Every modification to an entity type increments its schema version. Existing entities retain a reference to the schema version they were created under. This allows the system to:

- Track how the ontology has evolved over time.
- Handle entities that were valid under a previous schema version but might not conform to the current one.
- Support eventual migration tooling that can upgrade entities from one schema version to another.

---

## 5. Storage Layer

Bellona uses PostgreSQL as its sole database, with a hybrid storage strategy: relational tables for ontology metadata and system data, and JSONB columns for dynamic entity properties.

### 5.1 Database Schema

#### 5.1.1 Ontology Metadata Tables

**entity_types**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| name | VARCHAR | Unique entity type name |
| description | TEXT | Human-readable description |
| schema_version | INTEGER | Current schema version, incremented on modification |
| created_at | TIMESTAMPTZ | Creation timestamp |
| updated_at | TIMESTAMPTZ | Last modification timestamp |

**property_definitions**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| entity_type_id | UUID (FK) | References entity_types.id |
| name | VARCHAR | Property name |
| data_type | VARCHAR | One of: string, integer, float, boolean, date, datetime, enum, json |
| required | BOOLEAN | Whether this property is mandatory |
| constraints | JSONB | Validation rules (min, max, pattern, enum values, etc.) |
| description | TEXT | Human-readable description |
| schema_version | INTEGER | Schema version when this property was added/modified |

**relationship_types**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| name | VARCHAR | Relationship name (e.g., employed_by) |
| source_entity_type_id | UUID (FK) | Entity type on the "from" side |
| target_entity_type_id | UUID (FK) | Entity type on the "to" side |
| cardinality | VARCHAR | one-to-one, one-to-many, many-to-many |
| properties | JSONB | Property definitions for the relationship itself |

#### 5.1.2 Entity Data Tables

**entities**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| entity_type_id | UUID (FK) | References entity_types.id |
| properties | JSONB | Entity data, validated against the entity type schema |
| schema_version | INTEGER | Schema version this entity was validated against |
| source_connector_id | UUID (FK) | Which connector ingested this entity |
| source_record_id | VARCHAR | Original record identifier from the source system |
| created_at | TIMESTAMPTZ | Ingestion timestamp |
| updated_at | TIMESTAMPTZ | Last update timestamp |

**relationships**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| relationship_type_id | UUID (FK) | References relationship_types.id |
| source_entity_id | UUID (FK) | The entity on the "from" side |
| target_entity_id | UUID (FK) | The entity on the "to" side |
| properties | JSONB | Relationship properties (e.g., start_date) |
| created_at | TIMESTAMPTZ | Creation timestamp |

#### 5.1.3 System Tables

**connectors**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| type | VARCHAR | Connector type (csv, rest_api, etc.) |
| name | VARCHAR | User-defined connector name |
| config | JSONB | Connector-specific configuration (URLs, auth, etc.) |
| last_sync_at | TIMESTAMPTZ | Last successful sync timestamp |
| status | VARCHAR | active, paused, error |

**ingestion_jobs**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| connector_id | UUID (FK) | Which connector ran this job |
| status | VARCHAR | pending, running, completed, failed |
| records_processed | INTEGER | Number of records processed |
| records_failed | INTEGER | Number of records that failed validation |
| error_log | JSONB | Structured error details for failed records |
| started_at | TIMESTAMPTZ | Job start timestamp |
| completed_at | TIMESTAMPTZ | Job completion timestamp |

**field_mappings**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key |
| connector_id | UUID (FK) | Which connector this mapping belongs to |
| entity_type_id | UUID (FK) | Target entity type |
| mapping_config | JSONB | Field-to-property mapping rules |
| status | VARCHAR | proposed, confirmed, archived |
| proposed_by | VARCHAR | "agent" or "user" |

### 5.2 Indexing Strategy

The JSONB properties column requires targeted indexing to support performant queries. Bellona uses a layered indexing approach:

- **Default GIN index:** Every entity type's properties column gets a GIN index, enabling general-purpose JSONB queries (key existence, containment).
- **Targeted B-tree indexes:** For properties marked as frequently queried or used in filters, the system creates expression indexes on specific JSONB paths (e.g., `CREATE INDEX ON entities ((properties->>'name')) WHERE entity_type_id = '...'`).
- **Full-text search:** String properties can optionally be indexed for full-text search using PostgreSQL's tsvector/tsquery, with GIN indexes on the generated tsvector column.

Index creation is automated: when an entity type is created, the system generates the default GIN index. Targeted indexes can be added via the API as query patterns emerge.

---

## 6. Agent Layer

Bellona uses Agno to implement a multi-agent system where each agent has a focused responsibility and a small set of tools. Agents operate in an advisory capacity in v1: they propose actions that require human confirmation before execution.

### 6.1 Agent Architecture

The agent layer uses Agno's team pattern, where specialized agents are coordinated by the framework. Each agent is model-agnostic, supporting any LLM provider (OpenAI, Anthropic, Groq, local models). Agents communicate through Agno's built-in message passing and share access to the ontology metadata as a knowledge source.

### 6.2 Specialized Agents

#### 6.2.1 Mapper Agent

Examines incoming data source schemas and proposes field-to-property mappings against existing ontology types.

- **Input:** Schema discovery output from a connector (field names, inferred types, sample values).
- **Tools:** Read ontology entity types and property definitions, read sample records from the connector, propose field mappings.
- **Output:** A proposed field_mapping record with confidence scores per field, presented to the user for confirmation.
- Uses embedding similarity between source field names and ontology property names/descriptions as a first-pass heuristic, then reasons over sample values for type compatibility.

#### 6.2.2 Schema Agent

Proposes new entity types or property additions when incoming data doesn't fit existing ontology structures.

- **Input:** Unmapped fields from the Mapper Agent's output, or explicit user request to analyze a data source.
- **Tools:** Read current ontology types, propose new entity type, propose property additions to existing types.
- **Output:** Proposed entity type definitions or property additions, presented to the user for review.
- Considers naming conventions, data types, and relationships to existing entity types when making proposals.

#### 6.2.3 Quality Agent

Monitors data quality during and after ingestion.

- **Input:** Entity records during ingestion, or existing entities for periodic quality checks.
- **Tools:** Query entities, flag duplicates, suggest entity merges, report data quality metrics.
- **Output:** Quality reports with specific issues (suspected duplicates, missing required fields on older entities, outlier values).
- Runs both inline during ingestion (catching issues before write) and as a background process on existing data.

#### 6.2.4 Query Agent

Translates natural language questions into structured queries against the ontology.

- **Input:** A natural language question from the user (e.g., "Which companies founded after 2020 have more than 100 employees?").
- **Tools:** Read ontology metadata (to understand what's queryable), execute structured queries against the entities table, format and present results.
- **Output:** Query results in a readable format, along with the structured query that was executed (for transparency).
- Exposes the generated query so users can learn the query syntax and eventually write their own.

### 6.3 Human-in-the-Loop

All agent proposals go through a confirmation flow:

1. Agent generates a proposal (mapping, schema change, merge, etc.).
2. Proposal is persisted with status "proposed" and a confidence score.
3. User is notified and can review, modify, or reject the proposal.
4. On confirmation, the system executes the proposed action.
5. Confirmation history is logged for future agent training and improved suggestions.

Exception: The Query Agent executes read-only queries without confirmation, since it cannot modify data.

---

## 7. Query / API Layer

Bellona exposes three query interfaces, all backed by the same underlying query engine.

### 7.1 REST API

A FastAPI application providing comprehensive CRUD and query endpoints. All responses use Pydantic models for serialization and validation.

#### 7.1.1 Ontology Management Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/entity-types | Create a new entity type |
| GET | /api/v1/entity-types | List all entity types |
| GET | /api/v1/entity-types/{id} | Get entity type details with properties |
| PATCH | /api/v1/entity-types/{id} | Modify entity type (add/update properties) |
| POST | /api/v1/relationship-types | Create a new relationship type |
| GET | /api/v1/relationship-types | List relationship types |

#### 7.1.2 Entity Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/v1/entities | Query entities with filters |
| GET | /api/v1/entities/{id} | Get single entity with relationships |
| GET | /api/v1/entities/{id}/relationships | Get entity's relationships |
| POST | /api/v1/entities/query | Advanced structured query |

#### 7.1.3 Connector & Ingestion Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/connectors | Register a new connector |
| POST | /api/v1/connectors/{id}/sync | Trigger a sync job |
| POST | /api/v1/connectors/csv/upload | Upload a CSV file for ingestion |
| GET | /api/v1/ingestion-jobs/{id} | Get ingestion job status and results |

#### 7.1.4 Agent & Mapping Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | /api/v1/mappings/propose | Ask the Mapper Agent to propose mappings |
| POST | /api/v1/mappings/{id}/confirm | Confirm a proposed mapping |
| POST | /api/v1/query/natural | Natural language query via Query Agent |
| GET | /api/v1/proposals | List pending agent proposals |

### 7.2 Natural Language Query Interface

The Query Agent provides a conversational interface for exploring ontology data. Users submit questions in plain English, and the agent translates them into structured queries, executes them, and returns formatted results alongside the generated query.

This interface is accessible both through the REST API endpoint (/api/v1/query/natural) and through the frontend UI. The agent maintains conversational context within a session, allowing follow-up questions like "now filter those to just the ones in Boston."

### 7.3 Data Explorer UI

A lightweight frontend built with htmx and Alpine.js that provides:

- An ontology browser for viewing and editing entity types, properties, and relationships.
- A data table view for browsing entities by type, with column sorting, filtering, and pagination.
- A connector management panel for configuring data sources, reviewing agent proposals, and monitoring ingestion jobs.
- A natural language query input that sends requests to the Query Agent and displays results inline.
- A relationship graph visualization showing connections between entities (using a lightweight JS graph library such as Cytoscape.js or D3 force layout).

---

## 8. Project Structure

The project follows a src/ layout, the Python Packaging Authority's recommended structure for Python projects. This separates the installable package from project-level configuration and ensures tests always run against the installed package, not accidental local imports:

```
bellona/
    src/
        bellona/          # Installable Python package
            api/              # FastAPI routers and endpoint definitions
            core/             # Settings, config, dependencies
            models/           # SQLAlchemy models (database schema)
            schemas/          # Pydantic models (API request/response)
            services/         # Business logic layer
            ontology/         # Ontology validation and mapping engine
            connectors/       # Connector interface and implementations
            agents/           # Agno agent definitions and tools
            db/               # Database session, migrations (Alembic)
    templates/        # Jinja2 templates (htmx partials and pages)
    static/           # CSS, JS (Alpine.js), images
    tests/            # Mirrors src/bellona/ structure
    alembic/          # Database migration scripts
    pyproject.toml
    Dockerfile
    docker-compose.yml
```

---

## 9. Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| API Framework | FastAPI | Async HTTP API with automatic OpenAPI docs |
| Validation | Pydantic v2 | Request/response validation, ontology schema enforcement |
| ORM | SQLAlchemy 2.0 (async) | Database models and query building |
| Database | PostgreSQL 16+ | Primary data store with JSONB support |
| Migrations | Alembic | Database schema migrations |
| Agent Framework | Agno | Multi-agent orchestration, tool management |
| Task Queue | Celery + Redis (v1: optional) | Background ingestion jobs and scheduled syncs |
| Frontend | htmx + Alpine.js | Lightweight reactive UI without a JS build step |
| Containerization | Docker + Docker Compose | Local development and deployment packaging |
| Testing | pytest + pytest-asyncio | Unit and integration tests with async support |

---

## 10. Development Roadmap

### 10.1 Phase 1: Foundation

Project scaffolding, database schema, and core ontology operations.

- FastAPI project setup with async SQLAlchemy and Alembic.
- PostgreSQL database with all ontology metadata and entity tables.
- CRUD endpoints for entity types, property definitions, and relationship types.
- Ontology validation engine: validate records against entity type definitions.
- Basic test suite with pytest.

### 10.2 Phase 2: Connectors

Data ingestion pipeline with the two v1 connectors.

- Connector interface (abstract base class) definition.
- CSV connector with schema discovery and file upload.
- REST API connector with configurable auth, pagination, and scheduling.
- Ingestion job tracking (status, record counts, error logs).
- Integration tests covering the full ingest pipeline.

### 10.3 Phase 3: Agent Layer

AI-assisted mapping and schema suggestion using Agno.

- Agno integration and agent infrastructure setup.
- Mapper Agent with ontology-aware field mapping proposals.
- Schema Agent for new entity type suggestions.
- Human-in-the-loop confirmation flow (propose/review/confirm).
- Quality Agent for duplicate detection and data quality reports.

### 10.4 Phase 4: Query Layer

Comprehensive querying across all three interfaces.

- Structured query endpoint with filtering, sorting, and pagination on JSONB properties.
- Query Agent for natural language queries with conversational context.
- JSONB indexing automation based on query patterns.
- Relationship traversal queries (graph-style lookups).

### 10.5 Phase 5: Frontend

Lightweight UI for ontology management, data exploration, and agent interaction.

- htmx + Alpine.js application scaffold with Jinja2 templates.
- Ontology browser (view/edit entity types and relationships).
- Data table explorer with filtering and pagination.
- Connector management panel and ingestion monitoring.
- Natural language query interface.
- Relationship graph visualization.

### 10.6 Future Considerations

- Multi-tenant support with organization-scoped ontologies.
- PostgreSQL-to-PostgreSQL connector for direct database ingestion.
- Webhook listener connector for real-time data push.
- Agent autonomy controls: configurable auto-approval for high-confidence proposals.
- Ontology versioning UI with migration tooling.
- Export capabilities (CSV, JSON, API) for getting data back out.
- Event streaming (e.g., emit events on entity create/update for downstream consumers).
- Cloud deployment configuration (AWS/GCP) with infrastructure-as-code.

---

## 11. Success Criteria

Bellona v1 is complete when a user can:

1. Upload a CSV file and have the system infer its structure.
2. Review and confirm an AI-proposed mapping of CSV columns to ontology properties.
3. Configure a REST API connector that pulls external data on a schedule.
4. Define entity types and relationships through the API or UI.
5. Query ingested data using structured filters, natural language, and a visual explorer.
6. See the relationships between entities visualized as a graph.

The demo path for showing this to a prospective employer should take under five minutes and demonstrate the full pipeline from data source to queryable knowledge graph.
