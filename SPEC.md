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

## Added 3/20/2026
## 6.2.5 Discovery Agent

The Discovery Agent automates the reconnaissance step of connecting a new REST API. Given only a base URL, it explores the API, discovers available resources, infers pagination strategies and authentication requirements, determines record extraction paths, and produces a complete connector configuration for each discovered resource -- all as a single reviewable proposal.

This agent is what makes Bellona's "just give it a URL" promise real. Without it, users must manually determine endpoint paths, pagination parameters, JSONPath expressions, and auth requirements before creating a connector -- exactly the kind of tedious, error-prone work the platform is designed to eliminate.

### Motivation

The SWAPI integration test exposed this gap directly. Creating a working connector required:

1. Manually discovering that `/api/people/` was a valid resource endpoint
2. Knowing that records live at `$.results` in the response (not `$.data`, not the root)
3. Knowing that pagination uses `?page=N` with a `next` field for continuation
4. Getting the connector config structure exactly right (`records_jsonpath` not `record_path`, nested `pagination.strategy` not flat `pagination_strategy`)

Each of these is a question the agent can answer by making HTTP requests and examining responses.

### Input

A single base URL (e.g., `https://swapi.dev/api/`). Optionally, auth credentials if the user knows the API requires authentication.

### Output

A `DiscoveryProposal` containing:

- A list of discovered API resources, each with:
  - Resource name (human-readable, inferred from the endpoint path)
  - Endpoint path (relative to base URL)
  - Pagination strategy and parameters
  - Record JSONPath (where the actual records live in the response)
  - Sample record (one representative record for the user to inspect)
  - Inferred record schema (field names, types, required/optional)
- Auth detection results (what kind of auth the API expects, if any)
- A proposed connector config for each resource, ready to be confirmed and created

The user reviews the full proposal, can accept/reject individual resources, and confirmed resources become connectors with field mappings automatically queued for the Mapper Agent.

### Agent Design

#### Identity

```
name: "discovery-agent"
description: "Explores REST APIs to discover available resources, pagination strategies, record formats, and authentication requirements."
instructions: See prompt specification below.
output_schema: DiscoveryProposalContent (Pydantic model)
```

#### Tools

The Discovery Agent uses four custom tools, all implemented as Python functions following the Agno tool pattern (typed parameters, docstrings, string-serializable return values).

**1. `http_get(url: str, headers: dict[str, str] | None = None) -> str`**

Makes a GET request to the given URL and returns the response as a JSON string, including the status code and response headers. This is the agent's primary exploration mechanism.

Returns a JSON object:
```json
{
  "status_code": 200,
  "headers": {"content-type": "application/json", ...},
  "body": { ... },
  "url": "https://swapi.dev/api/people/"
}
```

On non-2xx responses, returns the status code and body so the agent can reason about auth requirements (401/403) or missing endpoints (404).

Implementation note: Uses `httpx.AsyncClient` with a timeout (30s), follows redirects, and strips sensitive headers from the response. The agent cannot make POST/PUT/DELETE requests -- discovery is read-only.

**2. `extract_jsonpath(data: str, path: str) -> str`**

Applies a JSONPath expression to a JSON string and returns the matched results. Used by the agent to test candidate record extraction paths.

```python
# Agent calls: extract_jsonpath('{"results": [{"name": "Luke"}]}', '$.results')
# Returns: '[{"name": "Luke"}]'
```

Implementation note: Uses `jsonpath_ng` (already a dependency via the REST connector).

**3. `infer_schema(records_json: str) -> str`**

Takes a JSON array of records and returns a schema summary: field names, inferred types (string, integer, float, boolean, array, object, null), and whether each field appears in all records (required) or only some (optional).

```json
{
  "fields": [
    {"name": "name", "type": "string", "required": true},
    {"name": "height", "type": "string", "required": true},
    {"name": "mass", "type": "string", "required": false}
  ],
  "record_count": 10
}
```

Implementation note: Pure Python, no LLM call. Iterates over records and builds a type/presence map. This is deterministic -- the agent uses it as a data tool, not for reasoning.

**4. `detect_pagination(response_json: str, url: str) -> str`**

Analyzes a single API response and the URL that produced it. Looks for common pagination indicators:

- Response body keys: `next`, `previous`, `count`, `total`, `page`, `offset`, `cursor`, `has_more`, `next_cursor`
- Link headers: parses `Link` header for `rel="next"` / `rel="prev"`
- URL query params: detects `?page=`, `?offset=`, `?cursor=`, `?limit=`

Returns a JSON object with detected signals:
```json
{
  "detected_strategy": "offset",
  "confidence": "high",
  "signals": {
    "has_next_field": true,
    "has_count_field": true,
    "next_url": "https://swapi.dev/api/people/?page=2",
    "page_param": "page"
  }
}
```

Implementation note: Pure Python heuristic analysis, no LLM call. The agent uses these signals to make its final pagination determination.

#### Prompt Specification

The Discovery Agent receives a system prompt with these instructions:

```
You are a REST API discovery agent. Your job is to explore an API starting from a base URL and produce a complete description of its resources, suitable for configuring data connectors.

EXPLORATION STRATEGY:

1. Fetch the base URL. Examine the response:
   - If it returns a directory of endpoints (like {"people": "https://swapi.dev/api/people/", ...}), you've found a resource index. Explore each one.
   - If it returns a collection of records directly, treat the base URL itself as a single resource.
   - If it returns 401/403, note that auth is required and include that in your output.
   - If it returns 404 or an error, try common patterns: /api/, /v1/, /api/v1/

2. For each resource endpoint:
   a. Fetch the endpoint.
   b. Use detect_pagination to analyze the response for pagination signals.
   c. Determine where records live in the response:
      - Try common paths: $.results, $.data, $.items, $.records, $ (root array)
      - Use extract_jsonpath to test each candidate path.
      - The correct path is the one that returns an array of objects with consistent keys.
   d. Use extract_jsonpath with the correct path to get sample records.
   e. Use infer_schema on the sample records to get the field structure.

3. For auth detection:
   - A 200 on the base URL with data means no auth required (at least for read).
   - A 401 means auth is required. Check the WWW-Authenticate header for the scheme.
   - A 403 may mean auth is required or the resource is forbidden even with auth.
   - Common patterns: Bearer token, API key (in header or query param), Basic auth.

IMPORTANT CONSTRAINTS:

- You are read-only. Never attempt to write, modify, or delete data.
- Limit exploration to 20 HTTP requests total to avoid hammering the API.
- Fetch at most 1 page per resource -- you need a sample, not the full dataset.
- If an API has more than 10 resources, include all of them but note that the user may want to select a subset.
- Be conservative in your type inferences. If a field contains "unknown" or "n/a" as string values, keep it as string even if other values look numeric.
```

### Data Model

#### DiscoveryProposalContent (Pydantic -- output_schema for the agent)

```python
class DiscoveredResource(BaseModel):
    """A single API resource discovered by the agent."""
    resource_name: str                           # e.g., "people", "planets"
    endpoint_path: str                           # e.g., "/api/people/"
    records_jsonpath: str                        # e.g., "$.results"
    pagination: PaginationConfig                 # strategy + params
    sample_record: dict[str, Any]                # one representative record
    schema_summary: list[FieldSummary]           # inferred field names/types
    record_count_estimate: int | None = None     # total records if available from API

class FieldSummary(BaseModel):
    """Inferred field from sample records."""
    name: str
    inferred_type: str                           # string, integer, float, boolean, array, object
    required: bool                               # appears in all sampled records
    sample_values: list[str]                     # up to 3 example values (as strings)

class PaginationConfig(BaseModel):
    """Pagination configuration for a resource."""
    strategy: Literal["offset", "cursor", "link_header", "none"]
    page_param: str | None = None                # e.g., "page" for offset
    size_param: str | None = None                # e.g., "page_size", "limit"
    cursor_param: str | None = None              # for cursor-based
    next_field_jsonpath: str | None = None       # e.g., "$.next" for SWAPI

class AuthDetection(BaseModel):
    """What the agent detected about API authentication."""
    auth_required: bool
    detected_scheme: Literal["none", "bearer", "api_key", "basic", "unknown"] = "none"
    details: str | None = None                   # e.g., "WWW-Authenticate: Bearer"

class DiscoveryProposalContent(BaseModel):
    """Full output of the Discovery Agent."""
    base_url: str
    api_description: str                         # agent's summary of what this API provides
    auth: AuthDetection
    resources: list[DiscoveredResource]
    agent_notes: str | None = None               # any caveats or observations
```

#### AgentProposal integration

Discovery proposals use the existing `AgentProposal` model with `proposal_type = "discovery"`. The `proposal_content` JSONB column stores the serialized `DiscoveryProposalContent`.

Confirmation of a discovery proposal triggers creation of:
1. One `Connector` per confirmed resource (with the proposed config)
2. Automatic queueing of schema proposals for each connector (delegating to the Schema Agent)

The user can selectively confirm resources within a single proposal -- this is handled at the service layer by accepting a list of resource indices to confirm.

### Service Layer

Add to `services/agent_service.py`:

```python
async def discover_api(
    db: AsyncSession,
    base_url: str,
    auth_config: dict[str, Any] | None = None,
    *,
    _mock_result: DiscoveryProposalContent | None = None,
) -> AgentProposal:
    """
    Run the Discovery Agent against a base URL.
    Returns an AgentProposal with proposal_type="discovery".
    """
    if _mock_result is not None:
        content = _mock_result
    else:
        try:
            agent = DiscoveryAgent(api_key=_get_api_key(), model=_get_model())
            content = await agent.discover(base_url, auth_config)
        except Exception as exc:
            raise ProposalError(f"Discovery agent failed: {exc}") from exc

    proposal = AgentProposal(
        proposal_type="discovery",
        proposal_content=content.model_dump(),
        status="proposed",
        proposed_by="agent",
    )
    db.add(proposal)
    await db.commit()
    await db.refresh(proposal)
    return proposal


async def confirm_discovery_proposal(
    db: AsyncSession,
    proposal_id: uuid.UUID,
    selected_resources: list[int] | None = None,
) -> list[Connector]:
    """
    Confirm a discovery proposal. Creates connectors for selected resources
    (or all resources if selected_resources is None).
    Queues schema proposals for each created connector.

    Returns the list of created Connectors.
    """
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "discovery":
        raise ProposalError("Proposal is not a discovery proposal")
    if proposal.status != "proposed":
        raise ProposalError(f"Proposal status is '{proposal.status}', expected 'proposed'")

    content = DiscoveryProposalContent.model_validate(proposal.proposal_content)

    indices = selected_resources or list(range(len(content.resources)))
    connectors = []

    for idx in indices:
        if idx < 0 or idx >= len(content.resources):
            raise ProposalError(f"Resource index {idx} out of range")
        resource = content.resources[idx]

        connector = Connector(
            type="rest_api",
            name=f"{resource.resource_name} ({content.base_url})",
            config={
                "base_url": content.base_url,
                "endpoint": resource.endpoint_path,
                "auth": content.auth.model_dump() if content.auth.auth_required else {"type": "none"},
                "records_jsonpath": resource.records_jsonpath,
                "pagination": resource.pagination.model_dump(exclude_none=True),
            },
            status="active",
        )
        db.add(connector)
        connectors.append(connector)

    proposal.status = "confirmed"
    await db.commit()

    # Queue schema proposals for each new connector
    for connector in connectors:
        await db.refresh(connector)
        try:
            await propose_schema(db, connector.id)
        except ProposalError:
            pass  # Non-fatal: user can trigger schema proposal manually

    return connectors
```

### API Endpoints

Add to the agent router (`api/v1/agents.py`):

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/discovery/discover` | Start discovery for a base URL |
| POST | `/api/v1/discovery/{proposal_id}/confirm` | Confirm discovery, create connectors |

```python
@router.post("/discovery/discover")
async def discover_api(
    request: DiscoveryRequest,
    db: AsyncSession = Depends(get_db),
) -> AgentProposalRead:
    proposal = await agent_service.discover_api(
        db, request.base_url, request.auth_config
    )
    return AgentProposalRead.model_validate(proposal)


@router.post("/discovery/{proposal_id}/confirm")
async def confirm_discovery(
    proposal_id: uuid.UUID,
    request: ConfirmDiscoveryRequest | None = None,
    db: AsyncSession = Depends(get_db),
) -> list[ConnectorRead]:
    selected = request.selected_resources if request else None
    connectors = await agent_service.confirm_discovery_proposal(
        db, proposal_id, selected
    )
    return [ConnectorRead.model_validate(c) for c in connectors]
```

#### Request/Response Schemas

```python
class DiscoveryRequest(BaseModel):
    base_url: str                                   # e.g., "https://swapi.dev/api/"
    auth_config: dict[str, Any] | None = None       # optional pre-known auth

class ConfirmDiscoveryRequest(BaseModel):
    selected_resources: list[int] | None = None     # indices to confirm, None = all
```

### UI Integration

Add to the Connectors page:

1. A "Discover API" form at the top of the connector list page with a single input: base URL (and an expandable auth section).
2. The form POSTs to `/ui/connectors/discover`, which calls the service and redirects to the Proposals page.
3. On the Proposals page, discovery proposals render with a resource list showing name, endpoint, record count estimate, and sample record preview. Each resource has an individual confirm/skip toggle.
4. "Confirm Selected" creates connectors for toggled resources and automatically triggers schema proposals.

### Testing Strategy

#### Unit Tests (`tests/unit/test_discovery_agent.py`)

- `test_build_prompt` -- verify the prompt includes the base URL and optional auth context
- `test_parse_response` -- verify `DiscoveryProposalContent` parses correctly from agent output
- Mock `_run_agent` and verify the agent is constructed with the correct tools

#### Tool Tests (`tests/unit/test_discovery_tools.py`)

- `test_http_get_success` -- mock httpx, verify response structure
- `test_http_get_auth_required` -- mock 401 response, verify status code returned
- `test_http_get_timeout` -- mock timeout, verify graceful error
- `test_extract_jsonpath_valid` -- test `$.results` against SWAPI-shaped data
- `test_extract_jsonpath_root_array` -- test `$` against a root-level array
- `test_extract_jsonpath_no_match` -- test bad path returns empty
- `test_infer_schema_basic` -- mixed types, required vs optional
- `test_infer_schema_empty` -- empty array input
- `test_detect_pagination_offset` -- SWAPI-style next/previous/count
- `test_detect_pagination_cursor` -- cursor-based response shape
- `test_detect_pagination_link_header` -- Link header parsing
- `test_detect_pagination_none` -- single-page response

#### Integration Tests (`tests/integration/test_discovery_service.py`)

- `test_discover_api` -- mock agent result, verify proposal created with correct type and content
- `test_confirm_discovery_creates_connectors` -- verify connectors created with correct config
- `test_confirm_discovery_selected_resources` -- verify only selected indices create connectors
- `test_confirm_discovery_queues_schema_proposals` -- verify schema proposals triggered
- `test_confirm_discovery_wrong_type` -- verify error for non-discovery proposal
- `test_confirm_discovery_already_confirmed` -- verify status guard

#### Live Tests (`tests/live/test_live_discovery.py`)

- `test_discover_swapi` -- hit real SWAPI, verify agent finds people/planets/films/etc., correct pagination, correct record paths
- `test_discover_auth_required` -- hit an API that requires auth (e.g., httpbin.org/bearer), verify auth detection

### Development Roadmap Integration

This should be inserted as a **new phase** or as part of an MVP polish phase, since it depends on the existing agent infrastructure (Phase 3) and the UI (Phase 5) being complete. Suggested insertion:

**Phase 6: Discovery Agent**

- Discovery Agent with four tools (http_get, extract_jsonpath, infer_schema, detect_pagination)
- DiscoveryProposalContent schema and proposal flow
- Service layer: discover_api, confirm_discovery_proposal
- API endpoints: POST /discovery/discover, POST /discovery/{proposal_id}/confirm
- UI: "Discover API" form on Connectors page, resource selection on Proposals page
- Confirmation auto-creates connectors and queues schema proposals
- Unit, integration, and live tests

**Acceptance Criteria:**

Given `https://swapi.dev/api/` as input, the Discovery Agent should:
1. Find at least 5 resources (people, planets, films, species, vehicles, starships)
2. Correctly identify `$.results` as the record extraction path
3. Correctly identify offset pagination with `?page=N`
4. Correctly determine no auth is required
5. Produce a valid connector config for each resource that, when confirmed and synced, successfully ingests data

This is the "five-minute demo" path from the spec, but starting one step earlier -- the user doesn't even need to know what endpoints exist.

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
