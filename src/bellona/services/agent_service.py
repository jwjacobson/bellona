"""Agent service layer: orchestrates agent calls and persists proposals."""
import uuid
from typing import Any
from urllib.parse import urlparse

import structlog
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from bellona.agents.discovery_agent import DiscoveryAgent
from bellona.agents.mapper_agent import MapperAgent
from bellona.agents.quality_agent import QualityAgent
from bellona.agents.query_agent import QueryAgent
from bellona.agents.schema_agent import SchemaAgent
from bellona.core.config import get_settings
from bellona.models.entities import Entity
from bellona.models.ontology import EntityType
from bellona.models.system import AgentProposal, Connector, FieldMapping
from bellona.schemas.agents import (
    DiscoveryProposalContent,
    EntityTypeProposalContent,
    MappingProposalContent,
    NaturalLanguageQueryResponse,
    ProposedPropertyDefinition,
    QualityReport,
    QueryAgentResult,
)
from bellona.schemas.query import EntityQuery, FilterCondition, FilterGroup, SortClause
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.services.entity_type import create_entity_type
from bellona.services.ingestion import _create_connector_instance, _load_entity_type

logger = structlog.get_logger()


class ProposalError(Exception):
    """Raised when a proposal operation cannot be completed."""


def _get_api_key() -> str:
    return get_settings().claude_api_key

def _get_model() -> str:
    return get_settings().claude_model

async def _get_connector_or_raise(db: AsyncSession, connector_id: uuid.UUID) -> Connector:
    connector = await db.get(Connector, connector_id)
    if connector is None:
        raise ProposalError(f"Connector {connector_id} not found")
    return connector


async def _get_entity_type_or_raise(
    db: AsyncSession, entity_type_id: uuid.UUID
) -> EntityType:
    et = await _load_entity_type(db, entity_type_id)
    if et is None:
        raise ProposalError(f"Entity type {entity_type_id} not found")
    return et


async def _get_proposal_or_raise(
    db: AsyncSession, proposal_id: uuid.UUID
) -> AgentProposal:
    proposal = await db.get(AgentProposal, proposal_id)
    if proposal is None:
        raise ProposalError(f"Proposal {proposal_id} not found")
    return proposal


def _entity_types_to_context(entity_types: list[EntityType]) -> list[dict[str, Any]]:
    return [
        {
            "name": et.name,
            "description": et.description or "",
            "properties": [
                {
                    "name": p.name,
                    "data_type": p.data_type,
                    "required": p.required,
                    "description": p.description or "",
                }
                for p in et.property_definitions
            ],
        }
        for et in entity_types
    ]


async def propose_mapping(
    db: AsyncSession,
    connector_id: uuid.UUID,
    entity_type_id: uuid.UUID,
    *,
    _mock_result: MappingProposalContent | None = None,
) -> AgentProposal:
    """Run the Mapper Agent and persist the proposal as an AgentProposal."""
    connector = await _get_connector_or_raise(db, connector_id)
    entity_type = await _get_entity_type_or_raise(db, entity_type_id)

    connector_instance = _create_connector_instance(connector)
    schema = await connector_instance.discover_schema()

    et_context = _entity_types_to_context([entity_type])

    if _mock_result is not None:
        proposal_content = _mock_result
    else:
        try:
            agent = MapperAgent(api_key=_get_api_key(), model=_get_model())
            proposal_content = await agent.propose(schema, et_context)
        except Exception as exc:
            raise ProposalError(f"Mapper agent failed: {exc}") from exc

    proposal = AgentProposal(
        proposal_type="mapping",
        status="proposed",
        content=proposal_content.model_dump(),
        confidence=proposal_content.overall_confidence,
        connector_id=connector_id,
        entity_type_id=entity_type_id,
    )
    db.add(proposal)
    await db.flush()
    logger.info(
        "mapping proposal created",
        proposal_id=str(proposal.id),
        connector_id=str(connector_id),
        entity_type_id=str(entity_type_id),
        confidence=proposal_content.overall_confidence,
    )
    return proposal


async def propose_schema(
    db: AsyncSession,
    connector_id: uuid.UUID,
    *,
    _mock_result: EntityTypeProposalContent | None = None,
) -> AgentProposal:
    """Run the Schema Agent and persist the proposal as an AgentProposal."""
    connector = await _get_connector_or_raise(db, connector_id)

    connector_instance = _create_connector_instance(connector)
    schema = await connector_instance.discover_schema()

    # Provide all existing entity types as context so the agent avoids duplicates.
    all_et_result = await db.execute(
        select(EntityType).options(selectinload(EntityType.property_definitions))
    )
    all_entity_types = list(all_et_result.scalars().all())
    et_context = _entity_types_to_context(all_entity_types)

    if _mock_result is not None:
        proposal_content = _mock_result
    else:
        try:
            agent = SchemaAgent(api_key=_get_api_key(), model=_get_model())
            proposal_content = await agent.propose(schema, et_context)
        except Exception as exc:
            raise ProposalError(f"Schema agent failed: {exc}") from exc

    proposal = AgentProposal(
        proposal_type="entity_type",
        status="proposed",
        content=proposal_content.model_dump(),
        confidence=proposal_content.confidence,
        connector_id=connector_id,
    )
    db.add(proposal)
    await db.flush()
    logger.info(
        "schema proposal created",
        proposal_id=str(proposal.id),
        connector_id=str(connector_id),
        entity_type_name=proposal_content.entity_type_name,
        confidence=proposal_content.confidence,
    )
    return proposal


async def confirm_mapping_proposal(
    db: AsyncSession, proposal_id: uuid.UUID
) -> FieldMapping:
    """Convert a confirmed mapping AgentProposal into a confirmed FieldMapping."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "mapping":
        raise ProposalError(
            f"Proposal {proposal_id} is type '{proposal.proposal_type}', not 'mapping'"
        )
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )
    entity_type = await _get_entity_type_or_raise(db, proposal.entity_type_id)
    # Extract mapping entries (strip agent-only fields like reasoning/confidence).
    raw_mappings = proposal.content.get("mappings", [])
    clean_mappings = [
    {
        "source_field": m["source_field"],
        "target_property": m["target_property"].removeprefix(f"{entity_type.name}."),
    }
    for m in raw_mappings
    ]

    field_mapping = FieldMapping(
        connector_id=proposal.connector_id,
        entity_type_id=proposal.entity_type_id,
        mapping_config={"mappings": clean_mappings},
        status="confirmed",
        proposed_by="agent",
    )
    db.add(field_mapping)

    proposal.status = "confirmed"
    await db.flush()

    logger.info(
        "mapping proposal confirmed",
        proposal_id=str(proposal_id),
        field_mapping_id=str(field_mapping.id),
        field_count=len(clean_mappings),
    )
    return field_mapping


async def confirm_schema_proposal(
    db: AsyncSession, proposal_id: uuid.UUID
) -> EntityType:
    """Execute a confirmed schema AgentProposal by creating the entity type."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "entity_type":
        raise ProposalError(
            f"Proposal {proposal_id} is type '{proposal.proposal_type}', not 'entity_type'"
        )
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )

    content = EntityTypeProposalContent.model_validate(proposal.content)
    entity_type = await create_entity_type(
        db,
        EntityTypeCreate(
            name=content.entity_type_name,
            description=content.description or None,
            properties=[
                PropertyDefinitionCreate(
                    name=p.name,
                    data_type=p.data_type,
                    required=p.required,
                    description=p.description or None,
                )
                for p in content.properties
            ],
        ),
    )

    proposal.entity_type_id = entity_type.id
    proposal.status = "confirmed"
    await db.flush()

    logger.info(
        "schema proposal confirmed",
        proposal_id=str(proposal_id),
        entity_type_id=str(entity_type.id),
        entity_type_name=entity_type.name,
    )
    return entity_type


async def reject_proposal(db: AsyncSession, proposal_id: uuid.UUID) -> AgentProposal:
    """Mark any proposal as rejected."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )
    proposal.status = "rejected"
    await db.flush()
    logger.info("proposal rejected", proposal_id=str(proposal_id))
    return proposal


async def list_proposals(db: AsyncSession) -> list[AgentProposal]:
    """Return all proposals currently in 'proposed' status."""
    result = await db.execute(
        select(AgentProposal)
        .where(AgentProposal.status == "proposed")
        .order_by(AgentProposal.created_at.desc())
    )
    return list(result.scalars().all())


async def check_quality(
    db: AsyncSession,
    entity_type_id: uuid.UUID,
    *,
    _mock_result: QualityReport | None = None,
) -> QualityReport:
    """Run the Quality Agent against entities of the given type. Returns report (not persisted)."""
    entity_type = await _get_entity_type_or_raise(db, entity_type_id)

    entities_result = await db.execute(
        select(Entity).where(Entity.entity_type_id == entity_type_id).limit(100)
    )
    entities = list(entities_result.scalars().all())

    entity_type_context: dict[str, Any] = {
        "name": entity_type.name,
        "properties": [
            {
                "name": p.name,
                "data_type": p.data_type,
                "required": p.required,
            }
            for p in entity_type.property_definitions
        ],
    }
    entities_context = [
        {"id": str(e.id), "properties": e.properties}
        for e in entities
    ]

    if _mock_result is not None:
        return _mock_result

    try:
        agent = QualityAgent(api_key=_get_api_key(), model=_get_model())
        return await agent.check(entity_type_context, entities_context)
    except Exception as exc:
        raise ProposalError(f"Quality agent failed: {exc}") from exc


def _normalize_filters(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize agent filter output to match our Pydantic schemas.

    Handles common agent mistakes like using 'field' instead of 'property',
    'and'/'or' as keys instead of 'op' with 'conditions', etc.
    """
    # Handle {"and": [...]} or {"or": [...]} → {"op": "and", "conditions": [...]}
    for logical_op in ("and", "or"):
        if logical_op in raw and isinstance(raw[logical_op], list):
            return {
                "op": logical_op,
                "conditions": [_normalize_filters(c) for c in raw[logical_op]],
            }

    # Handle {"field": x, ...} → {"property": x, ...}
    if "field" in raw and "property" not in raw:
        raw["property"] = raw.pop("field")

    # Handle {"neq": val} or {"eq": val} style → {"operator": "neq", "value": val}
    if "operator" not in raw:
        for op in ("eq", "neq", "gt", "gte", "lt", "lte", "contains", "in", "is_null", "not_null"):
            if op in raw:
                raw = {**raw, "operator": op, "value": raw.pop(op)}
                break

    # Recurse into conditions if present
    if "conditions" in raw and isinstance(raw["conditions"], list):
        raw = {**raw, "conditions": [_normalize_filters(c) for c in raw["conditions"]]}

    return raw

async def run_nl_query(
    db: AsyncSession,
    question: str,
    entity_type_id: uuid.UUID | None = None,
    *,
    _mock_result: QueryAgentResult | None = None,
) -> NaturalLanguageQueryResponse:
    """Translate a natural language question to a structured query and execute it."""
    from bellona.services.query import query_entities

    # Build ontology context
    if entity_type_id is not None:
        et = await _get_entity_type_or_raise(db, entity_type_id)
        entity_types = [et]
    else:
        all_et_result = await db.execute(
            select(EntityType).options(selectinload(EntityType.property_definitions))
        )
        entity_types = list(all_et_result.scalars().all())

    et_context = _entity_types_to_context(entity_types)

    if _mock_result is not None:
        agent_result = _mock_result
    else:
        try:
            agent = QueryAgent(api_key=_get_api_key(), model=_get_model())
            agent_result = await agent.translate(question, et_context)
        except Exception as exc:
            raise ProposalError(f"Query agent failed: {exc}") from exc

    # Resolve entity_type_name → entity_type_id
    resolved_et_id: uuid.UUID | None = entity_type_id
    if resolved_et_id is None and agent_result.entity_type_name is not None:
        matched = next(
           (et for et in entity_types if et.name.lower() == agent_result.entity_type_name.lower()),
            None,
        )
        if matched is not None:
            resolved_et_id = matched.id
        else:
            raise ProposalError(
                f"Agent selected entity type '{agent_result.entity_type_name}' "
                f"which does not exist in the ontology"
            )

    # Build filters from agent result, with normalization for common agent mistakes
    filters = None
    if agent_result.filters is not None:
        raw = _normalize_filters(agent_result.filters)
        try:
            filters = FilterGroup.model_validate(raw)
        except ValidationError:
            filters = FilterCondition.model_validate(raw)

    sort = [
        SortClause(
            property=s["property"],
            direction=s.get("direction", "asc"),
        )
        for s in agent_result.sort
    ]

    entity_query = EntityQuery(
        entity_type_id=resolved_et_id,
        filters=filters,
        sort=sort,
        page=1,
        page_size=50,
    )

    try:
        page = await query_entities(db, entity_query)
    except ValueError as exc:
        raise ProposalError(f"Query execution failed: {exc}") from exc

    logger.info(
        "nl query executed",
        question=question,
        entity_type_name=agent_result.entity_type_name,
        total=page.total,
        confidence=agent_result.confidence,
    )
    return NaturalLanguageQueryResponse(
        question=question,
        explanation=agent_result.explanation,
        query_used=entity_query.model_dump(mode="json"),
        results=[item.model_dump(mode="json") for item in page.items],
        total_results=page.total,
    )


async def discover_api(
    db: AsyncSession,
    base_url: str,
    auth_config: dict[str, Any] | None = None,
    *,
    _mock_result: DiscoveryProposalContent | None = None,
) -> AgentProposal:
    """Run the Discovery Agent against a base URL and persist the proposal."""
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
        status="proposed",
        content=content.model_dump(),
        confidence=None,
    )
    db.add(proposal)
    await db.flush()
    logger.info(
        "discovery proposal created",
        proposal_id=str(proposal.id),
        base_url=base_url,
        resource_count=len(content.resources),
    )
    return proposal


async def confirm_discovery_proposal(
    db: AsyncSession,
    proposal_id: uuid.UUID,
    selected_resources: list[int] | None = None,
) -> list[Connector]:
    """Confirm a discovery proposal. Creates connectors for selected resources."""
    proposal = await _get_proposal_or_raise(db, proposal_id)
    if proposal.proposal_type != "discovery":
        raise ProposalError(
            f"Proposal {proposal_id} is type '{proposal.proposal_type}', not 'discovery'"
        )
    if proposal.status != "proposed":
        raise ProposalError(
            f"Proposal {proposal_id} is already '{proposal.status}'"
        )

    content = DiscoveryProposalContent.model_validate(proposal.content)
    indices = selected_resources if selected_resources is not None else list(range(len(content.resources)))
    connectors: list[Connector] = []

    for idx in indices:
        if idx < 0 or idx >= len(content.resources):
            raise ProposalError(f"Resource index {idx} out of range")
        resource = content.resources[idx]

        base_parsed = urlparse(content.base_url)

        endpoint = resource.endpoint_path
        if endpoint.startswith(("http://", "https://")):
            # Agent returned a full URL — extract just the path relative to base_url
            endpoint = endpoint.removeprefix(content.base_url.rstrip("/"))
            if not endpoint.startswith("/"):
                endpoint = "/" + endpoint
        elif endpoint.startswith(base_parsed.path) and base_parsed.path != "/":
            endpoint = endpoint[len(base_parsed.path):]
            if not endpoint.startswith("/"):
                endpoint = "/" + endpoint

        pagination = resource.pagination.model_dump(exclude_none=True)
        if pagination.get("strategy") == "offset" and "page_size" not in pagination:
            pagination["page_size"] = 10

        connector = Connector(
            type="rest_api",
            name=f"{resource.resource_name} ({content.base_url})",
            config={
                "base_url": content.base_url,
                "endpoint": endpoint,
                "auth": content.auth.model_dump() if content.auth.auth_required else {"type": "none"},
                "records_jsonpath": resource.records_jsonpath,
                "pagination": pagination,
            },
            status="active",
        )
       
        db.add(connector)
        connectors.append(connector)
    proposal.status = "confirmed"
    await db.flush()

    # Queue schema proposals for each new connector (non-fatal)
    for connector in connectors:
        try:
            await propose_schema(db, connector.id)
        except ProposalError:
            logger.warning(
                "schema proposal auto-queue failed",
                connector_id=str(connector.id),
            )

    logger.info(
        "discovery proposal confirmed",
        proposal_id=str(proposal_id),
        connectors_created=len(connectors),
    )
    return connectors
