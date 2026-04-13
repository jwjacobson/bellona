"""Live integration tests for the Query Agent.

These tests call the real Claude API and also execute against the test database.

Run with:

    uv run pytest tests/live/test_query_agent_live.py --live
"""

import uuid

import pytest
import pytest_asyncio
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from bellona.agents.query_agent import QueryAgent
from bellona.core.config import get_settings
from bellona.models.base import Base
from bellona.models.entities import Entity
from bellona.schemas.agents import QueryAgentResult
from bellona.schemas.ontology import EntityTypeCreate, PropertyDefinitionCreate
from bellona.schemas.query import EntityQuery, FilterCondition, FilterGroup, SortClause
from bellona.services.entity_type import create_entity_type
from bellona.services.query import query_entities

pytestmark = [
    pytest.mark.asyncio(loop_scope="session"),
    pytest.mark.live,
]


def _api_key() -> str:
    return get_settings().claude_api_key


def _model() -> str:
    return get_settings().claude_model


# ── Seed data ─────────────────────────────────────────────────────────────────

_COMPANIES = [
    {
        "name": "AnchorTech",
        "founded_year": 2010,
        "employee_count": 320,
        "status": "active",
    },
    {
        "name": "NovaSpark",
        "founded_year": 2017,
        "employee_count": 85,
        "status": "active",
    },
    {
        "name": "BluePeak",
        "founded_year": 2020,
        "employee_count": 12,
        "status": "active",
    },
    {
        "name": "RocketWare",
        "founded_year": 2019,
        "employee_count": 140,
        "status": "active",
    },
    {
        "name": "OldVentures",
        "founded_year": 2003,
        "employee_count": 600,
        "status": "inactive",
    },
    {"name": "SwiftIO", "founded_year": 2022, "employee_count": 7, "status": "active"},
]

_COMPANY_ONTOLOGY = [
    {
        "name": "Company",
        "description": "A business entity.",
        "properties": [
            {
                "name": "name",
                "data_type": "string",
                "required": True,
                "description": "Company name",
            },
            {
                "name": "founded_year",
                "data_type": "integer",
                "required": False,
                "description": "Year the company was founded",
            },
            {
                "name": "employee_count",
                "data_type": "integer",
                "required": False,
                "description": "Number of employees",
            },
            {
                "name": "status",
                "data_type": "string",
                "required": False,
                "description": "active or inactive",
            },
        ],
    }
]


# ── Session-scoped DB fixture ─────────────────────────────────────────────────


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def seeded_company_session():
    """Open a DB connection, seed Company data, yield (session, entity_type_id),
    then roll back everything so the live test leaves no trace in the DB."""
    from bellona.db.session import engine

    async with engine.connect() as conn:
        await conn.begin()
        # Ensure tables exist (idempotent).
        await conn.run_sync(Base.metadata.create_all)

        session = AsyncSession(
            bind=conn,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        try:
            et = await create_entity_type(
                session,
                EntityTypeCreate(
                    name="Company",
                    properties=[
                        PropertyDefinitionCreate(
                            name="name", data_type="string", required=True
                        ),
                        PropertyDefinitionCreate(
                            name="founded_year", data_type="integer"
                        ),
                        PropertyDefinitionCreate(
                            name="employee_count", data_type="integer"
                        ),
                        PropertyDefinitionCreate(name="status", data_type="string"),
                    ],
                ),
            )
            for company in _COMPANIES:
                session.add(
                    Entity(
                        entity_type_id=et.id,
                        properties=company,
                        schema_version=et.schema_version,
                    )
                )
            await session.flush()

            yield session, et.id
        finally:
            await session.close()
            await conn.rollback()


# ── Live tests ────────────────────────────────────────────────────────────────


async def test_live_query_agent_returns_valid_result(
    seeded_company_session,
) -> None:
    session, entity_type_id = seeded_company_session

    agent = QueryAgent(api_key=_api_key(), model=_model())
    result = await agent.translate(
        "Which companies were founded after 2015 with more than 50 employees?",
        _COMPANY_ONTOLOGY,
    )

    assert isinstance(result, QueryAgentResult)
    assert result.entity_type_name is not None
    assert "company" in result.entity_type_name.lower()
    assert result.explanation
    assert 0.0 <= result.confidence <= 1.0


async def test_live_query_agent_confidence_above_threshold(
    seeded_company_session,
) -> None:
    session, entity_type_id = seeded_company_session

    agent = QueryAgent(api_key=_api_key(), model=_model())
    result = await agent.translate(
        "Which companies were founded after 2015 with more than 50 employees?",
        _COMPANY_ONTOLOGY,
    )

    assert result.confidence > 0.5


async def test_live_query_agent_references_valid_properties(
    seeded_company_session,
) -> None:
    session, entity_type_id = seeded_company_session

    agent = QueryAgent(api_key=_api_key(), model=_model())
    result = await agent.translate(
        "Which companies were founded after 2015 with more than 50 employees?",
        _COMPANY_ONTOLOGY,
    )

    valid_properties = {"name", "founded_year", "employee_count", "status"}

    # If any filters were generated, they should reference valid properties.
    if result.filters:

        def _collect_properties(node: dict) -> set[str]:
            if "property" in node:
                return {node["property"]}
            props: set[str] = set()
            for cond in node.get("conditions", []):
                props |= _collect_properties(cond)
            return props

        used_properties = _collect_properties(result.filters)
        assert used_properties.issubset(valid_properties), (
            f"Agent referenced unknown properties: {used_properties - valid_properties}"
        )


async def test_live_query_agent_executes_against_db(
    seeded_company_session,
) -> None:
    """Build an EntityQuery from the agent result and execute it against the DB.
    The returned entities should be a plausible subset of the seeded data.
    """
    session, entity_type_id = seeded_company_session

    agent = QueryAgent(api_key=_api_key(), model=_model())
    result = await agent.translate(
        "Which companies were founded after 2015 with more than 50 employees?",
        _COMPANY_ONTOLOGY,
    )

    # Build EntityQuery from agent result.
    filters = None
    if result.filters is not None:
        try:
            filters = FilterGroup.model_validate(result.filters)
        except ValidationError, Exception:
            try:
                filters = FilterCondition.model_validate(result.filters)
            except ValidationError, Exception:
                filters = None  # unparseable filter — run without it

    sort = [
        SortClause(property=s["property"], direction=s.get("direction", "asc"))
        for s in result.sort
        if "property" in s
    ]

    entity_query = EntityQuery(
        entity_type_id=entity_type_id,
        filters=filters,
        sort=sort,
        page=1,
        page_size=50,
    )

    page = await query_entities(session, entity_query)

    # Shape assertions — don't over-assert on exact results.
    assert page.total >= 0
    assert isinstance(page.items, list)
    assert page.total <= len(_COMPANIES)

    # All returned entities must belong to the Company entity type.
    for item in page.items:
        assert item.entity_type_id == entity_type_id

    # The question asks for companies founded after 2015 with > 50 employees.
    # Matching companies: NovaSpark (2017, 85), RocketWare (2019, 140).
    # If the agent got it right, at least one of these should appear.
    if page.total > 0 and filters is not None:
        returned_names = {item.properties["name"] for item in page.items}
        plausible = {"NovaSpark", "RocketWare"}
        assert returned_names & plausible, (
            f"Expected at least one of {plausible} in results, got {returned_names}"
        )
