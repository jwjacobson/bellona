"""Schema Agent: proposes new entity types when source data doesn't fit existing ontology."""

from typing import Any

import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude

from bellona.connectors.base import SchemaDiscovery
from bellona.schemas.agents import EntityTypeProposalContent

logger = structlog.get_logger()

_INSTRUCTIONS = """\
You are a data ontology expert. Your job is to analyze a source data schema and propose \
a new entity type definition that best represents the data.

Rules:
- Propose a single entity type with a clear, descriptive name (PascalCase).
- Define properties that match the source fields, using appropriate data types \
  (string, integer, float, boolean, date, datetime, enum, json).
- Mark properties as required if they appear non-nullable and semantically essential.
- Consider existing entity types and avoid duplicating them; propose something distinct.
- Provide reasoning and a confidence score (0.0–1.0).
- Detect POTENTIAL RELATIONSHIPS: if any source fields look like foreign-key \
  references to another entity (e.g. `manager_id`, `department_id`, fields containing \
  URLs that point at another resource, nested object/array fields), populate \
  `potential_relationships` with one entry per signal. For each signal provide the \
  `source_field`, the suspected `target_entity_type_name` (it may be the entity type \
  you are currently proposing, for self-references), and a short `basis` explaining \
  the inference (e.g. "naming convention: '_id' suffix"). If no references are \
  apparent, leave `potential_relationships` empty.
- Respond ONLY with valid JSON matching the requested schema. No prose outside the JSON.
"""


def _build_prompt(
    schema: SchemaDiscovery,
    existing_entity_types: list[dict[str, Any]],
) -> str:
    schema_lines = []
    for f in schema.fields:
        samples = (
            ", ".join(str(v) for v in f.sample_values[:3])
            if f.sample_values
            else "none"
        )
        schema_lines.append(
            f"  - {f.name} (type: {f.inferred_type}, nullable: {f.nullable}, samples: [{samples}])"
        )

    existing_lines = []
    for et in existing_entity_types:
        existing_lines.append(f"  - {et['name']}: {et.get('description', '')}")

    record_hint = (
        f"Estimated record count: {schema.record_count_estimate}"
        if schema.record_count_estimate
        else ""
    )

    return (
        "SOURCE SCHEMA:\n"
        + "\n".join(schema_lines)
        + (f"\n{record_hint}" if record_hint else "")
        + "\n\nEXISTING ENTITY TYPES (do not duplicate these):\n"
        + ("\n".join(existing_lines) if existing_lines else "  (none)")
        + "\n\nPropose a new entity type that best represents this source data."
    )


class SchemaAgent:
    """Wraps an Agno Agent to propose new entity type definitions."""

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self._api_key = api_key
        self._model = model

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id=self._model, api_key=self._api_key),
            description="Ontology expert that proposes new entity type definitions.",
            instructions=_INSTRUCTIONS,
            output_schema=EntityTypeProposalContent,
        )

    async def _run_agent(self, prompt: str) -> EntityTypeProposalContent:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, EntityTypeProposalContent):
            return content
        if isinstance(content, str):
            return EntityTypeProposalContent.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def propose(
        self,
        schema: SchemaDiscovery,
        existing_entity_types: list[dict[str, Any]],
    ) -> EntityTypeProposalContent:
        """Run the Schema Agent and return a structured entity type proposal."""
        prompt = _build_prompt(schema, existing_entity_types)
        logger.info(
            "schema agent running",
            source_fields=len(schema.fields),
            existing_type_count=len(existing_entity_types),
        )
        result = await self._run_agent(prompt)
        logger.info(
            "schema agent proposal ready",
            entity_type_name=result.entity_type_name,
            property_count=len(result.properties),
            confidence=result.confidence,
        )
        return result
