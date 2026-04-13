"""Mapper Agent: proposes field-to-property mappings for incoming data schemas."""

import json
from typing import Any

import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude

from bellona.connectors.base import SchemaDiscovery
from bellona.schemas.agents import MappingProposalContent

logger = structlog.get_logger()

_INSTRUCTIONS = """\
You are a data mapping expert. Your job is to analyze a source data schema and \
propose how each source field maps to properties in an existing ontology entity type.

Rules:
- Map each source field to the best-matching ontology property based on name similarity, \
  data type compatibility, and sample values.
- Use bare property names in target_property (e.g. "name", not "EntityType.name").
- Assign a confidence score (0.0–1.0) to each mapping.
- If a source field has no reasonable match, omit it from the mappings list.
- Provide brief reasoning for each mapping.
- Respond ONLY with valid JSON matching the requested schema. No prose outside the JSON.
"""


def _build_prompt(schema: SchemaDiscovery, entity_types: list[dict[str, Any]]) -> str:
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

    entity_lines = []
    for et in entity_types:
        props = ", ".join(
            f"{p['name']} ({p['data_type']}{'*' if p.get('required') else ''})"
            for p in et.get("properties", [])
        )
        entity_lines.append(f"  - {et['name']}: {props}")
        if et.get("description"):
            entity_lines.append(f"    description: {et['description']}")

    record_hint = (
        f"Estimated record count: {schema.record_count_estimate}"
        if schema.record_count_estimate
        else ""
    )

    return (
        f"SOURCE SCHEMA:\n"
        + "\n".join(schema_lines)
        + (f"\n{record_hint}" if record_hint else "")
        + "\n\nAVAILABLE ENTITY TYPES (* = required property):\n"
        + ("\n".join(entity_lines) if entity_lines else "  (none)")
        + "\n\nPropose field mappings from the source schema to these entity type properties."
    )


class MapperAgent:
    """Wraps an Agno Agent to propose field-to-property mappings."""

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self._api_key = api_key
        self._model = model

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id=self._model, api_key=self._api_key),
            description="Data mapping expert that proposes field-to-property mappings.",
            instructions=_INSTRUCTIONS,
            output_schema=MappingProposalContent,
        )

    async def _run_agent(self, prompt: str) -> MappingProposalContent:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, MappingProposalContent):
            return content
        # Fallback: try to parse JSON string response
        if isinstance(content, str):
            return MappingProposalContent.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def propose(
        self,
        schema: SchemaDiscovery,
        entity_types: list[dict[str, Any]],
    ) -> MappingProposalContent:
        """Run the Mapper Agent and return a structured mapping proposal."""
        prompt = _build_prompt(schema, entity_types)
        logger.info(
            "mapper agent running",
            source_fields=len(schema.fields),
            entity_type_count=len(entity_types),
        )
        result = await self._run_agent(prompt)
        logger.info(
            "mapper agent proposal ready",
            mapping_count=len(result.mappings),
            overall_confidence=result.overall_confidence,
        )
        return result
