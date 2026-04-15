"""Relationship Agent: turns raw relationship signals into formal proposals."""

from typing import Any

import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude

from bellona.schemas.agents import (
    PotentialRelationship,
    RelationshipProposalContent,
)

logger = structlog.get_logger()

_INSTRUCTIONS = """\
You are a data ontology expert. Given a set of relationship signals detected in a \
source entity's schema and some sample records, produce a formal list of relationship \
proposals.

For each signal, decide:
- The relationship name in snake_case (e.g. `reports_to`, `belongs_to`, `owns`).
- The cardinality, inferred from the data where possible. Allowed values: \
  `one-to-one`, `one-to-many`, `many-to-one`, `many-to-many`. If the same reference \
  value appears multiple times across sample records, the relationship is many-to-one \
  from source → target. Nested arrays (REST) imply one-to-many or many-to-many.
- A confidence score (0.0–1.0) and short reasoning.

Only propose a relationship if the target entity type already exists in the ontology, \
or the target is the same entity type as the source (self-reference). Drop any signal \
whose target is not available.

Respond ONLY with valid JSON matching the requested schema.
"""


def _build_prompt(
    source_entity_type_name: str,
    signals: list[PotentialRelationship],
    sample_records: list[dict[str, Any]],
    existing_entity_types: list[dict[str, Any]],
) -> str:
    signal_lines = [
        f"  - source_field={s.source_field}, target={s.target_entity_type_name}, basis={s.basis}"
        for s in signals
    ] or ["  (none)"]

    existing_lines = [
        f"  - {et['name']}: {et.get('description', '')}" for et in existing_entity_types
    ] or ["  (none)"]

    sample_lines = []
    for rec in sample_records[:20]:
        sample_lines.append("  - " + ", ".join(f"{k}={v}" for k, v in rec.items()))

    return (
        f"SOURCE ENTITY TYPE: {source_entity_type_name}\n\n"
        "POTENTIAL RELATIONSHIP SIGNALS:\n"
        + "\n".join(signal_lines)
        + "\n\nEXISTING ENTITY TYPES (valid targets):\n"
        + "\n".join(existing_lines)
        + "\n\nSAMPLE SOURCE RECORDS:\n"
        + ("\n".join(sample_lines) if sample_lines else "  (none)")
        + "\n\nProduce formal relationship proposals."
    )


class RelationshipAgent:
    """Wraps an Agno Agent to propose formal entity relationships."""

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self._api_key = api_key
        self._model = model

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id=self._model, api_key=self._api_key),
            description="Ontology expert that proposes formal entity relationships.",
            instructions=_INSTRUCTIONS,
            output_schema=RelationshipProposalContent,
        )

    async def _run_agent(self, prompt: str) -> RelationshipProposalContent:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, RelationshipProposalContent):
            return content
        if isinstance(content, str):
            return RelationshipProposalContent.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def propose(
        self,
        source_entity_type_name: str,
        signals: list[PotentialRelationship],
        sample_records: list[dict[str, Any]],
        existing_entity_types: list[dict[str, Any]],
    ) -> RelationshipProposalContent:
        prompt = _build_prompt(
            source_entity_type_name, signals, sample_records, existing_entity_types
        )
        logger.info(
            "relationship agent running",
            source_entity_type=source_entity_type_name,
            signal_count=len(signals),
            sample_count=len(sample_records),
        )
        result = await self._run_agent(prompt)
        logger.info(
            "relationship agent proposal ready",
            source_entity_type=source_entity_type_name,
            proposed_count=len(result.relationships),
            confidence=result.overall_confidence,
        )
        return result
