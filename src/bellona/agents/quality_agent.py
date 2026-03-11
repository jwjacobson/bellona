"""Quality Agent: checks data quality for entities of a given type."""
import json
from typing import Any

import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude

from bellona.schemas.agents import QualityReport

logger = structlog.get_logger()

_INSTRUCTIONS = """\
You are a data quality analyst. Your job is to examine entity records for a given \
entity type and identify data quality issues.

Look for:
- missing_value: Required fields are null or absent, or optional fields are frequently missing.
- potential_duplicate: Two or more entities appear to represent the same real-world object.
- outlier: A field value is statistically anomalous or likely erroneous.
- type_mismatch: A field value doesn't match the expected data type.

Rules:
- Report each distinct issue once with the relevant entity IDs.
- Assign a severity: "low" (minor concern), "medium" (should be reviewed), "high" (likely error).
- Compute an overall_quality_score from 0.0 (terrible) to 1.0 (perfect).
- Write a concise summary.
- Respond ONLY with valid JSON matching the requested schema. No prose outside the JSON.
"""

# Maximum entities to include in the prompt to avoid exceeding context limits.
_MAX_ENTITIES_IN_PROMPT = 50


def _build_prompt(entity_type: dict[str, Any], entities: list[dict[str, Any]]) -> str:
    props_lines = []
    for p in entity_type.get("properties", []):
        req = " (required)" if p.get("required") else ""
        props_lines.append(f"  - {p['name']}: {p['data_type']}{req}")

    sample = entities[:_MAX_ENTITIES_IN_PROMPT]
    entities_json = json.dumps(sample, indent=2, default=str)

    total = len(entities)
    sample_note = f" (showing {len(sample)} of {total})" if total > _MAX_ENTITIES_IN_PROMPT else ""

    return (
        f"ENTITY TYPE: {entity_type['name']}\n"
        f"PROPERTIES:\n" + "\n".join(props_lines)
        + f"\n\nENTITY RECORDS ({total} total{sample_note}):\n"
        + entities_json
        + "\n\nCheck the entity records for data quality issues."
    )


class QualityAgent:
    """Wraps an Agno Agent to generate data quality reports."""

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id="claude-sonnet-4-6", api_key=self._api_key),
            description="Data quality analyst that identifies issues in entity records.",
            instructions=_INSTRUCTIONS,
            output_model=QualityReport,
        )

    async def _run_agent(self, prompt: str) -> QualityReport:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, QualityReport):
            return content
        if isinstance(content, str):
            return QualityReport.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def check(
        self,
        entity_type: dict[str, Any],
        entities: list[dict[str, Any]],
    ) -> QualityReport:
        """Run the Quality Agent and return a quality report."""
        prompt = _build_prompt(entity_type, entities)
        logger.info(
            "quality agent running",
            entity_type_name=entity_type.get("name"),
            entity_count=len(entities),
        )
        result = await self._run_agent(prompt)
        logger.info(
            "quality agent report ready",
            issue_count=len(result.issues),
            quality_score=result.overall_quality_score,
        )
        return result
