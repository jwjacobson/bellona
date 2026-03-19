"""Query Agent: translates natural language questions into structured EntityQuery objects."""
from typing import Any

import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude

from bellona.schemas.agents import QueryAgentResult

logger = structlog.get_logger()

_INSTRUCTIONS = """\
You are a data query expert. Your job is to translate a natural language question \
into a structured query against an ontology database.

Rules:
- Choose the most relevant entity type from the available ontology.
- Build filters using EXACTLY this schema:
  - Single condition: {"property": "<name>", "operator": "<op>", "value": "<val>"}
  - Compound group: {"op": "and"|"or", "conditions": [<filters>]}
  - Conditions inside a group can be single conditions or nested groups.
  - Valid operators: eq, neq, gt, gte, lt, lte, contains, in, is_null, not_null.
  - For is_null and not_null operators, omit the "value" field.
- Only filter on properties that exist in the chosen entity type.
- Property names must match EXACTLY — use the names from the ontology, not synonyms.
- If the question cannot be answered with the available ontology, set entity_type_name \
  and filters to null and explain why.
- Provide a clear explanation of how you interpreted the question.
- Respond ONLY with valid JSON matching the requested schema. No prose outside the JSON.
"""


def _build_prompt(question: str, ontology: list[dict[str, Any]]) -> str:
    entity_lines = []
    for et in ontology:
        props = ", ".join(
            f"{p['name']} ({p['data_type']}{'*' if p.get('required') else ''})"
            for p in et.get("properties", [])
        )
        entity_lines.append(f"  - {et['name']}: {props}")
        if et.get("description"):
            entity_lines.append(f"    description: {et['description']}")

    return (
        f"QUESTION: {question}\n\n"
        "AVAILABLE ENTITY TYPES (* = required property):\n"
        + ("\n".join(entity_lines) if entity_lines else "  (none)")
        + "\n\nTranslate the question into a structured query."
    )


class QueryAgent:
    """Wraps an Agno Agent to translate natural language into structured queries."""

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self._api_key = api_key
        self._model = model

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id=self._model, api_key=self._api_key),
            description="Data query expert that translates natural language to structured queries.",
            instructions=_INSTRUCTIONS,
            output_schema=QueryAgentResult,
        )

    async def _run_agent(self, prompt: str) -> QueryAgentResult:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, QueryAgentResult):
            return content
        if isinstance(content, str):
            return QueryAgentResult.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def translate(
        self,
        question: str,
        ontology: list[dict[str, Any]],
    ) -> QueryAgentResult:
        """Run the Query Agent and return a structured query result."""
        prompt = _build_prompt(question, ontology)
        logger.info(
            "query agent running",
            entity_type_count=len(ontology),
        )
        result = await self._run_agent(prompt)
        logger.info(
            "query agent result ready",
            entity_type_name=result.entity_type_name,
            confidence=result.confidence,
        )
        return result
