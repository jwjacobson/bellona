"""Discovery Agent: explores REST APIs to discover resources, pagination, and auth."""
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import structlog
from agno.agent import Agent
from agno.models.anthropic import Claude
from jsonpath_ng import parse as jsonpath_parse

from bellona.schemas.agents import DiscoveryProposalContent

logger = structlog.get_logger()


# ── Tools (module-level async functions for Agno) ────────────────────────────


async def http_get(url: str, headers: dict[str, str] | None = None) -> str:
    """Make a GET request to a URL and return status, headers, and body as JSON."""
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            response = await client.get(url, headers=headers or {})
            try:
                body = response.json()
            except (json.JSONDecodeError, ValueError):
                body = response.text
            return json.dumps({
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": body,
                "url": str(response.url),
            })
    except httpx.TimeoutException:
        return json.dumps({
            "status_code": 0,
            "headers": {},
            "body": "Request timed out after 30 seconds",
            "url": url,
            "error": "timeout",
        })
    except httpx.RequestError as exc:
        return json.dumps({
            "status_code": 0,
            "headers": {},
            "body": f"Request failed: {exc}",
            "url": url,
            "error": "request_error",
        })


async def extract_jsonpath(data: str, path: str) -> str:
    """Apply a JSONPath expression to JSON data and return matched results."""
    try:
        parsed_data = json.loads(data) if isinstance(data, str) else data
        expr = jsonpath_parse(path)
        matches = expr.find(parsed_data)
        results = [m.value for m in matches]
        return json.dumps(results)
    except Exception as exc:
        return json.dumps({"error": str(exc)})


async def infer_schema(records_json: str) -> str:
    """Infer schema from a JSON array of records. Returns field names, types, and required status."""
    try:
        records = json.loads(records_json) if isinstance(records_json, str) else records_json
        if not isinstance(records, list) or len(records) == 0:
            return json.dumps({"fields": [], "record_count": 0})

        field_types: dict[str, set[str]] = {}
        field_counts: dict[str, int] = {}
        total = len(records)

        for record in records:
            if not isinstance(record, dict):
                continue
            for key, value in record.items():
                if key not in field_types:
                    field_types[key] = set()
                    field_counts[key] = 0
                field_counts[key] += 1
                field_types[key].add(_infer_type(value))

        fields = []
        for name in field_types:
            types = field_types[name] - {"null"}
            inferred = types.pop() if len(types) == 1 else "string"
            fields.append({
                "name": name,
                "type": inferred,
                "required": field_counts[name] == total,
            })

        return json.dumps({"fields": fields, "record_count": total})
    except Exception as exc:
        return json.dumps({"error": str(exc)})


def _infer_type(value: Any) -> str:
    """Infer the JSON type of a Python value."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "string"


async def detect_pagination(response_json: str, url: str) -> str:
    """Analyze an API response for pagination signals."""
    try:
        data = json.loads(response_json) if isinstance(response_json, str) else response_json
        body = data.get("body", data) if isinstance(data, dict) else data
        headers = data.get("headers", {}) if isinstance(data, dict) else {}

        signals: dict[str, Any] = {}

        # Check body keys for pagination indicators
        if isinstance(body, dict):
            for key in ("next", "next_url"):
                if key in body and body[key] is not None:
                    signals["has_next_field"] = True
                    signals["next_url"] = body[key]
            for key in ("previous", "prev", "previous_url"):
                if key in body:
                    signals["has_previous_field"] = True
            for key in ("count", "total", "total_count", "total_results"):
                if key in body:
                    signals["has_count_field"] = True
                    signals["total"] = body[key]
            for key in ("next_cursor", "cursor", "next_page_token"):
                if key in body and body[key] is not None:
                    signals["has_cursor"] = True
                    signals["cursor_value"] = body[key]
            if "has_more" in body:
                signals["has_more"] = body["has_more"]

        # Check Link header
        link_header = headers.get("link", headers.get("Link", ""))
        if 'rel="next"' in link_header or "rel=next" in link_header:
            signals["has_link_header"] = True

        # Check URL params
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for param in ("page", "offset", "cursor", "limit", "page_size"):
            if param in params:
                signals[f"{param}_param_in_url"] = True

        # Determine strategy
        if signals.get("has_link_header"):
            strategy = "link_header"
            confidence = "high"
        elif signals.get("has_cursor"):
            strategy = "cursor"
            confidence = "high"
        elif signals.get("has_next_field") or signals.get("has_count_field"):
            strategy = "offset"
            confidence = "high"
            # Try to detect page param from next_url
            next_url = signals.get("next_url", "")
            if isinstance(next_url, str) and next_url:
                next_parsed = urlparse(next_url)
                next_params = parse_qs(next_parsed.query)
                if "page" in next_params:
                    signals["page_param"] = "page"
                elif "offset" in next_params:
                    signals["page_param"] = "offset"
        else:
            strategy = "none"
            confidence = "medium"

        return json.dumps({
            "detected_strategy": strategy,
            "confidence": confidence,
            "signals": signals,
        })
    except Exception as exc:
        return json.dumps({"error": str(exc)})


# ── Agent Class ──────────────────────────────────────────────────────────────


_INSTRUCTIONS = """\
You are a REST API discovery agent. Your job is to explore an API starting from \
a base URL and produce a complete description of its resources, suitable for \
configuring data connectors.

EXPLORATION STRATEGY:

1. Fetch the base URL. Examine the response:
   - If it returns a directory of endpoints (like {"people": "url", ...}), \
you've found a resource index. Explore each one.
   - If it returns a collection of records directly, treat the base URL itself \
as a single resource.
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

IMPORTANT CONSTRAINTS:

- You are read-only. Never attempt to write, modify, or delete data.
- Limit exploration to 20 HTTP requests total to avoid hammering the API.
- Fetch at most 1 page per resource -- you need a sample, not the full dataset.
- If an API has more than 10 resources, include all of them but note that the \
user may want to select a subset.
- Be conservative in your type inferences.

Respond ONLY with valid JSON matching the requested schema. No prose outside the JSON.
"""


def _build_discovery_prompt(
    base_url: str, auth_config: dict[str, Any] | None = None
) -> str:
    parts = [f"Discover the REST API at: {base_url}"]
    if auth_config:
        parts.append(f"\nAuth configuration provided: {json.dumps(auth_config)}")
        parts.append("Use these credentials when making requests.")
    else:
        parts.append("\nNo auth credentials provided. Detect if auth is required.")
    parts.append("\nExplore the API and produce a complete discovery proposal.")
    return "\n".join(parts)


class DiscoveryAgent:
    """Wraps an Agno Agent to discover REST API resources."""

    def __init__(self, api_key: str | None = None, model: str | None = None) -> None:
        self._api_key = api_key
        self._model = model

    def _make_agent(self) -> Agent:
        return Agent(
            model=Claude(id=self._model, api_key=self._api_key),
            description="REST API discovery agent that explores APIs and proposes connector configurations.",
            instructions=_INSTRUCTIONS,
            tools=[http_get, extract_jsonpath, infer_schema, detect_pagination],
            output_schema=DiscoveryProposalContent,
        )

    async def _run_agent(self, prompt: str) -> DiscoveryProposalContent:
        agent = self._make_agent()
        response = await agent.arun(prompt)
        content = response.content
        if isinstance(content, DiscoveryProposalContent):
            return content
        if isinstance(content, str):
            return DiscoveryProposalContent.model_validate_json(content)
        raise ValueError(f"Unexpected agent response type: {type(content)}")

    async def discover(
        self,
        base_url: str,
        auth_config: dict[str, Any] | None = None,
    ) -> DiscoveryProposalContent:
        """Run the Discovery Agent and return a structured proposal."""
        prompt = _build_discovery_prompt(base_url, auth_config)
        logger.info("discovery agent running", base_url=base_url)
        result = await self._run_agent(prompt)
        logger.info(
            "discovery agent proposal ready",
            base_url=base_url,
            resource_count=len(result.resources),
        )
        return result
