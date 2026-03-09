# Bellona — CLAUDE.md

## Project
Bellona is a universal data ontology platform. See SPEC.md for the full technical specification.

## Stack
- Python 3.14+
- FastAPI + Pydantic v2
- SQLAlchemy 2.0 (async)
- Alembic (migrations)
- PostgreSQL with JSONB
- Agno (agent framework, Phase 3)
- pytest + pytest-asyncio + pytest-cov

## Project Structure
Follows src/ layout. Installable package lives in `src/bellona/`. Tests mirror the package structure in `tests/`.

## Development Approach
- **TDD always.** Write tests before implementation. No exceptions without explicit instruction.
- **Small steps.** Implement one thing at a time, confirm it works, move on.
- **No commits.** Leave all git operations to the developer.

## Code Style
- Type annotations on all functions
- Async throughout — this is an async application
- Pydantic models for all API boundaries
- Explicit over implicit
