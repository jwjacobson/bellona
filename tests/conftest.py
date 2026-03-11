"""Root conftest: register custom pytest marks and configure collection."""
import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="Run live tests that call real LLM APIs (requires CLAUDE_API_KEY).",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:
    if not config.getoption("--live"):
        skip_live = pytest.mark.skip(reason="Pass --live to run live LLM tests")
        for item in items:
            if item.get_closest_marker("live"):
                item.add_marker(skip_live)
