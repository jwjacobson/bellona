"""Unit tests for EntityType.resolve_display_property."""

from bellona.models.ontology import EntityType, PropertyDefinition


def _et(display: str | None, *props: tuple[str, str]) -> EntityType:
    et = EntityType(name="X", description=None, display_property=display)
    et.property_definitions = [
        PropertyDefinition(name=n, data_type=t, required=False) for n, t in props
    ]
    return et


def test_uses_display_property_when_set() -> None:
    et = _et("ticker", ("ticker", "string"), ("name", "string"))
    assert et.resolve_display_property() == "ticker"


def test_falls_back_to_name() -> None:
    et = _et(None, ("name", "string"), ("age", "integer"))
    assert et.resolve_display_property() == "name"


def test_falls_back_to_title_when_no_name() -> None:
    et = _et(None, ("title", "string"), ("price", "float"))
    assert et.resolve_display_property() == "title"


def test_falls_back_to_first_string_property() -> None:
    et = _et(None, ("count", "integer"), ("code", "string"), ("other", "string"))
    assert et.resolve_display_property() == "code"


def test_returns_none_when_no_string_property() -> None:
    et = _et(None, ("count", "integer"), ("ratio", "float"))
    assert et.resolve_display_property() is None


def test_available_filters_candidates() -> None:
    # display_property set but that key isn't in available → try fallbacks
    et = _et("stale_field", ("name", "string"), ("code", "string"))
    assert et.resolve_display_property(available={"name", "code"}) == "name"


def test_available_filters_fallbacks_too() -> None:
    et = _et(None, ("name", "string"), ("code", "string"))
    assert et.resolve_display_property(available={"code"}) == "code"
