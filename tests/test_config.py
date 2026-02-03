from src.config import _parse_feature_flags


def test_parse_feature_flags_none():
    flags = _parse_feature_flags(None)
    assert flags["CS2_ENABLED"] is False
    assert flags["USE_REAL_RATE_LIMITS"] is True


def test_parse_feature_flags_json_valid():
    val = '{"CS2_ENABLED": true, "VALORANT_ENABLED": false}'
    flags = _parse_feature_flags(val)
    assert flags["CS2_ENABLED"] is True
    assert flags["VALORANT_ENABLED"] is False


def test_parse_feature_flags_json_coerce():
    val = '{"CS2_ENABLED": "true", "DOTA2_ENABLED": 1}'
    flags = _parse_feature_flags(val)
    assert flags["CS2_ENABLED"] is True
    # "1".lower() == "true" is False
    assert flags["DOTA2_ENABLED"] is False


def test_parse_feature_flags_json_null_safety():
    val = '{"CS2_ENABLED": null}'
    flags = _parse_feature_flags(val)
    assert flags["CS2_ENABLED"] is False


def test_parse_feature_flags_json_unknown_ignored():
    val = '{"UNKNOWN_FLAG": true, "CS2_ENABLED": true}'
    flags = _parse_feature_flags(val)
    assert "UNKNOWN_FLAG" not in flags
    assert flags["CS2_ENABLED"] is True


def test_parse_feature_flags_csv_valid():
    val = "CS2_ENABLED=true,VALORANT_ENABLED=false"
    flags = _parse_feature_flags(val)
    assert flags["CS2_ENABLED"] is True
    assert flags["VALORANT_ENABLED"] is False


def test_parse_feature_flags_csv_malformed():
    val = "CS2_ENABLED,VALORANT_ENABLED=true,=true"
    flags = _parse_feature_flags(val)
    assert flags["CS2_ENABLED"] is False
    assert flags["VALORANT_ENABLED"] is True


def test_parse_feature_flags_csv_unknown_ignored():
    val = "UNKNOWN=true,CS2_ENABLED=true"
    flags = _parse_feature_flags(val)
    assert "UNKNOWN" not in flags
    assert flags["CS2_ENABLED"] is True
