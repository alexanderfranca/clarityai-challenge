from src.core.loader import load_yaml


def test_load_yaml_ok(temp_project):
    cfgpath = temp_project["configs"] / "contracts.yaml"
    cfgpath.write_text("a: 1\n", encoding="utf-8")
    data = load_yaml("contracts.yaml")
    assert data["a"] == 1


def test_load_yaml_invalid_top_level(temp_project):
    p = temp_project["configs"] / "bad.yaml"
    p.write_text("- 1\n- 2\n", encoding="utf-8")
    try:
        load_yaml("bad.yaml")
        assert False, "expected SystemExit on invalid mapping"
    except SystemExit as e:
        assert "Invalid format" in str(e)
