import importlib
import pytest

MODULES = [
    "src.core.loader",
    "src.core.audit",
    "src.core.planner",
    "src.core.validator",
    # add the rest here...
]


@pytest.mark.parametrize("mod", MODULES)
def test_modules_are_importable(mod):
    m = importlib.import_module(mod)
    assert m is not None
