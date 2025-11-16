import pytest
from scheduler.inputs.creator import create_input_def
from scheduler.models import EspressoListInputDefinition


def test_create_input_def_list():
    input_def = create_input_def(
        id="test_list_input", type="list", items=["a", "b", "c"]
    )

    assert isinstance(input_def, EspressoListInputDefinition)
    assert input_def.id == "test_list_input"
    assert input_def.type == "list"
    assert input_def.items == ["a", "b", "c"]
