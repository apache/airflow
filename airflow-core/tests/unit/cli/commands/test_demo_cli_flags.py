import pytest
from airflow.cli.commands import variable_command

class DummyArgs:
    output = None
    verbose = False
    show_values = True
    hide_sensitive = False

def test_variables_list_show_values(monkeypatch):
    output = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: output.append(args))
    variable_command.variables_list(DummyArgs())
    assert output  # Checks that something was printed

class DummyArgsHide:
    output = None
    verbose = False
    show_values = False
    hide_sensitive = True

def test_variables_list_hide_sensitive(monkeypatch):
    output = []
    monkeypatch.setattr("builtins.print", lambda *args, **kwargs: output.append(args))
    variable_command.variables_list(DummyArgsHide())
    assert output
