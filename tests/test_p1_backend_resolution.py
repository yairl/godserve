"""Unit test for GODSERVE_BACKEND resolution in the worker agent.

Backends are a pluggable seam: `local` is the built-in, `module:attr` is an
import-path plugin instantiated zero-arg, and anything else fails fast.
"""

from __future__ import annotations

import pytest

from godserve.worker.agent import _make_backend
from godserve.worker.backends.local import LocalBackend
from godserve.worker.envs.venv import VenvProvider
from tests.fixtures.dummy_backend import DummyBackend


def _make(tmp_path):
    provider = VenvProvider(str(tmp_path))
    return provider, str(tmp_path / "scratch")


def test_default_unset_is_local(tmp_path, monkeypatch):
    monkeypatch.delenv("GODSERVE_BACKEND", raising=False)
    provider, scratch = _make(tmp_path)
    assert isinstance(_make_backend(provider, scratch), LocalBackend)


def test_local_is_local(tmp_path, monkeypatch):
    monkeypatch.setenv("GODSERVE_BACKEND", "local")
    provider, scratch = _make(tmp_path)
    assert isinstance(_make_backend(provider, scratch), LocalBackend)


def test_import_path_resolves(tmp_path, monkeypatch):
    monkeypatch.setenv("GODSERVE_BACKEND", "tests.fixtures.dummy_backend:DummyBackend")
    provider, scratch = _make(tmp_path)
    assert isinstance(_make_backend(provider, scratch), DummyBackend)


def test_bareword_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("GODSERVE_BACKEND", "nonexistent")
    provider, scratch = _make(tmp_path)
    with pytest.raises(ValueError):
        _make_backend(provider, scratch)


def test_bad_import_path_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("GODSERVE_BACKEND", "no.such.module:X")
    provider, scratch = _make(tmp_path)
    with pytest.raises(ValueError):
        _make_backend(provider, scratch)
