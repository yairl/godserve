"""Unit test for the worker's execution backend construction.

godserve always executes via the built-in LocalBackend. ``GODSERVE_BACKEND`` is
an opaque label inherited by job subprocesses (setup.sh + serve process), never
resolved by core into a backend selection — so whatever its value, the agent
constructs a LocalBackend.
"""

from __future__ import annotations

import pytest

from godserve.worker.agent import _make_backend
from godserve.worker.backends.local import LocalBackend
from godserve.worker.envs.venv import VenvProvider


def _make(tmp_path):
    provider = VenvProvider(str(tmp_path))
    return provider, str(tmp_path / "scratch")


@pytest.mark.parametrize(
    "label",
    [None, "local", "runpod", "examples.runpod_backend:RunpodBackend"],
)
def test_backend_is_always_local(tmp_path, monkeypatch, label):
    if label is None:
        monkeypatch.delenv("GODSERVE_BACKEND", raising=False)
    else:
        monkeypatch.setenv("GODSERVE_BACKEND", label)
    provider, scratch = _make(tmp_path)
    assert isinstance(_make_backend(provider, scratch), LocalBackend)
