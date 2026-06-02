"""Shared fixtures for the test suite.

Validators and ingestors snapshot ``config = Config()`` at import time but
read ``os.environ`` lazily on each property access, so tests set env vars via
``monkeypatch.setenv`` and the module-level config picks them up. The
``clean_env`` fixture strips the env vars these tests touch so a developer's
shell environment can't leak into a run.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from unittest.mock import MagicMock

import pandas as pd
import pytest


_ENV_VARS = (
    "SRC_PATH", "LABEL_FILE", "TABLE_NAME", "TITLE",
    "BACKEND_TOKEN", "CLIENT_ID", "CLIENT_PASSWORD",
    "CLIENT_ENV", "LOG_LEVEL", "BATCH_SIZE",
    "MYSQL_HOST", "MYSQL_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME",
)


@pytest.fixture
def clean_env(monkeypatch):
    """Strip env vars the ingestor config reads, so the host shell can't leak in."""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.fixture
def make_csv(tmp_path):
    """Write rows to a temp CSV and return its path.

    Accepts either a pandas DataFrame or a list-of-dicts. ``name`` lets a
    single test create several distinct files.
    """

    def _make(rows, name: str = "data.csv") -> Path:
        df = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
        path = tmp_path / name
        df.to_csv(path, index=False)
        return path

    return _make


@pytest.fixture
def make_voc_xml(tmp_path):
    """Write a Pascal VOC annotation XML and return its path.

    ``objects`` is a list of dicts with keys: name, xmin, ymin, xmax, ymax.
    Pass ``raw=`` to write arbitrary XML text instead (for malformed cases).
    """

    def _make(
        objects: Optional[Sequence[Dict[str, Any]]] = None,
        *,
        filename: str = "img.jpg",
        width: int = 640,
        height: int = 480,
        depth: int = 3,
        name: str = "ann.xml",
        raw: Optional[str] = None,
    ) -> Path:
        path = tmp_path / name
        if raw is not None:
            path.write_text(raw, encoding="utf-8")
            return path

        obj_xml = ""
        for obj in objects or []:
            obj_xml += f"""
  <object>
    <name>{obj['name']}</name>
    <bndbox>
      <xmin>{obj['xmin']}</xmin>
      <ymin>{obj['ymin']}</ymin>
      <xmax>{obj['xmax']}</xmax>
      <ymax>{obj['ymax']}</ymax>
    </bndbox>
  </object>"""

        xml = f"""<annotation>
  <filename>{filename}</filename>
  <size>
    <width>{width}</width>
    <height>{height}</height>
    <depth>{depth}</depth>
  </size>{obj_xml}
</annotation>"""
        path.write_text(xml, encoding="utf-8")
        return path

    return _make


@pytest.fixture
def make_tokenizer(tmp_path):
    """Write a HuggingFace-style tokenizer.json into a dir, return that dir.

    ``vocab`` is the model.vocab token list; ``added`` is the added_tokens
    content list. Pass ``raw=`` to write invalid JSON.
    """

    def _make(
        vocab: Optional[Sequence[str]] = ("hello", "world", "[MASK]", "[PAD]"),
        added: Optional[Sequence[str]] = None,
        *,
        subdir: str = "src",
        raw: Optional[str] = None,
    ) -> Path:
        src = tmp_path / subdir
        src.mkdir(parents=True, exist_ok=True)
        path = src / "tokenizer.json"
        if raw is not None:
            path.write_text(raw, encoding="utf-8")
            return src

        body: Dict[str, Any] = {"model": {}}
        if vocab is not None:
            body["model"]["vocab"] = {tok: i for i, tok in enumerate(vocab)}
        if added is not None:
            body["added_tokens"] = [{"content": t} for t in added]
        path.write_text(json.dumps(body), encoding="utf-8")
        return src

    return _make


@pytest.fixture
def make_image(tmp_path):
    """Write a solid-color image of a given size and return its path."""
    from PIL import Image

    def _make(
        size=(64, 64),
        *,
        name: str = "img.jpg",
        color=(128, 128, 128),
        mode: str = "RGB",
    ) -> Path:
        path = tmp_path / name
        Image.new(mode, size, color).save(path)
        return path

    return _make


@pytest.fixture
def mock_database():
    """A MagicMock standing in for tracebloc_ingestor.database.Database."""
    db = MagicMock(name="Database")
    return db


@pytest.fixture
def mock_api_client():
    """A MagicMock standing in for tracebloc_ingestor.api.client.APIClient."""
    api = MagicMock(name="APIClient")
    return api
