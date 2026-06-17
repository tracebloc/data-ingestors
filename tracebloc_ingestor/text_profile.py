"""Data-derived text profile for NLP datasets (#805).

Computed at ingest from the staged text — **no tokenizer involved** — and
shipped on the global-metadata channel so the backend can run a warn-only
tokenizer-fit check at dataset linking (does the contributor tokenizer cover
the data's scripts?). Only aggregate statistics cross the cluster boundary:
the Unicode-script mix and the document-length distribution. Never raw text,
never a vocabulary, never a hash (the FL guardrail).
"""

from __future__ import annotations

import logging
import os
import unicodedata
from collections import Counter
from typing import Any, Dict, List, Optional

from tracebloc_ingestor import Config

config = Config()
logger = logging.getLogger(__name__)

TEXT_PROFILE_SCHEMA_VERSION = 1

# Cap on files read per dataset — a profile is a summary, so a deterministic
# strided sample is enough and keeps the post-ingest pass bounded on large
# datasets.
_MAX_FILES = 2000

# Unicode script families named explicitly; anything else buckets to "Other".
# Derived from the ``unicodedata.name()`` prefix (stdlib — no script-table
# dependency). ``CJK`` stays upper-case; the rest are title-cased.
_SCRIPT_PREFIXES = (
    "LATIN",
    "CJK",
    "CYRILLIC",
    "GREEK",
    "ARABIC",
    "HEBREW",
    "HANGUL",
    "HIRAGANA",
    "KATAKANA",
    "DEVANAGARI",
    "THAI",
    "ARMENIAN",
    "GEORGIAN",
    "BENGALI",
    "TAMIL",
    "TELUGU",
    "GUJARATI",
    "KANNADA",
    "MALAYALAM",
    "ORIYA",
    "GURMUKHI",
    "SINHALA",
    "MYANMAR",
    "KHMER",
    "LAO",
    "TIBETAN",
    "ETHIOPIC",
    "CHEROKEE",
)


def _script_of(ch: str) -> str:
    """Best-effort Unicode script of a single letter character, via its name."""
    try:
        name = unicodedata.name(ch)
    except ValueError:
        return "Other"
    for prefix in _SCRIPT_PREFIXES:
        if name.startswith(prefix):
            return prefix if prefix == "CJK" else prefix.title()
    return "Other"


def _sample(paths: List[str], max_files: int) -> List[str]:
    """Deterministic strided sample of at most ``max_files`` paths."""
    if len(paths) <= max_files:
        return paths
    step = len(paths) / max_files
    return [paths[int(i * step)] for i in range(max_files)]


def compute_text_profile(
    cfg: Optional[Config] = None, max_files: int = _MAX_FILES
) -> Optional[Dict[str, Any]]:
    """Profile the staged text files under ``DEST_PATH`` (data-derived only).

    Returns the profile dict, or ``None`` when no readable text is staged (so
    the caller simply omits the field). ``cfg`` is the run's resolved Config;
    ``None`` falls back to the module-global ``config``.
    """
    cfg = cfg or config
    dest = cfg.DEST_PATH
    if not os.path.isdir(dest):
        return None
    paths = sorted(
        os.path.join(dest, name)
        for name in os.listdir(dest)
        if os.path.isfile(os.path.join(dest, name))
    )
    if not paths:
        return None

    script_counts: Counter = Counter()
    letters = 0
    char_total = 0
    word_total = 0
    docs = 0
    max_chars = 0
    max_words = 0

    for path in _sample(paths, max_files):
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as fh:
                text = fh.read()
        except OSError:
            continue
        docs += 1
        n_chars = len(text)
        n_words = len(text.split())
        char_total += n_chars
        word_total += n_words
        max_chars = max(max_chars, n_chars)
        max_words = max(max_words, n_words)
        for ch in text:
            if unicodedata.category(ch).startswith("L"):
                script_counts[_script_of(ch)] += 1
                letters += 1

    if docs == 0:
        return None

    scripts = (
        {s: round(c / letters, 4) for s, c in script_counts.most_common()}
        if letters
        else {}
    )
    profile = {
        "schema_version": TEXT_PROFILE_SCHEMA_VERSION,
        "docs_sampled": docs,
        "char_count": char_total,
        "scripts": scripts,
        "doc_length_chars": {"mean": round(char_total / docs, 2), "max": max_chars},
        "doc_length_words": {"mean": round(word_total / docs, 2), "max": max_words},
    }
    logger.info(f"Computed text profile for NLP dataset: {profile}")
    return profile
