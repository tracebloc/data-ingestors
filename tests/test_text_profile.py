"""Tests for the data-derived NLP text profile (#805).

The profile is computed from the staged text with NO tokenizer; only aggregate
statistics (Unicode-script mix + length distribution) are produced.
"""

from types import SimpleNamespace

from tracebloc_ingestor.text_profile import (
    TEXT_PROFILE_SCHEMA_VERSION,
    _script_of,
    compute_text_profile,
)


def _cfg(dest):
    return SimpleNamespace(DEST_PATH=str(dest))


def _write(dest, name, text):
    (dest / name).write_text(text, encoding="utf-8")


def test_script_of_common_scripts():
    assert _script_of("a") == "Latin"
    assert _script_of("Z") == "Latin"
    assert _script_of("я") == "Cyrillic"
    assert _script_of("猫") == "CJK"
    assert _script_of("あ") == "Hiragana"
    assert _script_of("ا") == "Arabic"
    # whitespace / unnamed control chars are not letters -> Other
    assert _script_of(" ") == "Other"


def test_profile_latin_english(tmp_path):
    _write(tmp_path, "a.txt", "hello world")
    _write(tmp_path, "b.txt", "the quick brown fox")
    p = compute_text_profile(_cfg(tmp_path))
    assert p["schema_version"] == TEXT_PROFILE_SCHEMA_VERSION
    assert p["docs_sampled"] == 2
    assert p["scripts"] == {"Latin": 1.0}
    assert p["doc_length_words"]["max"] == 4
    assert p["doc_length_chars"]["max"] == len("the quick brown fox")


def test_profile_detects_cjk(tmp_path):
    _write(tmp_path, "a.txt", "猫が好き")  # Japanese: CJK + Hiragana letters
    p = compute_text_profile(_cfg(tmp_path))
    assert "CJK" in p["scripts"]
    assert abs(sum(p["scripts"].values()) - 1.0) < 1e-6


def test_profile_mixed_scripts_proportions(tmp_path):
    _write(tmp_path, "a.txt", "aя")  # one Latin letter, one Cyrillic letter
    p = compute_text_profile(_cfg(tmp_path))
    assert p["scripts"]["Latin"] == 0.5
    assert p["scripts"]["Cyrillic"] == 0.5


def test_profile_none_when_empty_dir(tmp_path):
    assert compute_text_profile(_cfg(tmp_path)) is None


def test_profile_none_when_dir_missing(tmp_path):
    assert compute_text_profile(_cfg(tmp_path / "nope")) is None


def test_profile_sampling_cap(tmp_path):
    for i in range(10):
        _write(tmp_path, f"f{i}.txt", "word")
    p = compute_text_profile(_cfg(tmp_path), max_files=4)
    assert p["docs_sampled"] == 4
