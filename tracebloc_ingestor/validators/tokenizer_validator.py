"""Tokenizer Validator Module.

Validates that a tokenizer.json file exists alongside the data and contains
the required special tokens ([MASK] and [PAD]) for masked language modeling.
Without these tokens the training client will fail with an embedding
out-of-bounds IndexError.
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

from .base import BaseValidator, ValidationResult
from ..config import Config

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


def _special_token_id(tokenizer_data: dict, token: str) -> Optional[int]:
    """Resolve a special token's id from a HuggingFace tokenizer JSON.

    Prefers the ``added_tokens`` list (where special tokens carry an explicit
    id), then falls back to the ``model.vocab`` mapping. Returns ``None`` when
    the token isn't present — e.g. classification tokenizers have no ``[MASK]``.
    """
    added = tokenizer_data.get("added_tokens", [])
    if isinstance(added, list):
        for entry in added:
            if isinstance(entry, dict) and entry.get("content") == token:
                token_id = entry.get("id")
                # Only trust an added_tokens entry that carries a real integer
                # id (bool excluded — JSON true/false is not a token id). A
                # missing or non-int id must not shadow the model.vocab mapping
                # below, nor let a non-scalar value reach the backend.
                if isinstance(token_id, int) and not isinstance(token_id, bool):
                    return token_id
    model = tokenizer_data.get("model")
    vocab = model.get("vocab") if isinstance(model, dict) else None
    if isinstance(vocab, dict):
        vid = vocab.get(token)
        if isinstance(vid, int) and not isinstance(vid, bool):
            return vid
    return None


def extract_tokenizer_metadata(tokenizer_data: dict) -> Dict[str, Any]:
    """Extract the 4 structural integers that fingerprint a tokenizer (#805).

    These — and only these — cross to the backend on the global-metadata
    channel so a contributor tokenizer can be cross-checked at dataset
    linking without ever shipping vocabulary content or a hash (the FL
    guardrail): the vocabulary of a custom (e.g. clinical / knowledge-graph)
    tokenizer is data-derived and must not be centrally fingerprinted.

    Returns a dict with:
      - ``vocab_size``:     number of distinct tokens (``model.vocab`` ∪
                            ``added_tokens``) — the bound the model's embedding
                            table must cover; matches ``len(tokenizer)`` and the
                            SDK's upload-time vocab-fit check.
      - ``mask_token_id``:  id of ``[MASK]``, or ``None`` (classification has
                            no ``[MASK]``).
      - ``pad_token_id``:   id of ``[PAD]``, or ``None``.
      - ``tokenizer_type``: ``model.type`` (``WordLevel`` / ``WordPiece`` /
                            ``BPE`` / ``Unigram``), or ``None``.
    """
    # Totality: a tokenizer.json that is valid JSON but not an object (e.g.
    # ``[]`` / ``"x"`` / ``null``) must not crash the post-ingest registration —
    # treat it as empty. The TokenizerValidator gate already rejects such files
    # upstream; this keeps the public helper safe for any caller.
    if not isinstance(tokenizer_data, dict):
        tokenizer_data = {}
    vocab = TokenizerValidator._extract_vocab(tokenizer_data) or set()
    model = tokenizer_data.get("model")
    tok_type = model.get("type") if isinstance(model, dict) else None
    # FL guardrail at the source: only scalar values ever leave the cluster.
    # Coerce a malformed non-int id / non-string type to None so a hand-crafted
    # tokenizer.json can't smuggle a nested object into the backend metadata.
    return {
        "vocab_size": len(vocab),
        "mask_token_id": _special_token_id(tokenizer_data, "[MASK]"),
        "pad_token_id": _special_token_id(tokenizer_data, "[PAD]"),
        "tokenizer_type": tok_type if isinstance(tok_type, str) else None,
    }


def load_tokenizer_metadata(tokenizer_path: str) -> Optional[Dict[str, Any]]:
    """Read ``tokenizer_path`` and return its structural fingerprint, or ``None``.

    Returns ``None`` (with a warning) when the file is absent or unparseable so
    callers on the post-ingest registration path never crash an already-committed
    dataset over a tokenizer read — presence and validity are enforced upstream
    by :class:`TokenizerValidator` at validation time.
    """
    if not os.path.isfile(tokenizer_path):
        return None
    try:
        with open(tokenizer_path, "r", encoding="utf-8") as f:
            tokenizer_data = json.load(f)
    except (OSError, ValueError) as e:
        logger.warning(f"Could not read tokenizer.json at {tokenizer_path}: {e}")
        return None
    if not isinstance(tokenizer_data, dict):
        logger.warning(
            f"tokenizer.json at {tokenizer_path} is not a JSON object "
            f"({type(tokenizer_data).__name__}); no fingerprint registered."
        )
        return None
    return extract_tokenizer_metadata(tokenizer_data)


class TokenizerValidator(BaseValidator):
    """Validator for tokenizer.json special-token requirements.

    Ensures that a tokenizer.json file exists at the configured data path
    and that its vocabulary includes all required special tokens.  For MLM
    the mandatory tokens are [MASK] (used to create training targets) and
    [PAD] (used to pad variable-length sequences in a batch).

    Attributes:
        required_tokens: Set of token strings that must appear in the vocab.
    """

    def __init__(
        self,
        required_tokens: tuple = ("[MASK]", "[PAD]"),
        name: str = "Tokenizer Validator",
        optional: bool = False,
    ):
        super().__init__(name)
        self.required_tokens = set(required_tokens)
        # When True, a missing tokenizer.json is a warning (not an error):
        # the training client falls back to the HuggingFace tokenizer_id /
        # default. Used by text/token classification, where the tokenizer is
        # optional. MLM keeps optional=False — its vocab IS the prediction
        # space, so a missing tokenizer must fail loud at ingest.
        self.optional = optional

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate tokenizer.json at the configured source path.

        Args:
            data: Unused (path is read from config.SRC_PATH).
            **kwargs: Additional validation parameters.

        Returns:
            ValidationResult with status and error details.
        """
        try:
            tokenizer_path = Path((self._config or config).SRC_PATH) / "tokenizer.json"

            if not tokenizer_path.exists():
                if self.optional:
                    warning = (
                        f"No tokenizer.json found at {tokenizer_path}. This is "
                        "optional for this category — the training client will "
                        "use the HuggingFace tokenizer_id / model_id (default "
                        "bert-base-uncased). Ship a tokenizer.json here only if "
                        "you need a custom tokenizer."
                    )
                    logger.warning(warning)
                    return self._create_result(
                        is_valid=True,
                        warnings=[warning],
                        metadata={
                            "path_checked": str(tokenizer_path),
                            "tokenizer_present": False,
                        },
                    )
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"tokenizer.json not found at {tokenizer_path}. "
                        "MLM training requires a tokenizer.json file alongside "
                        "the sequence data."
                    ],
                    metadata={"path_checked": str(tokenizer_path)},
                )

            with open(tokenizer_path, "r", encoding="utf-8") as f:
                tokenizer_data = json.load(f)

            vocab = self._extract_vocab(tokenizer_data)
            if vocab is None:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        "Could not extract vocabulary from tokenizer.json. "
                        "Expected a 'model.vocab' mapping or an 'added_tokens' list."
                    ],
                    metadata={"path_checked": str(tokenizer_path)},
                )

            missing = sorted(self.required_tokens - vocab)
            if missing:
                return self._create_result(
                    is_valid=False,
                    errors=[
                        f"Tokenizer is missing required special tokens: "
                        f"{', '.join(missing)}. "
                        f"Without these tokens, training will fail with an "
                        f"embedding out-of-bounds error. "
                        f"Re-train or update the tokenizer to include them."
                    ],
                    metadata={
                        "path_checked": str(tokenizer_path),
                        "missing_tokens": missing,
                        "required_tokens": sorted(self.required_tokens),
                    },
                )

            return self._create_result(
                is_valid=True,
                metadata={
                    "path_checked": str(tokenizer_path),
                    "required_tokens": sorted(self.required_tokens),
                    "vocab_size": len(vocab),
                },
            )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse tokenizer.json: {e}")
            return self._create_result(
                is_valid=False,
                errors=[f"tokenizer.json is not valid JSON: {e}"],
            )
        except Exception as e:
            logger.error(f"Tokenizer validation error: {e}")
            return self._create_result(
                is_valid=False,
                errors=[f"Tokenizer validation error: {str(e)}"],
            )

    @staticmethod
    def _extract_vocab(tokenizer_data: dict):
        """Extract the set of token strings from a HuggingFace tokenizer JSON.

        Checks both ``model.vocab`` (WordLevel / WordPiece / BPE) and
        ``added_tokens`` (special tokens added after training).

        Returns:
            Set of token strings, or None if the structure is unrecognised.
        """
        if not isinstance(tokenizer_data, dict):
            return None

        tokens = set()

        # model.vocab — the main vocabulary mapping
        # WordLevel/WordPiece/BPE store vocab as {token: id} dict.
        # Unigram stores vocab as [[token, score], ...] list.
        model = tokenizer_data.get("model")
        vocab = model.get("vocab") if isinstance(model, dict) else None
        if isinstance(vocab, dict):
            tokens.update(vocab.keys())
        elif isinstance(vocab, list):
            for entry in vocab:
                if isinstance(entry, (list, tuple)) and len(entry) >= 1:
                    tokens.add(str(entry[0]))

        # added_tokens — special tokens registered separately. Only count a
        # string ``content`` — a malformed non-string would be unhashable
        # (dict/list) or pollute the vocab count.
        added_tokens = tokenizer_data.get("added_tokens", [])
        if isinstance(added_tokens, list):
            for entry in added_tokens:
                if isinstance(entry, dict) and isinstance(entry.get("content"), str):
                    tokens.add(entry["content"])
                elif isinstance(entry, str):
                    tokens.add(entry)

        return tokens if tokens else None
