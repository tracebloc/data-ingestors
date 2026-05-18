"""Tokenizer Validator Module.

Validates that a tokenizer.json file exists alongside the data and contains
the required special tokens ([MASK] and [PAD]) for masked language modeling.
Without these tokens the training client will fail with an embedding
out-of-bounds IndexError.
"""

import json
import logging
from pathlib import Path
from typing import Any

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


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
    ):
        super().__init__(name)
        self.required_tokens = set(required_tokens)

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate tokenizer.json at the configured source path.

        Args:
            data: Unused (path is read from config.SRC_PATH).
            **kwargs: Additional validation parameters.

        Returns:
            ValidationResult with status and error details.
        """
        try:
            tokenizer_path = Path(config.SRC_PATH) / "tokenizer.json"

            if not tokenizer_path.exists():
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
        tokens = set()

        # model.vocab — the main vocabulary mapping
        model = tokenizer_data.get("model", {})
        vocab = model.get("vocab")
        if isinstance(vocab, dict):
            tokens.update(vocab.keys())

        # added_tokens — special tokens registered separately
        added_tokens = tokenizer_data.get("added_tokens", [])
        if isinstance(added_tokens, list):
            for entry in added_tokens:
                if isinstance(entry, dict) and "content" in entry:
                    tokens.update([entry["content"]])
                elif isinstance(entry, str):
                    tokens.add(entry)

        return tokens if tokens else None
