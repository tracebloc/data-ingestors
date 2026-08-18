"""Duplicate Validator Module.

This module provides validation to check if the destination directory exists,
raising errors if it does to prevent accidental overwrites.
"""

from pathlib import Path
from typing import Any, List, Optional
import logging

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.fs import ensure_reclaimable_dir

config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class DuplicateValidator(BaseValidator):
    """
    This validator checks if the destination directory already exists.

    It raises errors if the directory already exists,
    preventing accidental overwrites.

    Attributes:
        dest_path: Destination path to check
    """

    def __init__(
        self,
        dest_path: Optional[str] = None,
        name: str = "Duplicate Validator",
        data_id_strategy: Optional[str] = None,
        unique_id_column: Optional[str] = None,
    ):
        """Initialize the duplicate validator.

        Args:
            dest_path: Destination path to check (defaults to config.DEST_PATH)
            name: Human-readable name of the validator
            data_id_strategy: The run's ``data_id`` strategy (``"content_hash"``
                / ``"uuid"``), threaded in by ``map_validators`` so the
                within-CSV duplicate warning describes what actually happens to
                a duplicate row (#377). ``None`` (direct construction) keeps a
                strategy-agnostic wording that covers both outcomes.
            unique_id_column: The run's ``data_id`` source column, if any. It
                wins over ``data_id_strategy`` in ``RecordProcessor``, so it
                wins here too.
        """
        super().__init__(name)
        self._data_id_strategy = data_id_strategy
        self._unique_id_column = unique_id_column
        # Store the explicit override only; the config-backed default is
        # resolved lazily in the ``dest_path`` property so the run's injected
        # Config (bound by ``map_validators`` AFTER construction, P4b) wins.
        # Resolving here would snapshot the module-global DEST_PATH before the
        # bind and ignore the injected value.
        self._dest_path = dest_path

    @property
    def dest_path(self) -> str:
        """Destination directory: explicit override, else the bound Config's
        ``DEST_PATH`` (falling back to the module-global ``config`` when this
        validator was constructed outside the registry flow)."""
        return self._dest_path or (self._config or config).DEST_PATH

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate directory existence.

        Args:
            data: Not used, but required by base class
            **kwargs: Additional validation parameters

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            errors = []
            warnings = []
            metadata = {"dest_path": self.dest_path, "directory_exists": False}

            # Check destination directory existence
            directory_exists = self._check_directory_exists()
            metadata["directory_exists"] = directory_exists

            if directory_exists:
                # An empty directory typically means a previous ingestion
                # attempt aborted before any data landed. Reusing it is
                # safe and lets customers rerun without manual PVC cleanup.
                # A populated directory is treated as a real collision.
                is_empty = self._is_directory_empty()
                metadata["directory_empty"] = is_empty
                if is_empty:
                    warnings.append(
                        f"Destination directory '{self.dest_path}' exists but "
                        "is empty (likely from a previous failed run); reusing."
                    )
                else:
                    errors.append(
                        f"Destination directory '{self.dest_path}' already exists"
                    )

            # Within-CSV duplicate filename detection (warning). The check
            # above guards re-ingesting into an existing TABLE; this catches the
            # same filename appearing more than once in the INCOMING CSV, which
            # would otherwise ingest as separate records with no notice. Warn
            # only — repeated filenames may be intentional in some flows.
            dup_warnings = self._within_csv_duplicate_warnings(data)
            warnings.extend(dup_warnings)
            metadata["within_csv_duplicate_filenames"] = len(dup_warnings) > 0

            # Check if parent directory exists (for creating the destination)
            parent_dir = Path(self.dest_path).parent
            parent_exists = parent_dir.exists()
            metadata["parent_directory_exists"] = parent_exists

            if not parent_exists:
                warnings.append(
                    f"Parent directory '{parent_dir}' does not exist and will be created"
                )

            is_valid = len(errors) == 0

            return self._create_result(
                is_valid=is_valid, errors=errors, warnings=warnings, metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error during duplicate validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Duplicate validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _within_csv_duplicate_warnings(self, data: Any) -> List[str]:
        """Warn when a filename appears more than once in the incoming CSV.

        Bounded: reads only the ``filename`` column. A non-CSV input (e.g. a
        ``None`` from the table-only callers, or a JSON path) is a no-op, and a
        CSV with no ``filename`` column (tabular families) is skipped.
        """
        if not isinstance(data, (str, Path)) or Path(data).suffix.lower() != ".csv":
            return []

        import pandas as pd

        try:
            header = pd.read_csv(data, nrows=0, encoding="utf-8").columns
        except Exception:  # noqa: BLE001 — header probe is best-effort
            return []
        filename_col = self._match_column(header, "filename")
        if filename_col is None:
            return []

        try:
            series = pd.read_csv(
                data,
                usecols=[filename_col],
                encoding="utf-8",
                dtype=str,
                keep_default_na=False,
            )[filename_col]
        except Exception:  # noqa: BLE001 — let other validators surface read errors
            return []

        names = series.map(lambda v: str(v).strip())
        names = names[names != ""]
        counts = names.value_counts()
        duplicated = counts[counts > 1]
        if duplicated.empty:
            return []

        extra = int(duplicated.sum() - len(duplicated))  # rows beyond the first
        sample = list(duplicated.index[:10])
        suffix = " …" if len(duplicated) > 10 else ""
        return [
            f"{len(duplicated)} filename(s) appear more than once in the CSV "
            f"({extra} duplicate row(s)): {sample}{suffix}. "
            f"{self._duplicate_outcome_sentence()} Remove the duplicates if "
            f"this is unintended."
        ]

    def _duplicate_outcome_sentence(self) -> str:
        """What actually happens to a repeated filename, per ``data_id`` source.

        Under the ``content_hash`` default (#350) the warning's old "these will
        be ingested as separate records" is only half-true: byte-identical rows
        now collapse into one stored row via the ``data_id`` UNIQUE upsert
        (#225), while same-filename/different-content rows still land
        separately. ``uuid`` and an explicit id column both keep every row
        (#377).
        """
        if self._unique_id_column:
            return (
                f"Each row keeps its own record — data_id comes from the "
                f"{self._unique_id_column!r} column, so rows with distinct ids "
                f"are ingested separately."
            )
        if self._data_id_strategy == "uuid":
            return (
                "These will be ingested as separate records (data_id strategy "
                "'uuid' assigns a fresh id per row)."
            )
        if self._data_id_strategy == "content_hash":
            return (
                "Under data_id strategy 'content_hash', rows that are identical "
                "in both filename and content collapse into a single stored "
                "record; rows sharing a filename but differing in content (e.g. "
                "a different label) are ingested separately."
            )
        # Strategy unknown (validator constructed outside ``map_validators``):
        # describe both outcomes rather than assert the wrong one.
        return (
            "Depending on the run's data_id strategy these either collapse into "
            "one stored record (content_hash, when filename AND content are "
            "identical) or are ingested as separate records (uuid, or an "
            "explicit id column)."
        )

    def _check_directory_exists(self) -> bool:
        """Check if the destination directory exists.

        Returns:
            True if directory exists, False otherwise
        """
        try:
            dest_path = Path(self.dest_path)
            return dest_path.exists() and dest_path.is_dir()
        except Exception as e:
            logger.error(f"Error checking directory existence: {str(e)}")
            return False

    def _is_directory_empty(self) -> bool:
        """Return True iff dest_path is an empty directory.

        Returns False on any error so the validator falls back to the
        existing "fail loudly" behavior rather than masking a real issue.
        """
        try:
            return not any(Path(self.dest_path).iterdir())
        except Exception as e:
            logger.error(f"Error checking if directory is empty: {str(e)}")
            return False

    def _create_directory_if_needed(self) -> bool:
        """
        Create the destination directory if it doesn't exist.

        Returns:
            True if directory was created or already exists, False otherwise
        """
        try:
            dest_path = Path(self.dest_path)
            if not dest_path.exists():
                # Shared helper, not a bare mkdir: validation runs BEFORE the
                # transfer, so whoever creates this directory first decides its
                # mode -- and file_transfer deliberately does not re-chmod a
                # directory that already exists. A bare mkdir here therefore left
                # the tree at the umask default (0755, owned by the ingest uid),
                # which the `data delete` teardown pod -- a different uid -- cannot
                # remove: "rm: can't remove ...: Permission denied", with the
                # table already dropped. See utils/fs.py.
                ensure_reclaimable_dir(str(dest_path))
                logger.info(f"Created destination directory: {self.dest_path}")
            return True
        except Exception as e:
            logger.error(f"Error creating directory: {str(e)}")
            return False
