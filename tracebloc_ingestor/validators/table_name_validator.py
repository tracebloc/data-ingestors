"""Table Name Validator Module.

This module provides validation for table names to ensure they only contain
alphanumeric characters and underscores, following database naming conventions.
The validator reads the table name from the configuration automatically.
"""

import re
from typing import Any, List
import logging

from .base import BaseValidator, ValidationResult
from ..config import Config
from ..utils.logging import setup_logging

config = Config()
setup_logging(config)
logger = logging.getLogger(__name__)
logger.setLevel(config.LOG_LEVEL)


class TableNameValidator(BaseValidator):
    """Validator for ensuring table names follow proper naming conventions.

    This validator checks that table names contain only alphanumeric characters
    and underscores, which is a common requirement for database table names.
    The table name is automatically read from the configuration.

    Attributes:
        pattern: Regular expression pattern for valid table names
    """

    def __init__(self, name: str = "Table Name Validator"):
        """Initialize the table name validator.

        Args:
            name: Human-readable name of the validator
        """
        super().__init__(name)
        # Pattern: only alphanumeric characters and underscores
        # Must start with a letter
        self.pattern = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

    def validate(self, data: Any, **kwargs) -> ValidationResult:
        """Validate table names from config.

        Args:
            data:table name comes from config
            **kwargs: Additional validation parameters

        Returns:
            ValidationResult containing validation status and messages
        """
        try:
            # Get table name from config
            table_name = config.TABLE_NAME

            if not table_name:
                return self._create_result(
                    is_valid=False,
                    errors=["No table name found in configuration"],
                    metadata={"table_names_checked": 0},
                )

            # Validate the table name from config
            return self._validate_table_names([table_name])

        except Exception as e:
            logger.error(f"Error during table name validation: {str(e)}")
            return self._create_result(
                is_valid=False,
                errors=[f"Validation error: {str(e)}"],
                metadata={"error_type": "validation_exception"},
            )

    def _validate_table_names(self, table_names: List[str]) -> ValidationResult:
        """Validate a list of table names.

        Args:
            table_names: List of table names to validate

        Returns:
            ValidationResult containing validation status and messages
        """
        if not table_names:
            return self._create_result(
                is_valid=False,
                errors=["No table names to validate"],
                metadata={"table_names_checked": 0},
            )

        invalid_names = []
        warnings = []

        # Create progress bar
        progress_bar = self._create_progress_bar(
            len(table_names), "Validating table names"
        )

        try:
            for table_name in table_names:
                if not isinstance(table_name, str):
                    invalid_names.append(f"'{table_name}' (not a string)")
                    continue

                # Check if table name is empty
                if not table_name.strip():
                    invalid_names.append("'' (empty table name)")
                    continue

                # Check if table name matches the pattern
                if not self.pattern.match(table_name):
                    invalid_names.append(
                        f"'{table_name}' (contains invalid characters)"
                    )
                    continue

                # Check for reserved keywords (optional warning)
                if self._is_reserved_keyword(table_name):
                    warnings.append(f"'{table_name}' is a common reserved keyword")

                # Update progress bar
                if progress_bar:
                    progress_bar.update(1)
        finally:
            # Close progress bar
            if progress_bar:
                progress_bar.close()

        # Check for invalid table names
        if invalid_names:
            return self._create_result(
                is_valid=False,
                errors=[
                    f"Invalid table names found: {invalid_names}",
                    "Table names must contain only alphanumeric characters and underscores, and must start with a letter or underscore.",
                ],
                metadata={
                    "table_names_checked": len(table_names),
                    "invalid_names": invalid_names,
                    "valid_pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$",
                },
            )

        # Success case
        return self._create_result(
            is_valid=True,
            warnings=warnings,
            metadata={
                "table_names_checked": len(table_names),
                "valid_names": table_names,
                "valid_pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$",
            },
        )

    def _is_reserved_keyword(self, table_name: str) -> bool:
        """Check if a table name is a common reserved keyword.

        Args:
            table_name: Table name to check

        Returns:
            True if the table name is a reserved keyword, False otherwise
        """
        # Common SQL reserved keywords that should be avoided
        reserved_keywords = {
            "select",
            "from",
            "where",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "alter",
            "table",
            "database",
            "index",
            "view",
            "procedure",
            "function",
            "trigger",
            "constraint",
            "primary",
            "foreign",
            "key",
            "unique",
            "check",
            "default",
            "null",
            "not",
            "and",
            "or",
            "in",
            "like",
            "between",
            "is",
            "as",
            "order",
            "group",
            "by",
            "having",
            "union",
            "join",
            "inner",
            "left",
            "right",
            "outer",
            "full",
            "cross",
            "natural",
            "on",
            "using",
            "case",
            "when",
            "then",
            "else",
            "end",
            "if",
            "exists",
            "all",
            "any",
            "some",
            "distinct",
            "top",
            "limit",
            "offset",
            "fetch",
            "with",
            "recursive",
            "window",
            "over",
            "partition",
            "rows",
            "range",
            "preceding",
            "following",
            "current",
            "row",
            "unbounded",
            "first",
            "last",
            "value",
            "values",
            "set",
            "into",
            "values",
            "returning",
            "begin",
            "commit",
            "rollback",
            "transaction",
            "savepoint",
            "release",
            "lock",
            "unlock",
            "grant",
            "revoke",
            "deny",
            "exec",
            "execute",
            "sp_",
            "xp_",
            "fn_",
            "dt_",
            "user",
            "role",
            "schema",
            "catalog",
            "session",
            "system",
            "public",
            "private",
            "protected",
            "internal",
            "external",
            "temporary",
            "temp",
        }

        return table_name.lower() in reserved_keywords
