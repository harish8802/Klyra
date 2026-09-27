# src/klyra/validate.py

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Type

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# ============================================================
# 1. STANDARD RESULT
# ============================================================

@dataclass
class ValidationResult:
    rule_name: str
    status: str                  # PASS / WARN / FAIL
    severity: str                # INFO / WARN / ERROR
    message: str
    records_checked: int = 0
    records_failed: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ============================================================
# 2. BASE VALIDATION RULE
# ============================================================

class ValidationRule:
    """
    Every validation rule follows the same contract.

    Input:
        df          -> Spark DataFrame
        rule_config -> configuration for this rule

    Output:
        ValidationResult
    """

    name: str = ""

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:
        raise NotImplementedError


# ============================================================
# 3. NON-NULL VALIDATOR
# ============================================================

class NonNullableValidator(ValidationRule):

    name = "non_nullable"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        columns = rule_config.get("columns", [])
        severity = rule_config.get("severity", "ERROR")

        if not columns:
            return ValidationResult(
                rule_name=self.name,
                status="PASS",
                severity=severity,
                message="No columns configured for non-null validation."
            )

        missing_columns = [
            column for column in columns
            if column not in df.columns
        ]

        if missing_columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Columns not found in DataFrame: {missing_columns}"
            )

        # Check all configured columns in one aggregation
        expressions = [
            F.sum(
                F.when(F.col(column).isNull(), 1).otherwise(0)
            ).alias(column)
            for column in columns
        ]

        result = df.agg(*expressions).collect()[0]

        failed_columns = []
        failed_records = 0

        for column in columns:
            null_count = result[column]

            if null_count and null_count > 0:
                failed_columns.append(f"{column}={null_count}")
                failed_records += null_count

        if failed_columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    "Null values found in mandatory columns: "
                    + ", ".join(failed_columns)
                ),
                records_checked=df.count(),
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message="All mandatory columns passed null validation.",
            records_checked=df.count(),
            records_failed=0
        )


# ============================================================
# 4. UNIQUE KEY VALIDATOR
# ============================================================

class UniqueKeyValidator(ValidationRule):

    name = "unique_key"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        columns = rule_config.get("columns", [])
        severity = rule_config.get("severity", "ERROR")

        if not columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message="No key columns configured."
            )

        missing_columns = [
            column for column in columns
            if column not in df.columns
        ]

        if missing_columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Key columns not found: {missing_columns}"
            )

        total_count = df.count()

        null_key_filter = None

        for column in columns:
            condition = F.col(column).isNull()

            if null_key_filter is None:
                null_key_filter = condition
            else:
                null_key_filter = null_key_filter | condition

        null_key_count = df.filter(null_key_filter).count()

        duplicate_count = (
            df.filter(~null_key_filter)
              .groupBy(*columns)
              .count()
              .filter(F.col("count") > 1)
              .agg(
                  F.sum(F.col("count") - 1).alias("duplicate_count")
              )
              .collect()[0]["duplicate_count"]
        )

        duplicate_count = duplicate_count or 0

        failed_records = null_key_count + duplicate_count

        if failed_records > 0:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    f"Key validation failed. "
                    f"null_key_records={null_key_count}, "
                    f"duplicate_records={duplicate_count}"
                ),
                records_checked=total_count,
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Key validation passed for columns: {columns}",
            records_checked=total_count,
            records_failed=0
        )


# ============================================================
# 5. LENGTH VALIDATOR
# ============================================================

class LengthValidator(ValidationRule):

    name = "length"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        column = rule_config.get("column")
        min_length = rule_config.get("min")
        max_length = rule_config.get("max")
        severity = rule_config.get("severity", "ERROR")

        if not column:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message="Length rule requires a column."
            )

        if column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Column not found: {column}"
            )

        condition = None

        if min_length is not None:
            condition = F.length(F.col(column)) < min_length

        if max_length is not None:
            max_condition = F.length(F.col(column)) > max_length

            if condition is None:
                condition = max_condition
            else:
                condition = condition | max_condition

        failed_records = df.filter(
            F.col(column).isNotNull() & condition
        ).count()

        total_count = df.count()

        if failed_records > 0:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    f"Length validation failed for '{column}'. "
                    f"min={min_length}, max={max_length}"
                ),
                records_checked=total_count,
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Length validation passed for '{column}'.",
            records_checked=total_count
        )


# ============================================================
# 6. NUMERIC ONLY VALIDATOR
# ============================================================

class NumericOnlyValidator(ValidationRule):

    name = "numeric_only"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        column = rule_config.get("column")
        severity = rule_config.get("severity", "ERROR")

        if column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Column not found: {column}"
            )

        failed_records = (
            df.filter(
                F.col(column).isNotNull()
                & ~F.col(column).cast("string").rlike("^[0-9]+$")
            )
            .count()
        )

        total_count = df.count()

        if failed_records > 0:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=f"Non-numeric values found in '{column}'.",
                records_checked=total_count,
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Numeric validation passed for '{column}'.",
            records_checked=total_count
        )


# ============================================================
# 7. ALLOWED VALUES VALIDATOR
# ============================================================

class AllowedValuesValidator(ValidationRule):

    name = "allowed_values"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        column = rule_config.get("column")
        allowed_values = rule_config.get("values", [])
        severity = rule_config.get("severity", "ERROR")

        if column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Column not found: {column}"
            )

        failed_records = (
            df.filter(
                F.col(column).isNotNull()
                & ~F.col(column).isin(allowed_values)
            )
            .count()
        )

        total_count = df.count()

        if failed_records > 0:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    f"Invalid values found in '{column}'. "
                    f"Allowed={allowed_values}"
                ),
                records_checked=total_count,
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Allowed-value validation passed for '{column}'.",
            records_checked=total_count
        )


# ============================================================
# 8. RANGE VALIDATOR
# ============================================================

class RangeValidator(ValidationRule):

    name = "range"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        column = rule_config.get("column")
        minimum = rule_config.get("min")
        maximum = rule_config.get("max")
        severity = rule_config.get("severity", "ERROR")

        if column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=f"Column not found: {column}"
            )

        condition = None

        if minimum is not None:
            condition = F.col(column) < minimum

        if maximum is not None:
            max_condition = F.col(column) > maximum

            if condition is None:
                condition = max_condition
            else:
                condition = condition | max_condition

        failed_records = (
            df.filter(
                F.col(column).isNotNull() & condition
            )
            .count()
        )

        total_count = df.count()

        if failed_records > 0:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    f"Range validation failed for '{column}'. "
                    f"min={minimum}, max={maximum}"
                ),
                records_checked=total_count,
                records_failed=failed_records
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Range validation passed for '{column}'.",
            records_checked=total_count
        )


# ============================================================
# 9. ROW COUNT VALIDATOR
# ============================================================

class RowCountValidator(ValidationRule):

    name = "row_count"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        minimum = rule_config.get("min")
        maximum = rule_config.get("max")
        severity = rule_config.get("severity", "ERROR")

        total_count = df.count()

        failed = False

        if minimum is not None and total_count < minimum:
            failed = True

        if maximum is not None and total_count > maximum:
            failed = True

        if failed:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL" if severity == "ERROR" else "WARN",
                severity=severity,
                message=(
                    f"Row count validation failed. "
                    f"actual={total_count}, "
                    f"min={minimum}, max={maximum}"
                ),
                records_checked=total_count,
                records_failed=total_count
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity=severity,
            message=f"Row count validation passed. actual={total_count}",
            records_checked=total_count
        )


# ============================================================
# 10. SCHEMA COMPATIBILITY VALIDATOR
# ============================================================

class SchemaCompatibilityValidator(ValidationRule):

    name = "schema"

    def validate(
        self,
        df: DataFrame,
        rule_config: Dict[str, Any],
    ) -> ValidationResult:

        expected_schema: Optional[StructType] = rule_config.get(
            "expected_schema"
        )

        if expected_schema is None:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message="Expected target schema was not provided."
            )

        source_fields = {
            field.name: field
            for field in df.schema.fields
        }

        target_fields = {
            field.name: field
            for field in expected_schema.fields
        }

        missing_columns = [
            column
            for column in target_fields
            if column not in source_fields
        ]

        extra_columns = [
            column
            for column in source_fields
            if column not in target_fields
        ]

        type_mismatches = []

        for column in target_fields:
            if column in source_fields:
                source_type = source_fields[column].dataType
                target_type = target_fields[column].dataType

                if source_type != target_type:
                    type_mismatches.append(
                        f"{column}: "
                        f"{source_type} -> {target_type}"
                    )

        if missing_columns or type_mismatches:
            return ValidationResult(
                rule_name=self.name,
                status="FAIL",
                severity="ERROR",
                message=(
                    f"Schema mismatch. "
                    f"missing={missing_columns}, "
                    f"extra={extra_columns}, "
                    f"type_mismatches={type_mismatches}"
                )
            )

        return ValidationResult(
            rule_name=self.name,
            status="PASS",
            severity="ERROR",
            message="Source schema is compatible with target schema."
        )


# ============================================================
# 11. VALIDATION ENGINE
# ============================================================

class ValidationEngine:

    RULE_REGISTRY: Dict[str, Type[ValidationRule]] = {
        "schema": SchemaCompatibilityValidator,
        "non_nullable": NonNullableValidator,
        "unique_key": UniqueKeyValidator,
        "length": LengthValidator,
        "numeric_only": NumericOnlyValidator,
        "allowed_values": AllowedValuesValidator,
        "range": RangeValidator,
        "row_count": RowCountValidator,
    }

    def __init__(self):
        self.results: List[ValidationResult] = []

    def register_rule(
        self,
        rule_name: str,
        rule_class: Type[ValidationRule]
    ) -> None:
        """
        Allows future extensions without changing the engine.
        """
        self.RULE_REGISTRY[ ] = rule_class

    def get_validator(self, rule_name: str) -> ValidationRule:
        rule_class = self.RULE_REGISTRY.get(rule_name)

        if rule_class is None:
            raise ValueError(
                f"Unsupported validation rule: {rule_name}. "
                f"Available rules: {list(self.RULE_REGISTRY.keys())}"
            )

        return rule_class()

    def validate(
        self,
        df: DataFrame,
        validation_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Main entry point.

        Example:
            validator.validate(df, config["validation"])
        """

        self.results = []

        rules = validation_config.get("rules", [])

        for rule_config in rules:

            rule_name = rule_config.get("name")

            if not rule_name:
                self.results.append(
                    ValidationResult(
                        rule_name="UNKNOWN",
                        status="FAIL",
                        severity="ERROR",
                        message="Validation rule does not contain a name."
                    )
                )
                continue

            validator = self.get_validator(rule_name)

            result = validator.validate(
                df,
                rule_config
            )

            self.results.append(result)

        return self._build_summary()

    def _build_summary(self) -> Dict[str, Any]:

        failed = [
            result
            for result in self.results
            if result.status == "FAIL"
        ]

        warnings = [
            result
            for result in self.results
            if result.status == "WARN"
        ]

        overall_status = "PASS"

        if any(
            result.status == "FAIL"
            and result.severity in ("ERROR", "FATAL")
            for result in self.results
        ):
            overall_status = "FAIL"

        elif warnings:
            overall_status = "WARN"

        return {
            "overall_status": overall_status,
            "total_rules": len(self.results),
            "passed_rules": sum(
                result.status == "PASS"
                for result in self.results
            ),
            "failed_rules": len(failed),
            "warning_rules": len(warnings),
            "results": [
                result.to_dict()
                for result in self.results
            ]
        }