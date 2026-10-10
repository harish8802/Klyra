from dataclasses import dataclass, asdict
from functools import reduce
from operator import or_
from typing import Any, Dict, List
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    rule_name: str
    status: str
    severity: str
    message: str
    records_checked: int
    records_failed: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class NonNullableValidator:
    def validate(self, spark: Any, df: Any, rule_config: Dict[str, Any]) -> ValidationResult:
        total_records = df.count()
        columns = rule_config.get("columns", [])
        message = ""

        for column in columns:
            if column not in df.columns:
                raise ValueError(f"Column '{column}' specified in validation rule is not present in the DataFrame.")

            failed_column_records = df.filter(df[column].isNull()).count()
            if failed_column_records > 0:
                message = (
                    f"Column '{column}' has {failed_column_records} null values"
                    if not message
                    else f"{message}, Column '{column}' has {failed_column_records} null values"
                )

        failed_records = 0
        if columns:
            failed_records = df.filter(reduce(or_, [df[col].isNull() for col in columns])).count()

        status = "FAIL" if failed_records > 0 else "PASS"
        if not message:
            message = "All configured columns passed null validation."

        return ValidationResult(
            rule_name="non_nullable",
            status=status,
            severity=rule_config.get("severity", "ERROR"),
            message=message,
            records_checked=total_records,
            records_failed=failed_records,
        )


class ValidateEngine:
    rule_registry: Dict[str, Any] = {
        "non_nullable": NonNullableValidator
    }

    def get_validator(self, rule_name: str) -> Any:
        rule_class = self.rule_registry.get(rule_name)
        if not rule_class:
            raise ValueError(f"Validation rule '{rule_name}' is not registered.")
        return rule_class()

    def validate(self, spark: Any, df: Any, job_config: Dict[str, Any]) -> List[ValidationResult]:
        results: List[ValidationResult] = []
        validation_config = job_config.get("validation", {})
        logger.info("Starting validation with configured entries: %s", list(validation_config))

        for rule_name, rule_config in validation_config.items():
            if rule_name == "rules":
                logger.warning(
                    "Skipping validation config section 'rules'; it is not a registered validator."
                )
                continue

            validator = self.get_validator(rule_name)
            logger.info("Running validation rule '%s'", rule_name)
            try:
                result = validator.validate(spark, df, rule_config)
            except Exception:
                logger.exception("Validation rule '%s' raised an error", rule_name)
                raise

            results.append(result)
            logger.info(
                "Validation rule '%s' completed with status=%s; checked=%s failed=%s",
                result.rule_name,
                result.status,
                result.records_checked,
                result.records_failed,
            )

        for result in results:
            if result.status == "FAIL":
                message = (
                    f"Validation failed for rule: {result.rule_name}, "
                    f"Status: {result.status}, Message: {result.message}"
                )
                logger.error(message)
                raise ValueError(message)

        logger.info("All registered validations passed successfully.")

        return results