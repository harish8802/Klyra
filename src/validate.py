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

        for rules in job_config['validation']:
            validator = self.get_validator(rules)
            result = validator.validate(spark, df, job_config['validation'][rules])
            results.append(result)

        if results:
            for res in results:
                if res.status == "FAIL":
                    raise ValueError(f"Validation failed for rule: {res.rule_name}, Status: {res.status}, Message: {res.message}")
        else:
            logger.info("All validations passed successfully.")

        return results