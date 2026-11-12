"""
CleanPipe - Resilient Schema Validator & ETL Pipeline
Author: Hashim (@hashim_py | https://hashim.me.uk)
"""
import re
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class FieldRule:
    name: str
    target_type: str  # 'int', 'float', 'str', 'datetime', 'email'
    required: bool = True
    default: Any = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None

@dataclass
class PipelineMetrics:
    total_records: int = 0
    passed_records: int = 0
    quarantined_records: int = 0
    coerced_fields: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)

class CleanPipe:
    EMAIL_REGEX = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

    def __init__(self, rules: List[FieldRule]):
        self.rules = {r.name: r for r in rules}
        self.metrics = PipelineMetrics()

    def transform_record(self, raw: Dict[str, Any]) -> Tuple[bool, Dict[str, Any], List[str]]:
        cleaned = {}
        issues = []
        
        for name, rule in self.rules.items():
            val = raw.get(name)
            
            # Check presence
            if val is None or val == "":
                if rule.required:
                    if rule.default is not None:
                        val = rule.default
                        self.metrics.coerced_fields += 1
                    else:
                        issues.append(f"Missing required field: '{name}'")
                        continue
                else:
                    cleaned[name] = rule.default
                    continue

            # Type Coercion & Validation
            try:
                if rule.target_type == "int":
                    if not isinstance(val, int):
                        val = int(float(str(val).replace("$", "").replace(",", "").strip()))
                        self.metrics.coerced_fields += 1
                    if rule.min_value is not None and val < rule.min_value:
                        issues.append(f"Field '{name}' below minimum: {val} < {rule.min_value}")
                    if rule.max_value is not None and val > rule.max_value:
                        issues.append(f"Field '{name}' above maximum: {val} > {rule.max_value}")

                elif rule.target_type == "float":
                    if not isinstance(val, float):
                        val = float(str(val).replace("$", "").replace(",", "").strip())
                        self.metrics.coerced_fields += 1
                    if rule.min_value is not None and val < rule.min_value:
                        issues.append(f"Field '{name}' below minimum: {val} < {rule.min_value}")

                elif rule.target_type == "str":
                    val = str(val).strip()

                elif rule.target_type == "email":
                    val = str(val).strip().lower()
                    if not self.EMAIL_REGEX.match(val):
                        issues.append(f"Invalid email syntax: '{val}'")

                elif rule.target_type == "datetime":
                    if not isinstance(val, datetime):
                        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%m/%d/%Y"):
                            try:
                                val = datetime.strptime(str(val).strip(), fmt)
                                self.metrics.coerced_fields += 1
                                break
                            except ValueError:
                                pass
                        if not isinstance(val, datetime):
                            issues.append(f"Unparseable date in '{name}': '{val}'")

                cleaned[name] = val

            except Exception as e:
                issues.append(f"Failed to coerce '{name}' ({val}): {str(e)}")

        self.metrics.total_records += 1
        if issues:
            self.metrics.quarantined_records += 1
            self.metrics.errors.append({"record": raw, "issues": issues})
            return False, cleaned, issues
        else:
            self.metrics.passed_records += 1
            return True, cleaned, []

    def process_batch(self, batch: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        clean_batch = []
        quarantine_batch = []
        for rec in batch:
            ok, clean_rec, issues = self.transform_record(rec)
            if ok:
                clean_batch.append(clean_rec)
            else:
                quarantine_batch.append({"raw": rec, "issues": issues})
        return clean_batch, quarantine_batch
