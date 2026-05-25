from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Agent output models — validated from Claude API JSON responses
# ---------------------------------------------------------------------------

class DataDictColumn(BaseModel):
    name: str
    description: str
    example: str


class DataDictOutput(BaseModel):
    tema: str
    table_name: str
    description: str
    columns: list[DataDictColumn]


class ColumnSchema(BaseModel):
    name: str
    type: str       # "string" | "integer" | "float" | "boolean" | "date" | "timestamp"
    nullable: bool
    is_pk: bool


class SchemaOutput(BaseModel):
    table_name: str
    columns: list[ColumnSchema]


class DQRule(BaseModel):
    column: str
    rule_type: str  # "not_null" | "unique" | "range" | "regex" | "referential"
    expression: str # e.g. "value IS NOT NULL", "value BETWEEN 0 AND 100"
    severity: str   # "error" | "warning"


class DQRulesOutput(BaseModel):
    rules: list[DQRule]


class ColumnGovernance(BaseModel):
    name: str
    pii: bool
    sensitivity: str        # "public" | "internal" | "confidential" | "restricted"
    owner: str              # business area inferred from column semantics
    masking_strategy: str   # "none" | "hash" | "mask" | "encrypt"


class GovernanceOutput(BaseModel):
    columns: list[ColumnGovernance]


class PipelineOutput(BaseModel):
    bronze_notebook: str
    silver_notebook: str


class ReporterOutput(BaseModel):
    message: str


# ---------------------------------------------------------------------------
# Shared run state — passed through all phases
# ---------------------------------------------------------------------------

@dataclass
class RunContext:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    tema: str = ""
    csv_content: str = ""
    data_dict: DataDictOutput | None = None
    schema: SchemaOutput | None = None
    dq_rules: DQRulesOutput | None = None
    governance: GovernanceOutput | None = None
    bronze_notebook: str = ""
    silver_notebook: str = ""
    pr_url: str = ""
    table_bronze: str = ""
    table_silver: str = ""
    reporter_message: str = ""
    slack_sent: bool = False
