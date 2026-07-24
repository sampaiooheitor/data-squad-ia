import json

import anthropic

from config import settings
from src.models import ColumnGovernance, GovernanceOutput, RunContext

_client = anthropic.Anthropic()

_GOV_SCHEMA = json.dumps(GovernanceOutput.model_json_schema(), indent=2)


async def run(ctx: RunContext) -> RunContext:
    schema_summary = _format_schema(ctx)
    contract_context = _format_contract(ctx)

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=2048,
        system=f"""You are a data governance and privacy expert (LGPD/GDPR aware).
Classify each column of a dataset based on its name, type, and the data contract filled by the data owner.
Return ONLY valid JSON matching this exact schema (no markdown):
{_GOV_SCHEMA}

Classification rules:
- pii=true: columns containing personal data (name, email, CPF, phone, address, birth date, etc.)
- sensitivity:
  * "public": non-sensitive business data
  * "internal": internal use only
  * "confidential": financial, medical, or employment data
  * "restricted": PII, regulated data (CPF, CNPJ, credit card)
- masking_strategy:
  * "none": public or internal data
  * "hash": unique identifiers that need referential integrity (email used as FK)
  * "mask": show partial value (e.g., "***@domain.com", "XXX.XXX.XXX-XX")
  * "encrypt": highly sensitive fields (salary, medical, credit card)
- owner: use the business domain from the contract if provided; otherwise infer from column semantics

IMPORTANT: columns explicitly listed as sensitive by the data owner MUST have pii=true and a masking_strategy other than "none", regardless of their name.""",
        messages=[
            {
                "role": "user",
                "content": f"Classify governance for this schema:\n{schema_summary}\n\n{contract_context}",
            }
        ],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(raw)
    ctx.governance = GovernanceOutput(
        columns=[ColumnGovernance(**c) for c in parsed["columns"]]
    )
    return ctx


def _format_schema(ctx: RunContext) -> str:
    if not ctx.schema:
        return "No schema available"
    lines = [f"Table: {ctx.schema.table_name}", "Columns:"]
    for col in ctx.schema.columns:
        lines.append(f"  - {col.name}: {col.type} (nullable={col.nullable})")
    return "\n".join(lines)


def _format_contract(ctx: RunContext) -> str:
    if not ctx.contract_meta:
        return ""
    m = ctx.contract_meta
    lines = ["Data contract (filled by the data owner):"]
    if m.get("colunas_sensiveis"):
        lines.append(f"  - Columns declared as sensitive/PII by the owner: {m['colunas_sensiveis']}")
    if m.get("classificacao"):
        lines.append(f"  - Overall data classification: {m['classificacao']}")
    if m.get("dominio"):
        lines.append(f"  - Business domain: {m['dominio']}")
    if m.get("dono"):
        lines.append(f"  - Data owner: {m['dono']}")
    return "\n".join(lines) if len(lines) > 1 else ""
