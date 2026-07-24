import json

import anthropic

from config import settings
from src.models import DQRule, DQRulesOutput, RunContext

_client = anthropic.Anthropic()

_DQ_SCHEMA = json.dumps(DQRulesOutput.model_json_schema(), indent=2)


async def run(ctx: RunContext) -> RunContext:
    schema_summary = _format_schema(ctx)
    contract_context = _format_contract(ctx)

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=2048,
        system=f"""You are a data quality expert. Generate DQ rules for a dataset based on its schema and the data contract filled by the data owner.
Return ONLY valid JSON matching this exact schema (no markdown):
{_DQ_SCHEMA}

Rule types to consider:
- "not_null": required column — expression: "value IS NOT NULL"
- "unique": unique constraint — expression: "COUNT(DISTINCT value) = COUNT(value)"
- "range": numeric bounds — expression: "value BETWEEN X AND Y"
- "regex": format validation — expression: "value REGEXP 'pattern'"
- "referential": valid enum values — expression: "value IN ('a', 'b', 'c')"

Severity:
- "error": data cannot be loaded to Silver if this fails
- "warning": data quality issue, row still loads but gets flagged

Guidelines from the data contract (respect these over your own inference):
- If error tolerance is "Zero erros", mark all rules as "error" severity.
- If error tolerance is "Até 1%" or "Até 5%", use "warning" for borderline rules.
- If the file has a footer row (tem_rodape=Sim), add a rule to flag it.

Generate at least one rule per non-nullable column.""",
        messages=[
            {
                "role": "user",
                "content": f"Generate DQ rules for this schema:\n{schema_summary}\n\n{contract_context}",
            }
        ],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(raw)
    ctx.dq_rules = DQRulesOutput(rules=[DQRule(**r) for r in parsed["rules"]])
    return ctx


def _format_schema(ctx: RunContext) -> str:
    if not ctx.schema:
        return "No schema available"
    lines = [f"Table: {ctx.schema.table_name}", "Columns:"]
    for col in ctx.schema.columns:
        flags = []
        if col.is_pk:
            flags.append("PK")
        if not col.nullable:
            flags.append("NOT NULL")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        lines.append(f"  - {col.name}: {col.type}{flag_str}")
    return "\n".join(lines)


def _format_contract(ctx: RunContext) -> str:
    if not ctx.contract_meta:
        return ""
    m = ctx.contract_meta
    lines = ["Data contract (filled by the data owner):"]
    if m.get("tolerancia_pct"):
        lines.append(f"  - Error tolerance: {m['tolerancia_pct']}")
    if m.get("on_invalid"):
        lines.append(f"  - On invalid records: {m['on_invalid']}")
    if m.get("tem_rodape"):
        lines.append(f"  - Has footer row: {m['tem_rodape']}")
    if m.get("colunas_sensiveis"):
        lines.append(f"  - Sensitive columns declared by owner: {m['colunas_sensiveis']}")
    return "\n".join(lines) if len(lines) > 1 else ""
