import anthropic

from config import settings
from src.models import RunContext

_client = anthropic.Anthropic()


async def run(ctx: RunContext) -> RunContext:
    col_count = len(ctx.schema.columns) if ctx.schema else 0
    pii_cols = (
        [c.name for c in ctx.governance.columns if c.pii] if ctx.governance else []
    )
    dq_rule_count = len(ctx.dq_rules.rules) if ctx.dq_rules else 0
    row_count = _count_rows(ctx.csv_content)

    summary = f"""Run ID: {ctx.run_id}
Tema: {ctx.tema}
Table: {ctx.schema.table_name if ctx.schema else "unknown"}
Rows generated: {row_count}
Columns: {col_count}
DQ rules: {dq_rule_count}
PII columns: {', '.join(pii_cols) if pii_cols else 'none'}
Bronze table: {ctx.table_bronze}
Silver table: {ctx.table_silver}
PR: {ctx.pr_url if ctx.pr_url else 'pending'}"""

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=512,
        system="""You are a data engineering assistant writing Slack notifications.
Write a concise, friendly Slack message summarizing a completed data pipeline run.
Use Slack markdown: *bold*, `code`, bullet points with •.
Return ONLY the message text, no JSON wrapper, no explanation.""",
        messages=[{"role": "user", "content": f"Write a Slack summary for:\n{summary}"}],
    )

    ctx.reporter_message = response.content[0].text
    return ctx


def _count_rows(csv: str) -> int:
    lines = [line for line in csv.strip().splitlines() if line.strip()]
    return max(0, len(lines) - 1)
