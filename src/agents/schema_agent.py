import json

import anthropic

from config import settings
from src.models import ColumnSchema, RunContext, SchemaOutput

_client = anthropic.Anthropic()

_SCHEMA_JSON = json.dumps(SchemaOutput.model_json_schema(), indent=2)


async def run(ctx: RunContext) -> RunContext:
    table_name = ctx.data_dict.table_name if ctx.data_dict else "unknown_table"

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=2048,
        system=f"""You are a data schema inference expert. Analyze the CSV and infer the schema.
Return ONLY valid JSON matching this exact schema (no markdown):
{_SCHEMA_JSON}

Type mapping rules:
- Numeric integers → "integer"
- Numeric decimals → "float"
- ISO dates (YYYY-MM-DD) → "date"
- Timestamps → "timestamp"
- True/False/true/false/0/1 booleans → "boolean"
- Everything else → "string"

Nullability: if ANY value in the column is empty string or blank, nullable = true.
Primary key: column named "id" or ending in "_id" with all unique values → is_pk = true.""",
        messages=[
            {
                "role": "user",
                "content": (
                    f"Table name: {table_name}\n\n"
                    f"CSV data (first 20 rows shown):\n"
                    f"{_head(ctx.csv_content, 20)}"
                ),
            }
        ],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(raw)
    ctx.schema = SchemaOutput(
        table_name=parsed["table_name"],
        columns=[ColumnSchema(**c) for c in parsed["columns"]],
    )
    return ctx


def _head(csv: str, n: int) -> str:
    lines = csv.strip().splitlines()
    return "\n".join(lines[: n + 1])
