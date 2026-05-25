import json
import random

import anthropic

from config import settings, yaml_config
from src.models import DataDictColumn, DataDictOutput, RunContext

_client = anthropic.Anthropic()

_TEMAS: list[str] = yaml_config.get("data_generator", {}).get(
    "temas", ["rh", "financeiro", "ecommerce", "logistica", "marketing"]
)
_MIN_ROWS: int = yaml_config.get("data_generator", {}).get("min_rows", 50)
_MAX_ROWS: int = yaml_config.get("data_generator", {}).get("max_rows", 200)


async def run(ctx: RunContext) -> RunContext:
    ctx.tema = random.choice(_TEMAS)
    num_rows = random.randint(_MIN_ROWS, min(_MAX_ROWS, 50))

    response = _client.messages.create(
        model=settings.claude_model,
        max_tokens=16000,
        system="""You are a data generation expert. Return ONLY valid JSON with no markdown fences.
The JSON must have exactly these top-level fields:
- tema: string
- table_name: string (snake_case, related to the tema)
- description: string (one sentence describing the table)
- columns: list of objects with {name, description, example}
- csv_data: string (full CSV with header row and the requested number of rows, using comma separator)

Rules:
- Include 5-8 columns relevant to the business area
- Include at least one PII-like column (email, cpf, phone, nome_completo)
- All data must be fictional — no real names, no real CPFs
- CPFs: use format XXX.XXX.XXX-XX with fictional numbers
- CSV must not contain commas inside field values (use semicolons if needed inside text)""",
        messages=[
            {
                "role": "user",
                "content": (
                    f"Generate a realistic fictional {ctx.tema} dataset with exactly {num_rows} data rows "
                    f"(plus one header row). Return only JSON."
                ),
            }
        ],
    )

    raw = response.content[0].text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    parsed = json.loads(raw)

    ctx.csv_content = parsed.pop("csv_data")
    ctx.data_dict = DataDictOutput(
        tema=parsed["tema"],
        table_name=parsed["table_name"],
        description=parsed["description"],
        columns=[DataDictColumn(**c) for c in parsed["columns"]],
    )

    return ctx
