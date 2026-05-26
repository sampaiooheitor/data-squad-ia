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
        system="""You are a data engineering expert. Return ONLY valid JSON with no markdown fences.
The JSON must have exactly these top-level fields:
- tema: string (the business area)
- table_name: string (UPPERCASE, format TB + ORIGIN_CODE + _ + SUBJECT, e.g. TBCIAR_CLIENTES)
- description: string (one sentence describing the table in Portuguese)
- columns: list of objects with exactly these fields:
    - origem: string (short source system code, uppercase, e.g. "SEC", "CRM", "ERP")
    - tabela: string (same as table_name above, repeated for every column)
    - campo: string (column name in UPPERCASE with prefix, e.g. NM_CLIENTE, VL_TRANSACAO, DT_NASCIMENTO, CD_PRODUTO, ID_PEDIDO, FL_ATIVO)
    - datatype: string (SQL type: STRING, INTEGER, DECIMAL(15,2), DATE, TIMESTAMP, BOOLEAN)
    - format_data: string (only for DATE/TIMESTAMP: "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS", else empty string)
    - descricao: string (column description in Portuguese)
    - indicador_sensivel: string ("S" if PII/sensitive, "N" otherwise)
- csv_data: string (full CSV with header row using the campo names, and the requested number of rows)

Rules:
- Include 5-8 columns
- At least one column with indicador_sensivel = "S" (CPF, email, nome, salario)
- All data must be fictional — no real CPFs, no real names
- CPF format: XXX.XXX.XXX-XX
- CSV separator: pipe (|) to avoid conflicts with commas in values
- CSV must not have line breaks inside field values""",
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
