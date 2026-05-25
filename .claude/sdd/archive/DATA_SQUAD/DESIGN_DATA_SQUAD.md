# DESIGN: Data Squad — Engenheiro de Dados Autônomo

> Arquitetura técnica para um sistema multi-agente Python que automatiza o ciclo completo de ingestão de dados: geração → Google Forms → schema → DQ → governance → pipeline → Databricks + GitHub + Slack.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_SQUAD |
| **Date** | 2026-05-20 |
| **Author** | Heitor Sampaio |
| **DEFINE** | [DEFINE_DATA_SQUAD.md](./DEFINE_DATA_SQUAD.md) |
| **Status** | ✅ Shipped |

---

## Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SQUAD — ORQUESTRADOR                          │
│                         python -m src.orchestrator                          │
└───────────────────────────────────┬─────────────────────────────────────────┘
                                    │
              ┌─────────────────────▼─────────────────────┐
              │           FASE 1 — SEQUENCIAL              │
              │                                            │
              │  ┌─────────────────────────────────────┐  │
              │  │      Data Generator Agent           │  │
              │  │  Claude API → CSV + data_dict.json  │  │
              │  └───────────────┬─────────────────────┘  │
              │                  │ CSV in-memory           │
              │  ┌───────────────▼─────────────────────┐  │
              │  │      Playwright Runner (Tool)        │  │
              │  │  Browser → Google Form → Sheets      │  │
              │  └─────────────────────────────────────┘  │
              └─────────────────────┬─────────────────────┘
                                    │ lê Google Sheets API
              ┌─────────────────────▼─────────────────────┐
              │           FASE 2 — SEQUENCIAL              │
              │                                            │
              │  ┌─────────────────────────────────────┐  │
              │  │         Schema Agent                │  │
              │  │  Claude API → schema.json           │  │
              │  └─────────────────────────────────────┘  │
              └─────────────────────┬─────────────────────┘
                                    │ schema.json
              ┌─────────────────────▼─────────────────────┐
              │           FASE 3 — PARALELO                │
              │                                            │
              │  ┌──────────────┐  ┌─────────────────────┐│
              │  │  DQ Agent    │  │  Governance Agent   ││
              │  │  → dq_rules  │  │  → governance_meta  ││
              │  └──────┬───────┘  └──────────┬──────────┘│
              └─────────┼────────────────────┼────────────┘
                        │                    │
              ┌─────────▼────────────────────▼────────────┐
              │           FASE 4 — SEQUENCIAL              │
              │                                            │
              │  ┌─────────────────────────────────────┐  │
              │  │        Pipeline Agent               │  │
              │  │  Claude API →                       │  │
              │  │  bronze_ingest.py                   │  │
              │  │  silver_transform.py                │  │
              │  └─────────────────────────────────────┘  │
              └─────────────────────┬─────────────────────┘
                                    │ notebooks gerados
              ┌─────────────────────▼─────────────────────┐
              │           FASE 5 — PARALELO                │
              │                                            │
              │  ┌──────────────────────┐  ┌────────────┐ │
              │  │    GitHub Agent      │  │  Reporter  │ │
              │  │  → PR com artefatos  │  │  Agent     │ │
              │  │  → Databricks Client │  │  → Slack   │ │
              │  │    import + execute  │  └────────────┘ │
              │  │    → Delta Tables ✅ │                  │
              │  └──────────────────────┘                  │
              └─────────────────────────────────────────────┘

Artefatos de estado (passados entre fases via RunContext):
  run_id, tema, csv_path, schema.json, dq_rules.json,
  governance_metadata.json, bronze_ingest.py, silver_transform.py,
  pr_url, table_bronze, table_silver
```

---

## Components

| Component | Arquivo | Propósito | Tecnologia |
|-----------|---------|-----------|------------|
| Orchestrator | `src/orchestrator.py` | Coordena todas as fases, mantém RunContext | Python + asyncio |
| RunContext | `src/models.py` | Dataclass com estado compartilhado entre agentes | Python dataclass |
| Data Generator Agent | `src/agents/data_generator.py` | Gera CSV fictício + dicionário de dados via Claude API | Claude API (claude-sonnet-4-6) |
| Schema Agent | `src/agents/schema_agent.py` | Infere schema (tipos, nullability, PKs) via Claude API | Claude API (claude-sonnet-4-6) |
| DQ Agent | `src/agents/dq_agent.py` | Gera regras de qualidade de dados via Claude API | Claude API (claude-sonnet-4-6) |
| Governance Agent | `src/agents/governance_agent.py` | Classifica PII e sensibilidade via Claude API | Claude API (claude-sonnet-4-6) |
| Pipeline Agent | `src/agents/pipeline_agent.py` | Gera notebooks bronze_ingest.py e silver_transform.py via Claude API | Claude API (claude-sonnet-4-6) |
| Reporter Agent | `src/agents/reporter_agent.py` | Gera mensagem de resumo para Slack via Claude API | Claude API (claude-sonnet-4-6) |
| Playwright Runner | `src/tools/playwright_runner.py` | Submete CSV ao Google Form via browser automation | playwright-python |
| Sheets Reader | `src/tools/sheets_reader.py` | Lê resposta do Google Sheets via API | google-api-python-client |
| GitHub PR Tool | `src/tools/github_pr.py` | Cria branch, commit e PR com artefatos | PyGithub |
| Databricks Client | `src/tools/databricks_client.py` | Import de notebooks + execução via Commands API | requests (HTTP direto) |
| Slack Notifier | `src/tools/slack_notifier.py` | Envia notificação via webhook | requests |
| Config | `config/settings.yaml` | Todas as configurações externalizadas | YAML + pydantic-settings |

---

## Key Decisions

### Decision 1: RunContext como dataclass mutável passado por referência

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-05-20 |

**Context:** O orquestrador precisa passar estado acumulado entre fases (CSV gerado → schema → regras DQ → notebooks). As fases paralelas precisam escrever no mesmo estado sem conflito.

**Choice:** `RunContext` é uma dataclass Python com todos os campos opcionais. Cada agente recebe e retorna o contexto atualizado. Fases paralelas escrevem campos disjuntos (DQ e Governance escrevem campos diferentes).

**Rationale:** Simples, tipado, sem dependência de framework. Campos opcionais evitam bugs de inicialização. A separação de campos garante que o paralelismo não crie race conditions.

**Alternatives Rejected:**
1. Dict global — sem type hints, erros de chave em runtime
2. Redis/banco para estado — overhead desnecessário para um processo single-machine
3. Return values encadeados — perde legibilidade com muitos campos

**Consequences:**
- Estado explícito e rastreável a cada fase
- Paralelismo requer que as fases escrevam apenas nos seus próprios campos

---

### Decision 2: Claude API com tool_use para agentes que chamam ferramentas externas

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-05-20 |

**Context:** Alguns agentes apenas geram texto/JSON estruturado (Data Generator, Schema, DQ, Governance, Pipeline, Reporter). Nenhum deles precisa de autonomia de tool_use — a saída é sempre estruturada e consumida pelo orquestrador.

**Choice:** Todos os agentes usam `messages` API simples com `response_format` estruturado via system prompt. Sem tool_use. O orquestrador chama as ferramentas externas diretamente após receber a saída do agente.

**Rationale:** O demo precisa ser determinístico. O orquestrador controla explicitamente quando chamar Playwright, GitHub, Databricks. Claude não decide isso — o código decide. Isso torna o fluxo previsível e rastreável.

**Alternatives Rejected:**
1. Tool use por agente — não-determinístico, Claude pode decidir não chamar a ferramenta ou chamá-la com parâmetros inesperados
2. Managed Agents — mesma razão, comportamento emergente indesejável no demo

**Consequences:**
- Agentes são mais previsíveis e fáceis de testar isoladamente
- Orquestrador é mais verboso mas totalmente controlado

---

### Decision 3: Databricks via HTTP direto (requests), sem SDK oficial

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-05-20 |

**Context:** Free Edition não tem suporte ao Databricks SDK completo. A execução de notebooks precisa do endpoint legado `/api/1.2/commands/execute`.

**Choice:** Todas as chamadas Databricks via `requests` com autenticação Bearer Token. Dois endpoints: Workspace API (`/api/2.0/workspace/import`) para import e Commands API (`/api/1.2/commands/execute`) para execução.

**Rationale:** O SDK oficial não suporta Commands API 1.2. HTTP direto com requests é explícito, sem surpresas de compatibilidade, e funciona no Free Edition.

**Alternatives Rejected:**
1. databricks-sdk — não suporta `/api/1.2/commands/execute`
2. databricks-cli — não adequado para uso programático em Python

**Consequences:**
- Sem abstração de retry/auth do SDK (implementar manualmente se necessário)
- Código explícito e portável para qualquer versão do Databricks

---

### Decision 4: asyncio.gather para fases paralelas, ThreadPoolExecutor para chamadas síncronas externas

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-05-20 |

**Context:** DQ Agent + Governance Agent rodam em paralelo. GitHub Agent + Reporter Agent rodam em paralelo. Claude API e as ferramentas externas são I/O bound.

**Choice:** O orquestrador usa `asyncio` com `asyncio.gather` para paralelismo. Chamadas síncronas (Playwright, Google Sheets, GitHub, Databricks) são executadas em `ThreadPoolExecutor` via `loop.run_in_executor`.

**Rationale:** `asyncio` é idiomático para I/O bound em Python moderno. Playwright tem bindings assíncronos nativos. Chamadas HTTP (GitHub, Slack, Databricks) ficam no executor para não bloquear o event loop.

**Alternatives Rejected:**
1. multiprocessing — overhead de processo para I/O bound não justificado
2. threading puro — menos elegante, sem async/await
3. sequential para tudo — desperdiça tempo no passo mais longo do fluxo (DQ+Governance = ~30s paralelo vs ~60s sequencial)

**Consequences:**
- Código do orquestrador usa async/await em toda a cadeia
- Todos os agentes devem ter interface assíncrona

---

### Decision 5: Saída de cada agente é JSON validado por Pydantic

| Attribute | Value |
|-----------|-------|
| **Status** | Accepted |
| **Date** | 2026-05-20 |

**Context:** Claude API pode retornar JSON malformado ou com campos faltando se o prompt não forçar o schema.

**Choice:** Cada agente define um modelo Pydantic para sua saída. O system prompt inclui o JSON Schema do modelo. A saída é validada com `model.model_validate_json(response)` antes de ser salva no RunContext.

**Rationale:** Falha rápida e mensagem de erro clara se o LLM alucinar um campo. O JSON Schema no prompt aumenta a conformidade da saída.

**Alternatives Rejected:**
1. Parse manual com `json.loads` — sem validação de campos obrigatórios
2. TypedDict — sem validação em runtime

**Consequences:**
- Cada agente tem seu modelo Pydantic em `src/models.py`
- Falha na validação termina o run com mensagem clara

---

## File Manifest

| # | Arquivo | Action | Propósito | Dependências |
|---|---------|--------|-----------|--------------|
| 1 | `src/__init__.py` | Create | Torna `src` um pacote Python | — |
| 2 | `src/models.py` | Create | RunContext + modelos Pydantic de saída de todos os agentes | — |
| 3 | `src/orchestrator.py` | Create | Ponto de entrada; coordena todas as 5 fases com asyncio | 2, todos os agentes e tools |
| 4 | `src/agents/__init__.py` | Create | Pacote agents | — |
| 5 | `src/agents/data_generator.py` | Create | Gera CSV fictício + dicionário via Claude API | 2 |
| 6 | `src/agents/schema_agent.py` | Create | Infere schema a partir do CSV | 2 |
| 7 | `src/agents/dq_agent.py` | Create | Gera regras de DQ a partir do schema | 2 |
| 8 | `src/agents/governance_agent.py` | Create | Classifica PII/sensibilidade por coluna | 2 |
| 9 | `src/agents/pipeline_agent.py` | Create | Gera bronze_ingest.py e silver_transform.py | 2 |
| 10 | `src/agents/reporter_agent.py` | Create | Gera mensagem Slack com resumo do run | 2 |
| 11 | `src/tools/__init__.py` | Create | Pacote tools | — |
| 12 | `src/tools/playwright_runner.py` | Create | Submete CSV ao Google Form via Playwright | — |
| 13 | `src/tools/sheets_reader.py` | Create | Lê resposta do Google Sheets | — |
| 14 | `src/tools/github_pr.py` | Create | Cria branch, commit e PR no GitHub | — |
| 15 | `src/tools/databricks_client.py` | Create | Import + execução de notebooks via API | — |
| 16 | `src/tools/slack_notifier.py` | Create | Envia notificação via Slack webhook | — |
| 17 | `config/settings.yaml` | Create | Configurações externalizadas (URLs, IDs, modelo) | — |
| 18 | `config/__init__.py` | Create | Carrega settings.yaml com pydantic-settings | 17 |
| 19 | `generated/` | Create (dir) | Artefatos gerados a cada run (CSV, JSONs, notebooks) | — |
| 20 | `pyproject.toml` | Create | Dependências do projeto | — |
| 21 | `.env.example` | Create | Template de variáveis de ambiente sensíveis | — |
| 22 | `README.md` | Create | Como rodar o demo | — |

**Total Files:** 22

---

## Agent Assignment Rationale

| Agent | Arquivos | Por quê |
|-------|----------|---------|
| @python-developer | 2, 3, 4–11, 18 | Dataclasses, asyncio, Claude API, padrões Python limpos |
| @ai-data-engineer | 5–10 | Prompts de extração estruturada, Pydantic output, LLM workflows |
| @lambda-builder | 12–16 | Integração com APIs externas, error handling, HTTP |
| (general) | 17, 19–22 | Config, setup, docs |

---

## Code Patterns

### Pattern 1: Estrutura base de um agente

```python
# src/agents/schema_agent.py
import json
import anthropic
from src.models import RunContext, SchemaOutput
from config import settings

client = anthropic.Anthropic()

async def run(ctx: RunContext) -> RunContext:
    system = f"""You are a data schema expert.
Analyze the CSV and return ONLY valid JSON matching this schema:
{SchemaOutput.model_json_schema()}"""

    response = client.messages.create(
        model=settings.claude_model,
        max_tokens=2048,
        system=system,
        messages=[{"role": "user", "content": ctx.csv_content}],
    )

    ctx.schema = SchemaOutput.model_validate_json(response.content[0].text)
    return ctx
```

### Pattern 2: RunContext — estado compartilhado entre fases

```python
# src/models.py
from dataclasses import dataclass, field
from typing import Optional
from pydantic import BaseModel

@dataclass
class RunContext:
    run_id: str
    tema: str = ""
    csv_content: str = ""
    data_dict: Optional[dict] = None
    schema: Optional["SchemaOutput"] = None
    dq_rules: Optional["DQRulesOutput"] = None
    governance: Optional["GovernanceOutput"] = None
    bronze_notebook: str = ""
    silver_notebook: str = ""
    pr_url: str = ""
    table_bronze: str = ""
    table_silver: str = ""
    slack_sent: bool = False
```

### Pattern 3: Paralelismo no orquestrador

```python
# src/orchestrator.py (trecho da fase 3)
import asyncio
from src.agents import dq_agent, governance_agent

async def fase_3_parallel(ctx: RunContext) -> RunContext:
    dq_ctx, gov_ctx = await asyncio.gather(
        dq_agent.run(RunContext(**vars(ctx))),
        governance_agent.run(RunContext(**vars(ctx))),
    )
    ctx.dq_rules = dq_ctx.dq_rules
    ctx.governance = gov_ctx.governance
    return ctx
```

### Pattern 4: Chamada síncrona no executor (Playwright, HTTP)

```python
# src/orchestrator.py (trecho para ferramentas síncronas)
import asyncio
from src.tools import playwright_runner

async def fase_1_playwright(ctx: RunContext) -> RunContext:
    loop = asyncio.get_event_loop()
    ctx = await loop.run_in_executor(
        None, playwright_runner.submit_form, ctx
    )
    return ctx
```

### Pattern 5: Databricks — import e execução de notebook

```python
# src/tools/databricks_client.py
import requests, base64, time

def import_notebook(path: str, content: str, token: str, host: str) -> None:
    encoded = base64.b64encode(content.encode()).decode()
    requests.post(
        f"{host}/api/2.0/workspace/import",
        headers={"Authorization": f"Bearer {token}"},
        json={"path": path, "content": encoded, "format": "SOURCE",
              "language": "PYTHON", "overwrite": True},
    ).raise_for_status()

def execute_notebook(cluster_id: str, notebook_path: str,
                     token: str, host: str) -> str:
    ctx_resp = requests.post(
        f"{host}/api/1.2/contexts/create",
        headers={"Authorization": f"Bearer {token}"},
        json={"clusterId": cluster_id, "language": "python"},
    ).json()
    context_id = ctx_resp["id"]

    cmd_resp = requests.post(
        f"{host}/api/1.2/commands/execute",
        headers={"Authorization": f"Bearer {token}"},
        json={"clusterId": cluster_id, "contextId": context_id,
              "language": "python",
              "command": f"%run {notebook_path}"},
    ).json()
    command_id = cmd_resp["id"]

    # poll until finished
    while True:
        status = requests.get(
            f"{host}/api/1.2/commands/status",
            headers={"Authorization": f"Bearer {token}"},
            params={"clusterId": cluster_id, "contextId": context_id,
                    "commandId": command_id},
        ).json()
        if status["status"] in ("Finished", "Error", "Cancelled"):
            return status["status"]
        time.sleep(3)
```

### Pattern 6: Configuração com pydantic-settings

```python
# config/__init__.py
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml, pathlib

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    claude_model: str = "claude-sonnet-4-6"
    google_form_url: str = ""
    google_spreadsheet_id: str = ""
    github_repo: str = ""
    github_token: str = ""
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_cluster_id: str = ""
    slack_webhook_url: str = ""

settings = Settings()
```

### Pattern 7: settings.yaml

```yaml
# config/settings.yaml
claude_model: "claude-sonnet-4-6"
google_form_url: "${GOOGLE_FORM_URL}"
google_spreadsheet_id: "${GOOGLE_SPREADSHEET_ID}"
github_repo: "${GITHUB_REPO}"
databricks_host: "${DATABRICKS_HOST}"
databricks_cluster_id: "${DATABRICKS_CLUSTER_ID}"

# Parâmetros dos agentes
data_generator:
  min_rows: 50
  max_rows: 500
  temas:
    - rh
    - financeiro
    - ecommerce
    - logistica
    - marketing

pipeline:
  bronze_table_prefix: "bronze"
  silver_table_prefix: "silver"
  databricks_notebook_base_path: "/data_squad"
```

---

## Data Flow

```text
1. Orchestrator cria RunContext(run_id=uuid4(), ...)
   │
   ▼
2. Data Generator Agent
   └── Claude API → CSV (50-500 linhas) + data_dict.json
   └── ctx.csv_content, ctx.data_dict, ctx.tema preenchidos
   │
   ▼
3. Playwright Runner (ThreadPoolExecutor)
   └── Abre browser → preenche form com CSV → submete
   └── Aguarda confirmação de envio
   │
   ▼
4. Sheets Reader (ThreadPoolExecutor)
   └── Google Sheets API → lê última resposta
   └── Confirma que ctx.csv_content foi recebido
   │
   ▼
5. Schema Agent
   └── Claude API + CSV → schema.json
   └── ctx.schema = {columns: [{name, type, nullable, is_pk}]}
   │
   ▼
6. asyncio.gather(DQ Agent, Governance Agent)
   ├── DQ Agent: Claude API + schema → dq_rules.json
   │   └── ctx.dq_rules = {rules: [{column, rule_type, expression}]}
   └── Governance Agent: Claude API + schema → governance_metadata.json
       └── ctx.governance = {columns: [{name, pii, sensitivity, owner}]}
   │
   ▼
7. Pipeline Agent
   └── Claude API + schema + dq_rules + governance → notebooks Python
   └── ctx.bronze_notebook = código completo bronze_ingest.py
   └── ctx.silver_notebook = código completo silver_transform.py
   │
   ▼
8. asyncio.gather(GitHub Agent flow, Reporter Agent)
   ├── GitHub Agent:
   │   ├── PyGithub → cria branch data-squad/{run_id}
   │   ├── Commit: artefatos JSON + notebooks Python
   │   ├── Abre PR → ctx.pr_url preenchido
   │   └── Databricks Client (em sequência dentro do GitHub Agent flow):
   │       ├── Workspace API → importa bronze_ingest.py + silver_transform.py
   │       ├── Commands API → executa bronze_ingest.py → ctx.table_bronze
   │       └── Commands API → executa silver_transform.py → ctx.table_silver
   └── Reporter Agent:
       └── Claude API + ctx → mensagem Slack formatada
       └── Slack Notifier → POST webhook → ctx.slack_sent = True
   │
   ▼
9. Orchestrator imprime resumo do run no console
```

---

## Integration Points

| Sistema Externo | Tipo | Autenticação | Endpoint Principal |
|-----------------|------|--------------|--------------------|
| Claude API (Anthropic) | REST API | `ANTHROPIC_API_KEY` env var | `messages.create()` via SDK |
| Google Forms | Browser Automation | Nenhuma (form público) | URL do form |
| Google Sheets | REST API (google-api-python-client) | Service Account JSON | `spreadsheets.values.get` |
| GitHub | REST API (PyGithub) | PAT via `GITHUB_TOKEN` | `repo.create_pull` |
| Databricks Workspace API | REST API (requests) | Bearer Token | `POST /api/2.0/workspace/import` |
| Databricks Commands API | REST API (requests) | Bearer Token | `POST /api/1.2/commands/execute` |
| Slack | Webhook HTTP POST | Webhook URL | `POST {SLACK_WEBHOOK_URL}` |

---

## Pipeline Architecture

### DAG de Execução

```text
[Data Generator] ──→ [Playwright] ──→ [Sheets Reader] ──→ [Schema Agent]
                                                                  │
                              ┌───────────────────────────────────┤
                              │                                   │
                         [DQ Agent]                    [Governance Agent]
                              │                                   │
                              └───────────────────────────────────┘
                                                  │
                                         [Pipeline Agent]
                                                  │
                              ┌───────────────────┴────────────────┐
                              │                                    │
                      [GitHub + Databricks]               [Reporter + Slack]
```

### Artefatos gerados por run

```
generated/
└── {run_id}/
    ├── dataset.csv
    ├── data_dict.json
    ├── schema.json
    ├── dq_rules.json
    ├── governance_metadata.json
    ├── bronze_ingest.py
    └── silver_transform.py
```

### Schema dos modelos Pydantic de output

```python
class ColumnSchema(BaseModel):
    name: str
    type: str          # "string" | "integer" | "float" | "boolean" | "date" | "timestamp"
    nullable: bool
    is_pk: bool

class SchemaOutput(BaseModel):
    table_name: str
    columns: list[ColumnSchema]

class DQRule(BaseModel):
    column: str
    rule_type: str     # "not_null" | "unique" | "range" | "regex" | "referential"
    expression: str    # ex: "value IS NOT NULL", "value BETWEEN 0 AND 100"
    severity: str      # "error" | "warning"

class DQRulesOutput(BaseModel):
    rules: list[DQRule]

class ColumnGovernance(BaseModel):
    name: str
    pii: bool
    sensitivity: str   # "public" | "internal" | "confidential" | "restricted"
    owner: str         # área da empresa (inferida pelo agente)
    masking_strategy: str  # "none" | "hash" | "mask" | "encrypt"

class GovernanceOutput(BaseModel):
    columns: list[ColumnGovernance]
```

### Data Quality Gates (Silver)

| Gate | Threshold | Ação em falha |
|------|-----------|---------------|
| NOT NULL em PKs | 0 nulos | Rejeitar linha, logar no Silver com `_dq_status = "rejected"` |
| Regras DQ aplicadas | Configurable por regra | Warning → linha marcada; Error → linha rejeitada |
| Contagem Silver ≤ Bronze | ≥ 90% das linhas aprovadas | Log de warning no run summary |

---

## Testing Strategy

| Tipo | Escopo | Arquivos | Ferramentas | Meta |
|------|--------|----------|-------------|------|
| Unit | Cada agente isolado (sem Claude API) | `tests/test_agents.py` | pytest + respx (mock HTTP) | Lógica de parsing e validação Pydantic |
| Unit | Tools (sem APIs externas) | `tests/test_tools.py` | pytest + unittest.mock | Import/execute Databricks, GitHub PR |
| Integration | Fluxo schema → DQ → pipeline com Claude API real | `tests/integration/test_pipeline.py` | pytest + API real | Happy path de agentes encadeados |
| E2E | Run completo via `orchestrator.py` | Manual no demo | — | Verifica tabelas no Databricks + PR + Slack |

---

## Error Handling

| Erro | Estratégia | Retry? |
|------|-----------|--------|
| Claude API timeout / 5xx | Raise com mensagem clara, aborta o run | Não (demo) |
| JSON inválido da Claude API (falha Pydantic) | Log do raw response + raise `AgentOutputError` | Não |
| Playwright falha ao encontrar campo do form | Raise `PlaywrightFormError` com screenshot | Não |
| Google Sheets sem resposta em 15s | Raise `SheetsTimeoutError` | Não |
| GitHub API 4xx | Raise com status code e body | Não |
| Databricks Commands API estado `Error` | Raise `DatabricksExecutionError` com result.resultType | Não |
| Slack webhook falha | Log warning + continua (artefatos já criados) | Não |

**Princípio:** Falha rápida com mensagem clara. O demo não tem retry — cada falha indica um problema de configuração que o usuário deve corrigir antes de re-rodar.

---

## Configuration

| Chave | Tipo | Origem | Descrição |
|-------|------|--------|-----------|
| `ANTHROPIC_API_KEY` | string | `.env` | API key da Anthropic |
| `GOOGLE_FORM_URL` | string | `.env` | URL do Google Form |
| `GOOGLE_SPREADSHEET_ID` | string | `.env` | ID da planilha de respostas |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | path | `.env` | Path para o JSON da service account |
| `GITHUB_TOKEN` | string | `.env` | Personal Access Token do GitHub |
| `GITHUB_REPO` | string | `.env` | `owner/repo` do repositório alvo |
| `DATABRICKS_HOST` | string | `.env` | URL do workspace (ex: `https://adb-xxx.azuredatabricks.net`) |
| `DATABRICKS_TOKEN` | string | `.env` | Personal Access Token do Databricks |
| `DATABRICKS_CLUSTER_ID` | string | `.env` | ID do cluster Free Edition |
| `SLACK_WEBHOOK_URL` | string | `.env` | Webhook URL do canal Slack |
| `claude_model` | string | `settings.yaml` | Modelo Claude a usar (default: `claude-sonnet-4-6`) |

---

## Security Considerations

- Todas as credenciais via variáveis de ambiente — nunca hardcoded ou commitadas
- `.env` no `.gitignore` desde o início; `.env.example` sem valores reais
- Service account do Google com permissão mínima (read-only no Sheets)
- GitHub PAT com escopo mínimo (`repo` apenas)
- Databricks Token com permissão de cluster e workspace apenas
- CSV gerado contém dados fictícios — nenhum dado real em nenhum artefato
- `generated/` no `.gitignore` — artefatos de run não são commitados

---

## Observability

| Aspecto | Implementação |
|---------|----------------|
| Logging | `logging` padrão Python com nível configurável; cada fase loga início, duração e resultado |
| Run summary | Orchestrator imprime tabela ASCII no final com fase, status e duração de cada etapa |
| Artefatos | Todos os JSONs e notebooks salvos em `generated/{run_id}/` para inspeção pós-run |
| Erros | Stack trace completo no stderr com identificação da fase e do agente |

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-05-20 | Heitor Sampaio | Initial version |

---

## Next Step

**Ready for:** `/build .claude/sdd/features/DESIGN_DATA_SQUAD.md`
