# Data Squad — Engenheiro de Dados Autônomo

Sistema multi-agente que automatiza 100% do ciclo de ingestão de dados: geração de dados fictícios → Google Forms → schema inference → DQ + Governance → notebooks Databricks → GitHub PR + Slack.

## Arquitetura

```
Data Generator → Playwright → Sheets Reader → Schema Agent
                                                    │
                                          DQ Agent ═╬═ Governance Agent
                                                    │
                                          Pipeline Agent
                                                    │
                                     GitHub PR ════╬════ Reporter + Slack
                                      Databricks ──┘
```

## Pré-requisitos

- Python 3.10+
- Conta Anthropic (API key)
- Google Form configurado com pelo menos um campo "Parágrafo"
- Google Service Account com acesso ao Sheets
- GitHub Personal Access Token (escopo `repo`)
- Databricks Free Edition com cluster ativo
- Slack Incoming Webhook

## Setup

```bash
# 1. Instalar dependências
pip install -e ".[dev]"
playwright install chromium

# 2. Configurar variáveis de ambiente
cp .env.example .env
# editar .env com suas credenciais

# 3. Adicionar service account do Google
# Baixe o JSON da service account e salve em config/service_account.json
```

## Execução

```bash
python -m src.orchestrator
```

## O que o sistema faz (em ordem)

| Fase | Agente/Tool | O que acontece |
|------|-------------|----------------|
| 1 | Data Generator | Claude API gera CSV fictício + dicionário de dados com tema aleatório |
| 1b | Playwright Runner | Browser automation submete CSV ao Google Form real |
| 1c | Sheets Reader | Confirma que a resposta chegou no Google Sheets |
| 2 | Schema Agent | Claude API infere tipos, nullability e PKs do CSV |
| 3 | DQ Agent + Governance Agent | Em paralelo: regras de DQ + classificação PII |
| 4 | Pipeline Agent | Claude API gera `bronze_ingest.py` e `silver_transform.py` |
| 5a | GitHub Agent | Cria branch, commita artefatos e abre PR |
| 5b | Databricks Client | Importa e executa notebooks → Delta Tables criadas |
| 5c | Reporter + Slack | Gera mensagem e notifica o canal Slack |

## Estrutura do projeto

```
src/
├── orchestrator.py          # Ponto de entrada
├── models.py                # RunContext + modelos Pydantic
├── agents/
│   ├── data_generator.py
│   ├── schema_agent.py
│   ├── dq_agent.py
│   ├── governance_agent.py
│   ├── pipeline_agent.py
│   └── reporter_agent.py
└── tools/
    ├── playwright_runner.py
    ├── sheets_reader.py
    ├── github_pr.py
    ├── databricks_client.py
    └── slack_notifier.py

config/
├── settings.yaml            # Configurações não-sensíveis
└── __init__.py              # pydantic-settings

generated/                   # Artefatos por run (gitignored)
└── {run_id}/
    ├── dataset.csv
    ├── data_dict.json
    ├── schema.json
    ├── dq_rules.json
    ├── governance_metadata.json
    ├── bronze_ingest.py
    └── silver_transform.py
```

## Configuração do Google Form

O form precisa ter pelo menos um campo do tipo **Parágrafo** (long answer).
O Playwright preenche os campos na ordem em que aparecem:
1. **Campo 1** → CSV data
2. **Campo 2** (opcional) → data dictionary JSON
