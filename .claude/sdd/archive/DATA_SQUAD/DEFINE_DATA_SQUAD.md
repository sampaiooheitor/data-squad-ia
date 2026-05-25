# DEFINE: Data Squad — Engenheiro de Dados Autônomo

> Sistema multi-agente que automatiza 100% do ciclo de ingestão de dados, desde a geração de dados fictícios até a criação de tabelas Delta no Databricks, com PR no GitHub e notificação no Slack.

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_SQUAD |
| **Date** | 2026-05-20 |
| **Author** | Heitor Sampaio |
| **Status** | ✅ Shipped |
| **Clarity Score** | 15/15 |
| **Source** | BRAINSTORM_DATA_SQUAD.md |

---

## Problem Statement

Times de dados perdem tempo com tarefas repetitivas de ingestão — schema inference, regras de qualidade, classificação de PII, geração de código de pipeline e abertura de PRs são feitos manualmente a cada novo dataset. O Data Squad automatiza 100% desse ciclo: da solicitação via Google Forms até a tabela Delta pronta e visível no Databricks, sem nenhuma intervenção humana.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| AI Engineer (demo persona) | Demonstrar capacidade de orquestração multi-agente com Claude API | Precisa de um demo end-to-end previsível, visualmente convincente e rastreável passo a passo |
| Time de dados (pós-MVP) | Solicitar ingestão de novos datasets | Abre PR manualmente, escreve código de pipeline, infere schema e regras de DQ do zero a cada solicitação |

---

## Goals

O que sucesso parece (priorizado):

| Priority | Goal |
|----------|------|
| **MUST** | Fluxo 100% autônomo — nenhuma intervenção humana do início ao fim |
| **MUST** | Data Generator Agent cria CSV fictício realista com tema dinâmico e dicionário de dados |
| **MUST** | Playwright submete os dados ao Google Form real sem intervenção |
| **MUST** | Schema Agent infere schema correto (tipos, nullability, PKs) a partir do CSV gerado |
| **MUST** | DQ Agent e Governance Agent rodam em paralelo e entregam regras e metadados |
| **MUST** | Pipeline Agent gera `bronze_ingest.py` e `silver_transform.py` com DQ e governance inline |
| **MUST** | GitHub Agent abre PR automaticamente com todos os artefatos |
| **MUST** | Databricks Client importa e executa os notebooks — Bronze e Silver Delta Tables visíveis no workspace |
| **MUST** | Slack recebe notificação com resumo do run e links para as tabelas |
| **SHOULD** | Paralelismo entre DQ Agent e Governance Agent (reduz tempo total de execução) |
| **SHOULD** | Paralelismo entre GitHub Agent e Reporter Agent na fase final |
| **COULD** | Tema do dataset varia por área da empresa (RH, financeiro, e-commerce, logística, etc.) |

---

## Success Criteria

Outcomes mensuráveis (com condição de teste clara):

- [ ] Data Generator Agent produz um CSV válido com 50–500 linhas e um dicionário de dados JSON a cada execução
- [ ] Playwright submete o formulário ao Google Form real e a resposta aparece no Google Sheets em até 10 segundos
- [ ] Orchestrator lê a resposta do Google Sheets via Sheets API sem erro
- [ ] Schema Agent retorna `schema.json` com tipo, nullability e chave primária para cada coluna
- [ ] DQ Agent retorna `dq_rules.json` com pelo menos uma regra por coluna não-nula
- [ ] Governance Agent retorna `governance_metadata.json` com classificação PII para cada coluna
- [ ] Pipeline Agent gera `bronze_ingest.py` e `silver_transform.py` que passam em linting Python (ruff)
- [ ] GitHub Agent abre PR com título descritivo contendo os dois notebooks e os artefatos JSON
- [ ] Databricks Client importa notebooks via Workspace API sem erro 4xx/5xx
- [ ] Databricks Client executa os notebooks via Commands API (`/api/1.2/commands/execute`) e ambos retornam estado `Finished`
- [ ] Bronze Delta Table e Silver Delta Table visíveis no Databricks Unity Catalog / workspace após a execução
- [ ] Slack recebe mensagem com: tema do dataset, nome das tabelas criadas, link para o PR e link para o workspace Databricks
- [ ] Fluxo completo (do trigger ao Slack) conclui sem intervenção humana

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Happy path — fluxo completo | Credenciais configuradas (Google, GitHub, Slack, Databricks) e Google Form disponível | Orchestrator é executado (`python -m src.orchestrator`) | Bronze Table e Silver Table criadas no Databricks, PR aberto no GitHub, Slack notificado — sem erro em nenhuma fase |
| AT-002 | Data Generator — variação de tema | Nenhum tema pré-configurado | Data Generator Agent é chamado 3 vezes consecutivas | Cada execução produz CSV com tema diferente (área da empresa distinta) |
| AT-003 | Governance — detecção de PII | CSV contém colunas `email`, `cpf`, `salary` | Governance Agent processa o schema | `governance_metadata.json` classifica `email` e `cpf` como PII e `salary` como sensível |
| AT-004 | Pipeline Agent — DQ inline no Silver | `schema.json` e `dq_rules.json` disponíveis | Pipeline Agent gera `silver_transform.py` | Código gerado contém verificações de qualidade correspondentes às regras do `dq_rules.json` |
| AT-005 | Databricks — tabelas visíveis | Notebooks importados com sucesso via Workspace API | Commands API executa `bronze_ingest.py` e `silver_transform.py` | Ambas as tabelas Delta existem e têm contagem de linhas > 0 no Databricks |
| AT-006 | Falha isolada não quebra o orquestrador | Slack webhook URL inválida | Orchestrator chega à fase final | Reporter Agent falha com mensagem clara, mas os demais artefatos (PR, tabelas) já foram criados |

---

## Out of Scope

Explicitamente **NÃO** incluído nesta versão:

- Camada Gold (sem agregações no MVP)
- Human-in-the-loop — aprovações manuais antes de qualquer fase
- Dashboard web de monitoramento de status
- Autenticação de usuários / multi-tenancy
- Retry automático com exponential backoff
- Memória persistente entre execuções (vector DB)
- Execução agendada via cron
- Suporte a múltiplos workspaces Databricks
- Testes unitários automatizados (fora do escopo do demo)

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Databricks Free Edition — sem Jobs API | Usar Workspace API para import + `/api/1.2/commands/execute` para execução |
| Technical | Google Forms não tem API oficial de submissão | Playwright faz automação de browser para submeter o formulário |
| Technical | Python é a única linguagem do projeto | Sem scripts shell, sem notebooks como orquestrador |
| Demo | Fluxo deve ser determinístico e previsível | Sem frameworks de agente não-controlados (sem CrewAI, LangGraph) |
| Demo | Cada execução deve ser independente | Sem estado persistido entre runs |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `src/agents/`, `src/tools/`, `src/orchestrator.py` | Separação clara entre lógica de agente (LLM) e ferramentas externas |
| **KB Domains** | data-engineering, multi-agent, python | Padrões Medallion, Claude API tool use, asyncio |
| **IaC Impact** | Databricks Workspace API + Commands API | Sem provisionamento de infra — usa workspace Free Edition já existente |

---

## Data Contract

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| CSV gerado pelo Data Generator Agent | File (in-memory → Google Sheets) | 50–500 linhas | Gerado a cada execução | Data Generator Agent |

### Schema Contract (runtime — inferido pelo Schema Agent)

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| Dinâmico — definido pelo Data Generator Agent | Inferido pelo Schema Agent | NOT NULL inferido por amostragem | Classificado pelo Governance Agent |

### Output Tables

| Table | Layer | Location | Schema |
|-------|-------|----------|--------|
| `bronze_{tema}_{run_id}` | Bronze | Databricks Delta | Raw CSV sem transformações |
| `silver_{tema}_{run_id}` | Silver | Databricks Delta | Schema correto + DQ aplicado + PII masking |

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|-------------|
| Bronze | Criada durante o run, no mesmo pipeline | Commands API retorna estado `Finished` |
| Silver | Criada imediatamente após Bronze no mesmo run | Commands API retorna estado `Finished` |

### Completeness Metrics

- Bronze deve conter o mesmo número de linhas do CSV gerado
- Silver deve conter apenas linhas que passaram nas regras de DQ

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Databricks Free Edition aceita chamadas à Commands API (`/api/1.2/commands/execute`) | Seria necessário encontrar alternativa de execução (ex: apenas GitOps sem execução) | [ ] |
| A-002 | Google Form real está configurado com campos compatíveis com o CSV gerado | Playwright falharia ao tentar preencher campos inexistentes | [ ] |
| A-003 | Tokens de API disponíveis: GitHub PAT, Slack Webhook, Databricks Token, Google Service Account | Qualquer agente dependente falharia | [ ] |
| A-004 | Python 3.10+ disponível no ambiente de execução | Sintaxe de `match/case` e `asyncio` podem falhar em versões anteriores | [ ] |
| A-005 | Google Sheets armazena a resposta do Forms com latência < 10s | Orchestrator poderia ler resposta incompleta | [ ] |

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Dor específica (tempo com ingestão manual), impacto claro, solução bem definida |
| Users | 3 | Dois personas com papéis e pain points distintos |
| Goals | 3 | 9 MUSTs, 2 SHOULDs, 1 COULD — todos com condição de teste |
| Success | 3 | 13 critérios mensuráveis e verificáveis |
| Scope | 3 | 9 itens explicitamente fora do escopo |
| **Total** | **15/15** | |

---

## Open Questions

Nenhuma — pronto para Design.

As suposições em A-001 a A-005 devem ser validadas antes ou durante a fase de Build, não bloqueiam o Design.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-05-20 | Heitor Sampaio | Initial version — extraído de BRAINSTORM_DATA_SQUAD.md |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_DATA_SQUAD.md`
