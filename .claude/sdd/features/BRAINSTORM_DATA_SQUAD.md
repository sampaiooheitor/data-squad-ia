# BRAINSTORM: Data Squad — Engenheiro de Dados Autônomo

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | DATA_SQUAD |
| **Date** | 2026-05-19 |
| **Author** | brainstorm-agent |
| **Status** | Ready for Define |

---

## Initial Idea

**Raw Input:** Sistema multi-agente chamado Data Squad que recebe solicitações de ingestão de dados via Google Forms (com amostra CSV + dicionário de dados), orquestra agentes especializados, e entrega um pipeline completo no Databricks com PR no GitHub e notificação no Slack. O objetivo é simular um engenheiro de dados autônomo.

**Context Gathered:**
- Projeto demo — sem código existente, estrutura de pastas SDD criada
- Stack definida: Python, Claude API, DuckDB local, GitHub API, Slack webhook, Databricks Free Edition
- Nenhuma amostra de dados disponível — tudo gerado pelos agentes

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|-------------|
| Likely Location | `src/agents/`, `src/tools/`, `src/orchestrator.py` | Separação clara entre agentes e ferramentas |
| Relevant KB Domains | data-engineering, multi-agent, databricks | Padrões de Medallion e Claude API tool use |
| IaC Patterns | Databricks Workspace API + Commands API | Sem Jobs API — usar `/api/1.2/commands/execute` |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Quem são os usuários do sistema? | Projeto demo — agente gera os dados fictícios automaticamente | Remove dependência de usuário real; fluxo 100% autônomo |
| 2 | Qual o cenário de negócio? | Dinâmico — agente escolhe a área da empresa a cada execução | Data Generator Agent deve variar o tema (RH, financeiro, e-commerce, etc.) |
| 3 | Como submeter ao Google Forms? | Playwright — browser automation | Formulário real para produtização futura; demo visual |
| 4 | Como fazer deploy no Databricks Free Edition? | GitOps: Git sync + Commands API para execução | Notebooks commitados no GitHub → sync no Databricks → execução via `/api/1.2/commands/execute` |
| 5 | Agentes em paralelo ou sequencial? | Fan-out: Schema primeiro, depois DQ + Governance em paralelo, depois Pipeline | Pipeline Agent recebe schema + DQ + governance e gera código completo |

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | N/A | 0 | Gerado pelo Data Generator Agent |
| Output examples | N/A | 0 | Gerado pelos agentes |
| Ground truth | N/A | 0 | Não disponível |
| Related code | N/A | 0 | Projeto novo |

**Como os agentes vão lidar com a ausência de amostras:**
- Data Generator Agent usa Claude API para criar CSV fictício realista com base no tema sorteado
- O dicionário de dados também é gerado pelo mesmo agente
- Schema Agent infere tipos a partir do CSV gerado

---

## Approaches Explored

### Approach A: Orquestrador Python + Claude API por agente ⭐ Recommended

**Description:** Um `orchestrator.py` central gerencia as fases de execução (sequencial/paralelo). Cada agente é uma função Python especializada que chama a Claude API com system prompt dedicado. Ferramentas externas (Playwright, GitHub, Slack, Databricks) são módulos independentes em `src/tools/`.

```
src/
├── orchestrator.py
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
```

**Pros:**
- Controle total do fluxo — sem surpresas de framework
- Cada agente é testável isoladamente
- Paralelismo explícito com `asyncio` / `ThreadPoolExecutor`
- Demo previsível e rastreável passo a passo

**Cons:**
- Orquestrador escrito manualmente
- Paralelismo requer gerenciamento de estado compartilhado

**Why Recommended:** O demo precisa ser previsível. Frameworks como CrewAI ou LangGraph adicionam comportamento não-determinístico que pode surpreender ao vivo.

---

### Approach B: Claude Managed Agents com tool use loop

**Description:** Cada agente usa o loop nativo de tool use da Anthropic — o modelo decide quando chamar ferramentas.

**Pros:** Mais "agêntico" de verdade, menos código imperativo

**Cons:** Comportamento não-determinístico, difícil de controlar no demo, latência maior

---

### Approach C: Framework externo (CrewAI / LangGraph)

**Description:** Abstração de orquestração multi-agente via framework.

**Pros:** Abstrações prontas

**Cons:** Dependência de framework, overhead de aprendizado, menos transparência no demo

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|-----------------|-------------------|
| CSV fictício gerado | File (in-memory) | ~100-500 linhas | Gerado a cada execução |

### Data Flow

```
Data Generator Agent
    → CSV fictício + dicionário de dados
    → Playwright submete ao Google Form
    → Google Sheets armazena resposta
    → Orchestrator lê via Sheets API
    → Schema Agent (fase 1)
    → DQ Agent || Governance Agent (fase 2 — paralelo)
    → Pipeline Agent (fase 3)
        → bronze_ingest.py  (raw CSV → Bronze Delta Table)
        → silver_transform.py (Bronze → Silver com DQ rules)
    → GitHub Agent || Reporter Agent (fase 4 — paralelo)
        → PR criado com todos os artefatos
        → Databricks importa notebooks via Workspace API
        → Databricks executa via Commands API
        → Bronze Delta Table criada ✅
        → Silver Delta Table criada ✅
    → Slack notifica com resumo + link pro Databricks
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Quantas camadas Medallion? | Bronze + Silver (sem Gold) | Pipeline Agent gera dois notebooks |
| 2 | O que vai para Bronze? | CSV bruto exatamente como chegou | Sem transformações na ingestão |
| 3 | O que vai para Silver? | Schema correto + regras DQ aplicadas | DQ Agent define as validações; Pipeline Agent as implementa |
| 4 | Haverá agregações? | Não — sem camada Gold no MVP | Escopo controlado |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-05-19 |
| **Reasoning** | Demo previsível, controle total do fluxo, cada etapa visível e testável |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|----------------------|
| 1 | Playwright para submeter ao Forms | Forms real para produtização futura; demo visual e convincente | HTTP POST (endpoint não-oficial, pode quebrar) |
| 2 | GitOps + Commands API para Databricks | Compatível com Free Edition; mostra tabela real criada | Apenas Git sync sem execução (não mostraria a tabela) |
| 3 | Schema Agent roda primeiro, isolado | Todos os outros agentes dependem do schema | Paralelismo total (Pipeline sem contexto de DQ e Governance) |
| 4 | Pipeline Agent roda após DQ + Governance | Código gerado já inclui validações e masking de PII inline | Pipeline em paralelo com DQ/Governance (código incompleto) |
| 5 | Bronze + Silver sem Gold | Sem agregações no MVP; mantém escopo controlado | Pipeline completo com Gold (fora do escopo do demo) |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|-------------------|----------------|----------------|
| Human-in-the-loop (aprovação antes do PR) | Quebra o fluxo autônomo do demo | Sim — v2 |
| Retry com exponential backoff | Complexidade desnecessária para demo | Sim |
| Dashboard web de status | Slack já cobre o feedback visual | Sim — v3 |
| Múltiplos workspaces Databricks | Demo usa um workspace só | Sim |
| Memória entre execuções (vector DB) | Cada run é independente no MVP | Sim — v2 |
| Multi-tenant / autenticação de usuários | Projeto demo de one engineer | Sim — produtização |
| Camada Gold | Sem agregações no escopo | Sim — pós-MVP |
| Execução agendada (cron) | Trigger manual suficiente para demo | Sim — v2 |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|---------------|-----------|
| Arquitetura dos agentes e ordem de execução | ✅ | Aprovado | Não |
| Escopo Medallion (Bronze + Silver) | ✅ | "Não terão agregações, só Bronze bruto e Silver estruturada com DQ" | Sim — Gold removido |
| Execução no Databricks (Commands API) | ✅ | "A gente vai conseguir ver a tabela final?" — confirmado com Commands API | Sim — execução volta ao MVP |
| Escopo final completo | ✅ | "Tá perfeito" | Não |

---

## Suggested Requirements for /define

### Problem Statement (Draft)
Times de dados perdem tempo com tarefas repetitivas de ingestão — o Data Squad automatiza 100% do ciclo, desde a solicitação até a tabela Delta pronta no Databricks.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| AI Engineer (demo persona) | Demonstrar capacidade de um sistema multi-agente autônomo end-to-end |
| Times de dados (pós-MVP) | Solicitar ingestão via Forms e receber pipeline pronto sem escrever código |

### Success Criteria (Draft)

- [ ] Data Generator Agent cria CSV fictício com tema dinâmico e dicionário de dados
- [ ] Playwright submete os dados ao Google Form sem intervenção humana
- [ ] Orchestrator lê a resposta do Google Sheets e dispara os agentes
- [ ] Schema Agent infere schema correto com tipos, nullability e PKs
- [ ] DQ Agent gera regras de qualidade adequadas ao schema
- [ ] Governance Agent classifica colunas (PII, sensibilidade, owner)
- [ ] Pipeline Agent gera `bronze_ingest.py` e `silver_transform.py` com DQ e governance inline
- [ ] GitHub PR criado automaticamente com todos os artefatos
- [ ] Notebooks importados e executados no Databricks via API
- [ ] Bronze Delta Table e Silver Delta Table visíveis no Databricks
- [ ] Slack recebe notificação com resumo e link para as tabelas
- [ ] Fluxo completo roda sem intervenção humana

### Constraints Identified

- Databricks Free Edition: sem Jobs API — usar Workspace API + Commands API (`/api/1.2/commands/execute`)
- Google Forms: sem API oficial de submissão — usar Playwright
- Demo deve ser determinístico e previsível (sem frameworks não-controlados)
- Python como linguagem única em todo o projeto

### Out of Scope (Confirmed)

- Camada Gold (sem agregações)
- Human-in-the-loop / aprovações manuais
- Dashboard web de monitoramento
- Autenticação e multi-tenancy
- Retry automático com backoff
- Memória persistente entre execuções
- Execução agendada (cron)

---

## Agent Architecture Summary

```
FASE 1 — Sequencial
└── Data Generator Agent  → CSV fictício + dicionário (tema dinâmico)
└── Playwright Runner      → submete ao Google Form

FASE 2 — Sequencial
└── Schema Agent           → schema.json (tipos, nullability, PKs)

FASE 3 — Paralelo
├── DQ Agent               → dq_rules.json
└── Governance Agent       → governance_metadata.json

FASE 4 — Sequencial
└── Pipeline Agent         → bronze_ingest.py + silver_transform.py

FASE 5 — Paralelo
├── GitHub Agent           → PR com todos os artefatos
│   └── Databricks Client  → import + execute notebooks → Delta Tables ✅
└── Reporter Agent         → Slack notification com resumo + links
```

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 6 |
| Approaches Explored | 3 |
| Features Removed (YAGNI) | 8 |
| Validations Completed | 4 |
| Selected Approach | A — Orquestrador Python + Claude API |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_DATA_SQUAD.md`
