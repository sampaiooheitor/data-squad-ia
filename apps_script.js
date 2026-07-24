var GITHUB_OWNER = "sampaiooheitor";
var GITHUB_REPO  = "data-squad-ia";
var GITHUB_REF   = "main";

// =====================================================================
// CRIAÇÃO DO FORMULÁRIO
// Execute createForm() UMA VEZ no editor do Apps Script.
// Ela cria o form completo no Drive e instala o trigger onFormSubmit.
// =====================================================================
function createForm() {
  // Pasta de destino dos uploads
  var folders = DriveApp.getFoldersByName("Data Squad — Uploads");
  var uploadFolder = folders.hasNext()
    ? folders.next()
    : DriveApp.createFolder("Data Squad — Uploads");
  var folderId = uploadFolder.getId();

  var form = FormApp.create("Data Squad — Solicitação de Ingestão");
  form.setCollectEmail(true);
  form.setDescription(
    "Preencha este formulário para solicitar a ingestão de uma nova base de dados. " +
    "O agente lerá suas respostas e gerará o contrato de ingestão automaticamente."
  );

  // ── Seção 1: Identificação da fonte ──────────────────────────────────
  form.addSectionHeaderItem().setTitle("Seção 1 — Identificação da fonte");

  form.addTextItem()
    .setTitle("Sistema de origem")
    .setRequired(true);

  form.addTextItem()
    .setTitle("Descrição da base")
    .setRequired(true);

  form.addMultipleChoiceItem()
    .setTitle("Periodicidade da carga")
    .setChoiceValues(["Diária", "Semanal", "Mensal", "Eventual"])
    .setRequired(true);

  // ── Seção 2: Governança ───────────────────────────────────────────────
  form.addPageBreakItem().setTitle("Seção 2 — Governança");

  form.addTextItem()
    .setTitle("E-mail do dono do dado")
    .setRequired(true);

  form.addMultipleChoiceItem()
    .setTitle("Domínio de negócio")
    .setChoiceValues(["financeiro", "comercial", "operacional", "rh", "tecnologia"])
    .setRequired(true);

  form.addMultipleChoiceItem()
    .setTitle("Classificação da informação")
    .setHelpText(
      "Público: sem restrição de acesso. " +
      "Interno: uso interno da empresa. " +
      "Confidencial: dado sensível ou regulado (LGPD, financeiro, etc.)"
    )
    .setChoiceValues(["Público", "Interno", "Confidencial"])
    .setRequired(true);

  // ── Seção 3: Comportamento da carga ──────────────────────────────────
  form.addPageBreakItem().setTitle("Seção 3 — Comportamento da carga e reprocessamento");

  form.addMultipleChoiceItem()
    .setTitle("Como o arquivo se comporta?")
    .setChoiceValues([
      "Período específico — substitui só aquele período ao reprocessar",
      "Base completa — substitui a tabela inteira ao reprocessar"
    ])
    .setRequired(true);

  form.addTextItem()
    .setTitle("Qual campo representa a data de referência dos dados?")
    .setHelpText(
      "Nome da coluna com a competência dos dados (ex: dt_referencia). " +
      "NÃO é a data em que o arquivo foi gerado ou enviado."
    )
    .setRequired(false);

  form.addMultipleChoiceItem()
    .setTitle("A data de referência está no nome do arquivo?")
    .setChoiceValues(["Sim", "Não"])
    .setRequired(false);

  form.addTextItem()
    .setTitle("Padrão do nome do arquivo (se sim)")
    .setHelpText("Ex: relatorio_YYYYMMDD.csv ou vendas_MM_YYYY.txt")
    .setRequired(false);

  form.addMultipleChoiceItem()
    .setTitle("Granularidade do período")
    .setChoiceValues(["Diária", "Mensal", "Outra"])
    .setRequired(false);

  form.addMultipleChoiceItem()
    .setTitle("O arquivo pode trazer vários períodos misturados?")
    .setChoiceValues([
      "Não, um único período por arquivo",
      "Sim, pode misturar vários períodos"
    ])
    .setRequired(false);

  // ── Seção 4: Qualidade ────────────────────────────────────────────────
  form.addPageBreakItem().setTitle("Seção 4 — Qualidade");

  form.addMultipleChoiceItem()
    .setTitle("O que fazer com registros com problema?")
    .setChoiceValues([
      "Separar em quarentena e seguir com o restante",
      "Parar toda a carga (rejeitar o lote)",
      "Deixar entrar marcado como suspeito"
    ])
    .setRequired(true);

  form.addMultipleChoiceItem()
    .setTitle("Tolerância de erro")
    .setChoiceValues(["Zero erros", "Até 1%", "Até 5%"])
    .setRequired(true);

  form.addMultipleChoiceItem()
    .setTitle("O arquivo tem linha de rodapé com contagem de registros?")
    .setChoiceValues(["Sim", "Não", "Não sei"])
    .setRequired(true);

  // ── Seção 5: Dados sensíveis ──────────────────────────────────────────
  form.addPageBreakItem().setTitle("Seção 5 — Dados sensíveis");

  form.addParagraphTextItem()
    .setTitle("Quais colunas contêm dado pessoal ou sensível?")
    .setHelpText(
      "Informe os nomes separados por vírgula (ex: cpf_cliente, nome_cliente). " +
      "Se nenhuma, escreva 'nenhuma'."
    )
    .setRequired(true);

  form.addCheckboxItem()
    .setTitle("Confirmo que revisei todas as colunas do dicionário em busca de dados sensíveis")
    .setChoiceValues(["Sim, confirmo"])
    .setRequired(true);

  // ── Seção 6: Homologação ──────────────────────────────────────────────
  form.addPageBreakItem().setTitle("Seção 6 — Homologação");

  form.addTextItem()
    .setTitle("E-mail de quem valida a tabela em desenvolvimento")
    .setHelpText("Geralmente é você mesmo. Pode indicar outra pessoa se for o caso.")
    .setRequired(true);

  // ── Arquivos (adicionar manualmente no form após criar) ──────────────
  // addFileUploadItem() tem restrições no Apps Script — adicione os dois
  // campos de upload manualmente no editor do Google Forms:
  //   1. "Data Dictionary" — upload de arquivo, obrigatório
  //      Ajuda: "Arquivo XLSX: Origem | Tabela | Campo | Datatype | Format Data | Descricao | Indicador Sensivel"
  //   2. "CSV Data" — upload de arquivo, obrigatório
  //      Ajuda: "Amostra dos dados reais em CSV (primeiras 200 linhas são suficientes)"
  // ATENÇÃO: os títulos precisam ser exatamente esses — o Apps Script faz match pelo título.

  // ── Instala o trigger ─────────────────────────────────────────────────
  ScriptApp.newTrigger("onFormSubmit")
    .forForm(form)
    .onFormSubmit()
    .create();

  Logger.log("=== FORM CRIADO COM SUCESSO ===");
  Logger.log("URL de edição : " + form.getEditUrl());
  Logger.log("URL pública   : " + form.getPublishedUrl());
  Logger.log("Pasta uploads : " + uploadFolder.getUrl());
  Logger.log("Trigger onFormSubmit instalado. Delete o trigger antigo se existir (Editar > Acionadores).");
}
// =====================================================================

// Maps Google Form field title → contract_meta key.
// Titles here must match the form EXACTLY (case, acentos).
var FIELD_MAP = {
  "Sistema de origem":                                      "sistema_origem",
  "Descrição da base":                                      "descricao_base",
  "Periodicidade da carga":                                 "periodicidade",
  "E-mail do dono do dado":                                 "dono",
  "Domínio de negócio":                                     "dominio",
  "Classificação da informação":                            "classificacao",
  "Como o arquivo se comporta?":                            "modo_escrita",
  "Qual campo representa a data de referência dos dados?":  "campo_referencia",
  "A data de referência está no nome do arquivo?":          "referencia_no_nome",
  "Padrão do nome do arquivo (se sim)":                     "padrao_nome_arquivo",
  "Granularidade do período":                               "granularidade",
  "O arquivo pode trazer vários períodos misturados?":      "periodos_misturados",
  "O que fazer com registros com problema?":                "on_invalid",
  "Tolerância de erro":                                     "tolerancia_pct",
  "O arquivo tem linha de rodapé com contagem de registros?": "tem_rodape",
  "Quais colunas contêm dado pessoal ou sensível?":         "colunas_sensiveis",
  "E-mail de quem valida a tabela em desenvolvimento":      "homologador"
};

function onFormSubmit(e) {
  var pat = PropertiesService.getScriptProperties().getProperty("GITHUB_PAT");
  if (!pat) {
    Logger.log("GITHUB_PAT não configurado em Script Properties.");
    return;
  }

  var itemResponses = e.response.getItemResponses();
  var fileUrl   = null;
  var sampleCsv = "";
  var meta = {
    requested_by: e.response.getRespondentEmail() || "",
    requested_at: Utilities.formatDate(new Date(), "America/Sao_Paulo", "yyyy-MM-dd")
  };

  for (var i = 0; i < itemResponses.length; i++) {
    var item   = itemResponses[i];
    var title  = item.getItem().getTitle();
    var answer = item.getResponse();

    if (title === "Data Dictionary") {
      var urls = Array.isArray(answer) ? answer : [answer];
      fileUrl = urls[0];
    } else if (title === "CSV Data") {
      var sampleUrls = Array.isArray(answer) ? answer : [answer];
      if (sampleUrls && sampleUrls[0]) {
        var sampleFileId = extractFileId(sampleUrls[0]);
        var sampleFile = DriveApp.getFileById(sampleFileId);
        sampleCsv = sampleFile.getBlob().getDataAsString("UTF-8").trim();
      }
    } else {
      var key = FIELD_MAP[title];
      if (key) {
        meta[key] = Array.isArray(answer) ? answer.join(", ") : (answer || "");
      }
    }
  }

  if (!fileUrl) {
    Logger.log("Nenhum arquivo enviado em Data Dictionary.");
    return;
  }

  var fileId  = extractFileId(fileUrl);
  var dictCsv = readFileAsPipeSeparated(fileId);

  var lines     = dictCsv.trim().split("\n");
  var tableName = "";
  if (lines.length > 1) {
    var firstDataRow = lines[1].split("|");
    tableName = firstDataRow.length > 1 ? firstDataRow[1].trim() : "";
  }

  var payload = JSON.stringify({
    ref: GITHUB_REF,
    inputs: {
      table_name:    tableName,
      dict_csv:      dictCsv,
      sample_csv:    sampleCsv,
      contract_meta: JSON.stringify(meta)
    }
  });

  var options = {
    method: "post",
    contentType: "application/json",
    headers: {
      Authorization: "Bearer " + pat,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28"
    },
    payload: payload,
    muteHttpExceptions: true
  };

  var url = "https://api.github.com/repos/" + GITHUB_OWNER + "/" + GITHUB_REPO
            + "/actions/workflows/data_squad.yml/dispatches";

  var response = UrlFetchApp.fetch(url, options);
  Logger.log("GitHub Actions status: " + response.getResponseCode());
  Logger.log("Response: " + response.getContentText());
}

function readFileAsPipeSeparated(fileId) {
  var file     = DriveApp.getFileById(fileId);
  var mimeType = file.getMimeType();

  if (mimeType === "text/csv" || mimeType === "text/plain") {
    return file.getBlob().getDataAsString("UTF-8");
  }

  var tempFile = Drive.Files.copy(
    { title: "temp_dict_" + fileId, mimeType: MimeType.GOOGLE_SHEETS },
    fileId,
    { convert: true }
  );

  try {
    var ss    = SpreadsheetApp.openById(tempFile.id);
    var sheet = ss.getSheets()[0];
    var data  = sheet.getDataRange().getValues();

    var lines = data.map(function(row) {
      return row.join("|");
    });

    return lines.join("\n");
  } finally {
    Drive.Files.remove(tempFile.id);
  }
}

function extractFileId(url) {
  var match = url.match(/[-\w]{25,}/);
  return match ? match[0] : null;
}
