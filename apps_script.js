var GITHUB_OWNER = "sampaiooheitor";
var GITHUB_REPO  = "data-squad-ia";
var GITHUB_REF   = "main";

function onFormSubmit(e) {
  var pat = PropertiesService.getScriptProperties().getProperty("GITHUB_PAT");
  if (!pat) {
    Logger.log("GITHUB_PAT não configurado em Script Properties.");
    return;
  }

  var itemResponses = e.response.getItemResponses();
  var fileUrl   = null;
  var sampleCsv = "";

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
      table_name: tableName,
      tema: "",
      dict_csv: dictCsv,
      sample_csv: sampleCsv
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

  // CSV ou texto simples — lê direto
  if (mimeType === "text/csv" || mimeType === "text/plain") {
    return file.getBlob().getDataAsString("UTF-8");
  }

  // XLSX ou Google Sheets — converte para Sheet temporária e lê
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
