// ============================================================
// ⚙️ CONFIGURAÇÕES — ajuste conforme seu projeto
// ============================================================
const COLLECTION_NAME = 'transactions';  // nome da sua collection no Firestore
const SHEET_NAME      = 'Raw Data';    // nome da aba na planilha
const DATABASE_NAME   = 'big-julius-firestore' // nome do database

// Ordem das colunas na planilha
const HEADERS = [
  'bank', 'category', 'subcategory', 'date', 'description',
  'doc_type', 'installment', 'owner', 'payment_date',
  'settlement_period', 'value', 'extraction_date', 'source_file'
];

// Campos ignorados na deduplicação (conforme solicitado)
const DEDUP_EXCLUDE = ['category', 'subcategory', 'payment_date', 'settlement_period', 'extraction_date', 'source_file'];
const DEDUP_FIELDS  = HEADERS.filter(h => !DEDUP_EXCLUDE.includes(h));
// → ['bank', 'date', 'description', 'doc_type', 'installment', 'owner', 'value']


// ============================================================
// 🚀 IMPORTAR MÊS ÚNICO — prompt simples
// ============================================================
function importFirestoreData() {
  const ui = SpreadsheetApp.getUi();

  const response = ui.prompt(
    'Importar do Firestore',
    'Informe o mês e ano base (formato MM/YYYY):',
    ui.ButtonSet.OK_CANCEL
  );

  if (response.getSelectedButton() !== ui.Button.OK) return;

  const input = response.getResponseText().trim();

  const parts = input.split('/');
  const validFormat = parts.length === 2
    && parts[0].length === 2
    && parts[1].length === 4
    && !isNaN(parts[0])
    && !isNaN(parts[1]);

  if (!validFormat) {
    ui.alert('❌ Formato inválido. Use MM/YYYY — exemplo: 02/2026');
    return;
  }

  const [month, year] = input.split('/');
  const months = [{ month, year }];
  runImport_(months, `${month}/${year}`);
}


// ============================================================
// 📆 IMPORTAR POR PERÍODO — diálogo com combos de mês/ano
// ============================================================
function importFirestoreByRange() {
  const html = HtmlService
    .createHtmlOutput(buildRangeDialogHtml_())
    .setWidth(420)
    .setHeight(340);
  SpreadsheetApp.getUi().showModalDialog(html, '📆 Importar por período');
}


// ============================================================
// 🔧 HANDLER chamado pelo diálogo HTML
// ============================================================
function processRangeImport(startMonth, startYear, endMonth, endYear) {
  const months = generateMonthList_(startMonth, startYear, endMonth, endYear);

  if (months.length === 0) {
    return '❌ Período inválido: a data de início deve ser anterior ou igual à de fim.';
  }

  const label = `${startMonth}/${startYear} a ${endMonth}/${endYear}`;
  return runImport_(months, label);
}


// ============================================================
// 🚀 EXECUÇÃO COMUM DE IMPORTAÇÃO (usada por ambos os modos)
// ============================================================
function runImport_(monthList, label) {
  const ui = SpreadsheetApp.getUi();

  try {
    const token    = getAccessToken();
    const allDocs  = fetchAllDocuments(token);
    const filtered = filterByDateRange(allDocs, monthList);

    if (filtered.length === 0) {
      const msg = `ℹ️ Nenhum registro encontrado para ${label}.`;
      ui.alert(msg);
      return msg;
    }

    const inserted = writeToSheet(filtered);
    const msg = `✅ Concluído!\n\n📦 Encontrados: ${filtered.length}\n✨ Inseridos (novos): ${inserted}\n⏭️ Ignorados (duplicatas): ${filtered.length - inserted}`;
    ui.alert(msg);
    return msg;

  } catch (e) {
    const msg = '❌ Erro durante a importação:\n\n' + e.message;
    ui.alert(msg);
    Logger.log(e.stack || e.message);
    return msg;
  }
}


function getAccessToken() {
  return ScriptApp.getOAuthToken();
}

// ============================================================
// 📡 BUSCA TODOS OS DOCUMENTOS DA COLLECTION (com paginação)
// ============================================================
function fetchAllDocuments(token) {
  const props     = PropertiesService.getScriptProperties();
  const projectId = props.getProperty('FIREBASE_PROJECT_ID');
  const baseUrl   = `https://firestore.googleapis.com/v1/projects/${projectId}/databases/${DATABASE_NAME}/documents/${COLLECTION_NAME}`;
  const reqHeaders = { Authorization: `Bearer ${token}` };

  let documents = [];
  let pageToken = null;

  do {
    const url      = baseUrl + '?pageSize=300' + (pageToken ? `&pageToken=${pageToken}` : '');
    const response = UrlFetchApp.fetch(url, { headers: reqHeaders, muteHttpExceptions: true });
    const data     = JSON.parse(response.getContentText());

    if (data.error) {
      throw new Error(`Erro na API do Firestore: ${JSON.stringify(data.error)}`);
    }

    if (data.documents) {
      documents = documents.concat(data.documents);
    }

    pageToken = data.nextPageToken || null;

  } while (pageToken);

  Logger.log(`Total de documentos buscados: ${documents.length}`);
  return documents;
}


// ============================================================
// 📅 FILTRA DOCUMENTOS POR UMA LISTA DE MESES
//    Cartão de crédito → usa payment_date
//    Conta corrente    → usa date (não possui payment_date)
// ============================================================
function filterByDateRange(documents, monthList) {
  // Monta Set de chaves "MM-YYYY" para busca rápida
  const validPeriods = new Set(monthList.map(m => `${m.month}-${m.year}`));

  return documents.filter(doc => {
    const fields      = doc.fields || {};
    const paymentDate = extractValue(fields['payment_date']);
    const dateStr     = (paymentDate && typeof paymentDate === 'string')
                          ? paymentDate
                          : extractValue(fields['date']);

    if (!dateStr || typeof dateStr !== 'string') return false;

    const parts = dateStr.split('-');
    if (parts.length !== 3) return false;

    const [, mm, yyyy] = parts;
    return validPeriods.has(`${mm}-${yyyy}`);
  });
}


// ============================================================
// 📆 GERA LISTA DE MESES ENTRE INÍCIO E FIM (inclusive)
// ============================================================
function generateMonthList_(startMonth, startYear, endMonth, endYear) {
  const sm = parseInt(startMonth, 10);
  const sy = parseInt(startYear, 10);
  const em = parseInt(endMonth, 10);
  const ey = parseInt(endYear, 10);

  const list = [];
  let m = sm, y = sy;

  while (y < ey || (y === ey && m <= em)) {
    list.push({
      month: String(m).padStart(2, '0'),
      year:  String(y)
    });
    m++;
    if (m > 12) { m = 1; y++; }
  }

  return list;
}


// ============================================================
// 🧹 EXTRAI O VALOR LIMPO DE UM CAMPO TIPADO DO FIRESTORE
// ============================================================
function extractValue(field) {
  if (!field) return '';
  if (field.stringValue  !== undefined) return field.stringValue;
  if (field.doubleValue  !== undefined) return field.doubleValue;
  if (field.integerValue !== undefined) return Number(field.integerValue);
  if (field.booleanValue !== undefined) return field.booleanValue;
  if (field.nullValue    !== undefined) return '';
  if (field.timestampValue !== undefined) return field.timestampValue;
  return '';
}


// ============================================================
// 📝 ESCREVE NA PLANILHA EVITANDO DUPLICIDADES
// ============================================================
function writeToSheet(documents) {
  const ss    = SpreadsheetApp.getActiveSpreadsheet();
  let   sheet = ss.getSheetByName(SHEET_NAME);

  // Cria a aba se não existir
  if (!sheet) {
    sheet = ss.insertSheet(SHEET_NAME);
    sheet.appendRow(HEADERS);
    formatHeader(sheet);
  }

  // Garante cabeçalho se a aba estiver vazia
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(HEADERS);
    formatHeader(sheet);
  } else {
    // Se os cabeçalhos mudaram (ex: nova coluna), limpa e recria
    const currentHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    if (JSON.stringify(currentHeaders) !== JSON.stringify(HEADERS)) {
      Logger.log('⚠️ Cabeçalhos mudaram — limpando planilha para evitar desalinhamento.');
      sheet.clear();
      sheet.appendRow(HEADERS);
      formatHeader(sheet);
    }
  }

  // Carrega chaves de deduplicação dos dados já existentes na planilha
  const existingKeys = getExistingKeys(sheet);

  // Monta as linhas novas (evita duplicatas dentro do próprio lote também)
  const newRows = [];
  documents.forEach(doc => {
    const fields = doc.fields || {};
    const row    = buildRow(fields);
    const key    = buildDedupKey(row);

    if (!existingKeys.has(key)) {
      newRows.push(row);
      existingKeys.add(key);
    }
  });

  // Inserção em batch (muito mais rápido que appendRow em loop)
  if (newRows.length > 0) {
    const startRow = sheet.getLastRow() + 1;
    const range = sheet.getRange(startRow, 1, newRows.length, HEADERS.length);
    range.setValues(newRows);

    // Formata apenas colunas de data como texto para impedir auto-conversão,
    // preservando o tipo numérico do campo value.
    const DATE_COLUMNS = ['date', 'payment_date', 'settlement_period', 'extraction_date'];
    DATE_COLUMNS.forEach(col => {
      const colIndex = HEADERS.indexOf(col) + 1; // 1-based
      if (colIndex > 0) {
        sheet.getRange(startRow, colIndex, newRows.length, 1).setNumberFormat('@');
      }
    });
  }

  return newRows.length;
}


// ============================================================
// 🔑 CARREGA AS CHAVES DE DEDUPLICAÇÃO DA PLANILHA EXISTENTE
// ============================================================
function getExistingKeys(sheet) {
  const keys    = new Set();
  const lastRow = sheet.getLastRow();

  if (lastRow <= 1) return keys; // vazia ou só cabeçalho

  // Lê o cabeçalho real da planilha para mapear colunas corretamente,
  // mesmo que HEADERS tenha mudado desde a última importação.
  const sheetHeaders = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

  const data = sheet
    .getRange(2, 1, lastRow - 1, sheet.getLastColumn())
    .getValues();  // getValues() mantém os tipos originais (números, strings)

  data.forEach(row => {
    const obj = {};
    sheetHeaders.forEach((h, i) => { obj[h] = row[i]; });
    keys.add(buildDedupKey(obj));
  });

  return keys;
}


// ============================================================
// 🏗️ MONTA A LINHA NA ORDEM DOS HEADERS
// ============================================================
function buildRow(fields) {
  return HEADERS.map(h => extractValue(fields[h] || null));
}


// ============================================================
// 🔑 MONTA A CHAVE ÚNICA DE DEDUPLICAÇÃO
// Aceita tanto array (linha da planilha) quanto objeto
// ============================================================
function buildDedupKey(rowOrObj) {
  let obj = rowOrObj;

  if (Array.isArray(rowOrObj)) {
    obj = {};
    HEADERS.forEach((h, i) => { obj[h] = rowOrObj[i]; });
  }

  return DEDUP_FIELDS
    .map(f => normalizeValue(obj[f]))
    .join('||');
}


// ============================================================
// 🔄 NORMALIZA UM VALOR PARA COMPARAÇÃO CONSISTENTE
//    Garante que Date objects (auto-convertidos pelo Sheets)
//    sejam revertidos para o formato DD-MM-YYYY original.
// ============================================================
function normalizeValue(val) {
  if (val == null) return '';

  // Date objects criados por auto-conversão do Sheets
  if (val instanceof Date) {
    const dd   = String(val.getDate()).padStart(2, '0');
    const mm   = String(val.getMonth() + 1).padStart(2, '0');
    const yyyy = val.getFullYear();
    return `${dd}-${mm}-${yyyy}`;
  }

  return String(val).trim();
}


// ============================================================
// 🎨 FORMATA O CABEÇALHO DA PLANILHA
// ============================================================
function formatHeader(sheet) {
  const headerRange = sheet.getRange(1, 1, 1, HEADERS.length);
  headerRange.setBackground('#2c3e50');
  headerRange.setFontColor('#ffffff');
  headerRange.setFontWeight('bold');
  sheet.setFrozenRows(1);
}

function testeSimples() {
  SpreadsheetApp.getUi().alert('✅ Script funcionando!');
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('🔥 Firestore')
    .addItem('📅 Importar mês único...', 'importFirestoreData')
    .addItem('📆 Importar período...', 'importFirestoreByRange')
    .addToUi();
}


// ============================================================
// 🎨 HTML DO DIÁLOGO DE SELEÇÃO POR PERÍODO
// ============================================================
function buildRangeDialogHtml_() {
  const currentYear  = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1;
  const years = [];
  for (let y = currentYear - 3; y <= currentYear + 1; y++) years.push(y);

  const monthNames = [
    'Janeiro','Fevereiro','Março','Abril','Maio','Junho',
    'Julho','Agosto','Setembro','Outubro','Novembro','Dezembro'
  ];

  const monthOpts = monthNames.map((name, i) => {
    const val = String(i + 1).padStart(2, '0');
    const sel = (i + 1 === currentMonth) ? ' selected' : '';
    return '<option value="' + val + '"' + sel + '>' + val + ' - ' + name + '</option>';
  }).join('');

  const yearOpts = years.map(y => {
    const sel = (y === currentYear) ? ' selected' : '';
    return '<option value="' + y + '"' + sel + '>' + y + '</option>';
  }).join('');

  return '\
    <style>\
      * { box-sizing: border-box; font-family: "Google Sans", Arial, sans-serif; }\
      body { margin: 0; padding: 20px; background: #f8f9fa; color: #202124; }\
      h3 { margin: 0 0 6px; font-size: 14px; color: #5f6368; }\
      .row { display: flex; gap: 10px; margin-bottom: 16px; }\
      select {\
        flex: 1; padding: 8px 10px; border: 1px solid #dadce0;\
        border-radius: 8px; font-size: 14px; background: #fff;\
        cursor: pointer;\
      }\
      select:focus { outline: none; border-color: #1a73e8; }\
      .actions { display: flex; gap: 10px; justify-content: flex-end; margin-top: 20px; }\
      button {\
        padding: 10px 24px; border: none; border-radius: 8px;\
        font-size: 14px; font-weight: 500; cursor: pointer;\
      }\
      .btn-primary { background: #1a73e8; color: #fff; }\
      .btn-primary:hover { background: #1557b0; }\
      .btn-cancel { background: #e8eaed; color: #3c4043; }\
      .btn-cancel:hover { background: #d2d5d9; }\
      #status { margin-top: 14px; font-size: 13px; color: #5f6368; text-align: center; }\
      .separator { border-top: 1px solid #e0e0e0; margin: 4px 0 16px; }\
    </style>\
    <h3>🟢 Início</h3>\
    <div class="row">\
      <select id="sm">' + monthOpts + '</select>\
      <select id="sy">' + yearOpts + '</select>\
    </div>\
    <div class="separator"></div>\
    <h3>🔴 Fim</h3>\
    <div class="row">\
      <select id="em">' + monthOpts + '</select>\
      <select id="ey">' + yearOpts + '</select>\
    </div>\
    <div class="actions">\
      <button class="btn-cancel" onclick="google.script.host.close()">Cancelar</button>\
      <button class="btn-primary" id="btnImport" onclick="doImport()">Importar</button>\
    </div>\
    <div id="status"></div>\
    <script>\
      function doImport() {\
        var sm = document.getElementById("sm").value;\
        var sy = document.getElementById("sy").value;\
        var em = document.getElementById("em").value;\
        var ey = document.getElementById("ey").value;\
        document.getElementById("btnImport").disabled = true;\
        document.getElementById("status").textContent = "⏳ Importando, aguarde...";\
        google.script.run\
          .withSuccessHandler(function(msg) {\
            document.getElementById("status").textContent = msg || "Concluído!";\
            document.getElementById("btnImport").disabled = false;\
          })\
          .withFailureHandler(function(err) {\
            document.getElementById("status").textContent = "❌ " + err.message;\
            document.getElementById("btnImport").disabled = false;\
          })\
          .processRangeImport(sm, sy, em, ey);\
      }\
    </script>';
}


function diagnosticarPrivateKey() {
  const props  = PropertiesService.getScriptProperties();
  const rawKey = props.getProperty('PRIVATE_KEY');

  Logger.log('Tamanho da chave: ' + rawKey.length);
  Logger.log('Começa com BEGIN: ' + rawKey.startsWith('-----BEGIN PRIVATE KEY-----'));
  Logger.log('Contém \\\\n literal: ' + rawKey.includes('\\\\n'));
  Logger.log('Contém newline real: '+ rawKey.includes('\\n'));
  Logger.log('Primeiros 80 chars: ' + rawKey.substring(0, 80));
}

function diagnosticarPrivateKeyCompleta() {
  const props  = PropertiesService.getScriptProperties();
  const rawKey = props.getProperty('PRIVATE_KEY');

  Logger.log('Tamanho total: '    + rawKey.length);
  Logger.log('Termina com END: '  + rawKey.includes('-----END PRIVATE KEY-----'));
  Logger.log('Últimos 80 chars: ' + rawKey.substring(rawKey.length - 80));
}
