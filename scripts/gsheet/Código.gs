// ============================================================
// ⚙️ CONFIGURAÇÕES — ajuste conforme seu projeto
// ============================================================
const COLLECTION_NAME = 'transactions';  // nome da sua collection no Firestore
const SHEET_NAME      = 'Raw Data';    // nome da aba na planilha
const DATABASE_NAME   = 'big-julius-firestore' // nome do database

// Ordem das colunas na planilha
const HEADERS = [
  'bank', 'category', 'date', 'description',
  'doc_type', 'installment', 'owner', 'settlement_period',
  'value', 'extraction_date', 'source_file'
];

// Campos ignorados na deduplicação (conforme solicitado)
const DEDUP_EXCLUDE = ['category', 'settlement_period', 'extraction_date', 'source_file'];
const DEDUP_FIELDS  = HEADERS.filter(h => !DEDUP_EXCLUDE.includes(h));
// → ['bank', 'date', 'description', 'doc_type', 'installment', 'owner', 'value']


// ============================================================
// 🚀 PONTO DE ENTRADA — será chamado pelo botão
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

  try {
    ui.alert(`⏳ Buscando registros de ${month}/${year}...\\n\\nClique OK e aguarde. A planilha será atualizada automaticamente.`);

    const token     = getAccessToken();
    const allDocs   = fetchAllDocuments(token);
    const filtered  = filterByMonthYear(allDocs, month, year);

    if (filtered.length === 0) {
      ui.alert(`ℹ️ Nenhum registro encontrado para ${month}/${year}.`);
      return;
    }

    const inserted = writeToSheet(filtered);
    ui.alert(`✅ Concluído!\\n\\n📦 Encontrados: ${filtered.length}\\n✨ Inseridos (novos): ${inserted}\\n⏭️ Ignorados (duplicatas): ${filtered.length - inserted}`);

  } catch (e) {
    ui.alert('❌ Erro durante a importação:\\n\\n' + e.message);
    Logger.log(e.stack || e.message);
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
// 📅 FILTRA DOCUMENTOS PELO MÊS/ANO (campo settlement_period: "MM-YYYY")
// ============================================================
function filterByMonthYear(documents, month, year) {
  const target = `${month}-${year}`; // ex: "02-2026"

  return documents.filter(doc => {
    const fields    = doc.fields || {};
    const periodStr = extractValue(fields['settlement_period']); // "MM-YYYY"

    if (!periodStr || typeof periodStr !== 'string') return false;

    return periodStr === target;
  });
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
    sheet.getRange(startRow, 1, newRows.length, HEADERS.length).setValues(newRows);
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

  const data = sheet
    .getRange(2, 1, lastRow - 1, HEADERS.length)
    .getValues();

  data.forEach(row => {
    const obj = {};
    HEADERS.forEach((h, i) => { obj[h] = row[i]; });
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
    .map(f => String(obj[f] ?? '').trim())
    .join('||');
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
    .addItem('Importar por mês...', 'importFirestoreData')
    .addToUi();
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
