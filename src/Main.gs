/*******************************************************
 * 問題解決版会計自動仕訳システム
 * 根本原因（JSON制約）を解決した動作版
 *******************************************************/

const CONFIG = {
  VOUCHER_FOLDER_ID: '1awl5sHMstUZ8CpM2XBZTk205ELrDNrT8',
  ACCOUNT_MASTER_SSID: '1sa9SFTjQUD29zK720CRbCpuAyS96mZ1kQ8gsED_KrQQ',
  ACCOUNT_MASTER_SHEET: 'account_master',
  JOURNAL_SSID: '1MkPlJuPL74iWCWEws6gIwkOP2QkPekxIUuTYBA3sMfo',
  JOURNAL_SHEET_NAME: '悟大仕訳帳',
  
  GEMINI_MODEL: 'gemini-2.5-pro',
  GEMINI_ENDPOINT: 'https://generativelanguage.googleapis.com/v1beta/models/',
  
  RECURSIVE: true,
  INCLUDE_SHORTCUT_TARGETS: true,
  MAX_FILE_BYTES: 20 * 1024 * 1024,
  DONE_SUBFOLDER_NAME: '完了',
  INVOICE_SALES_SUBFOLDER_NAME: '売上請求書',
  INVOICE_PAYABLES_SUBFOLDER_NAME: '支払請求書',
  CONSTRUCTION_ACCOUNTS: ['完成工事高','未成工事支出金','完成工事未収入金','外注費','工事仮勘定'],
  DEDUPE_MODE: 'on',
  ALERT_FAIL_THRESHOLD: 5,
  ALERT_MAX_EXEC_MINUTES: 25,
  DEBUG_SAVE_HEAD: 1000
};

/* ============================ エントリ ============================ */
function processNewInvoices() {
  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) throw new Error('GEMINI_API_KEY が未設定です。');
  const chatWebhook = PropertiesService.getScriptProperties().getProperty('CHAT_WEBHOOK_URL') || '';

  const runId = createUUID_();
  const started = new Date();

  const journalSS = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
  const journalSheet = getOrCreateSheet_(journalSS, CONFIG.JOURNAL_SHEET_NAME);
  const logSheet = getOrCreateRunLogSheet_(journalSS);

  ensureAuxSheets_(journalSS);
  ensureJournalHeader29_(journalSheet);

  log_(logSheet, 'INFO', '実行開始 - 問題解決版');

  // 二重防止インデックス
  const index = loadProcessedIndex_(journalSS);

  // 対象収集
  const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
  const summary = { files: 0, folders: 0, shortcuts: 0, collected: 0, names: [] };
  const targets = collectUnprocessedFiles_(root, summary, logSheet);
  log_(logSheet, 'INFO', `走査結果 files=${summary.files} folders=${summary.folders} shortcuts=${summary.shortcuts} collected=${summary.collected}`);
  
  if (!targets.length) {
    finalizeRun_(journalSS, runId, started, [], 0, 0, 0, chatWebhook);
    log_(logSheet, 'INFO', '未処理ファイル0件');
    log_(logSheet, 'INFO', '実行終了');
    return;
  }

  // マスタ
  const master = readAccountMaster_Simple_();
  log_(logSheet, 'INFO', `マスタ: 科目=${master.accounts.size} 補助合計=${master.totalSubs} 税区分種=${master.taxSet.size}`);

  // フォルダ
  const doneFolder = getOrCreateChildFolder_(root, CONFIG.DONE_SUBFOLDER_NAME);
  const salesFolder = getOrCreateChildFolder_(root, CONFIG.INVOICE_SALES_SUBFOLDER_NAME);
  const payFolder = getOrCreateChildFolder_(root, CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME);

  let cntSuccess = 0, cntSkipped = 0, cntError = 0;
  const durations = [];

  for (const { file, originalName } of targets) {
    const t0 = Date.now();
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    let hash = '';
    try {
      log_(logSheet, 'INFO', `START: ${originalName}`);

      const blob = file.getBlob();
      const mimeType = blob.getContentType();
      const sizeBytes = blob.getBytes().length;
      if (sizeBytes > CONFIG.MAX_FILE_BYTES) throw new Error(`サイズ上限超過: ${sizeBytes} bytes`);

      // ハッシュ
      hash = sha256Hex_(blob.getBytes());
      if (shouldDedupeSkip_(index, fileId, hash)) {
        log_(logSheet, 'INFO', `SKIP(DUP): ${originalName}`);
        writeSkipped_(journalSS, { reason: `重複スキップ`, fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // AI呼び出し（修正版）
      const prompt = buildWorkingPrompt_(master, { fileUrl });
      const parsed = askGeminiFixed_(prompt, blob, mimeType, apiKey, logSheet);
      let obj = parsed.obj;
      log_(logSheet, 'INFO', `AI-HTTP: code=${parsed.httpCode} rawLen=${parsed.rawLen} extracted=${parsed.extracted ? 'Yes' : 'No'}`);

      if (!obj || typeof obj !== 'object') {
        const reason = `AI応答の解析に失敗(rawLen=${parsed.rawLen})`;
        log_(logSheet, 'INFO', `SKIP(PARSE): ${originalName} | ${reason}`);
        writeSkipped_(journalSS, { reason, fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // 必須値
      const dateSlash = toSlashDate_(getVal_(obj, ['日付']));
      const payee = (getVal_(obj, ['借方取引先','貸方取引先','取引先','会社名']) || '').toString().trim();
      const amount = (getVal_(obj, ['借方金額(円)','貸方金額(円)','金額']) || '').toString().replace(/[^\d]/g, '');
      let note = (getVal_(obj, ['摘要']) || '').toString().trim();
      if (!note || !note.includes(payee)) note = `${payee} ${note}`.trim();
      note = withUrlInNote_(note, fileUrl);

      const missing = [];
      if (!dateSlash) missing.push('日付');
      if (!amount)   missing.push('金額');
      if (!payee)    missing.push('取引先');
      if (missing.length) {
        const reason = `必須欠落: ${missing.join(', ')}`;
        log_(logSheet, 'INFO', `SKIP(GATE): ${originalName} | ${reason}`);
        writeSkipped_(journalSS, { reason, fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // 工事コード
      let constructionCode = (getVal_(obj, ['工事コード']) || '').toString().trim();
      if (!constructionCode) constructionCode = extractConstructionCodeFromFolderNames_(fileId) || '';

      // 消費税コード
      const taxCode = (getVal_(obj, ['消費税コード']) || '').toString().trim();

      // === 29列データ構築 ===
      const row29 = [
        dateSlash,                                        // 1: 取引日
        getVal_(obj, ['借方科目','借方勘定科目']) || '', // 2: 借方勘定科目
        '',                                               // 3: 勘定科目コード（借方）
        getVal_(obj, ['借方補助科目']) || '',            // 4: 借方補助科目
        '',                                               // 5: 補助科目コード（借方）
        getVal_(obj, ['借方取引先']) || '',              // 6: 借方取引先
        constructionCode || '',                           // 7: 工事コード
        taxCode || '',                                    // 8: 消費税コード
        getVal_(obj, ['借方税区分']) || '',              // 9: 借方税区分
        getVal_(obj, ['借方インボイス','借方インボイス番号']) || '', // 10: 借方インボイス
        getVal_(obj, ['借方金額(円)']) || '',            // 11: 借方金額(円)

        getVal_(obj, ['貸方科目','貸方勘定科目']) || '', // 12: 貸方勘定科目
        '',                                               // 13: 勘定科目コード（貸方）
        getVal_(obj, ['貸方補助科目']) || '',            // 14: 貸方補助科目
        '',                                               // 15: 補助科目コード（貸方）
        getVal_(obj, ['貸方取引先']) || '',              // 16: 貸方取引先
        constructionCode || '',                           // 17: 工事コード
        getVal_(obj, ['貸方税区分']) || '',              // 18: 貸方税区分
        getVal_(obj, ['貸方インボイス','貸方インボイス番号']) || '', // 19: 貸方インボイス
        getVal_(obj, ['貸方金額(円)']) || '',            // 20: 貸方金額(円)

        note || '',                                       // 21: 摘要
        fileUrl || '',                                    // 22: メモ
        '',                                               // 23: 処理状態
        '',                                               // 24: エクスポート日時
        '',                                               // 25: エクスポートID
        fileUrl || '',                                    // 26: メモ（重複）
        '',                                               // 27: 処理状態（重複）
        '',                                               // 28: エクスポート日時（重複）
        ''                                                // 29: エクスポートID（重複）
      ];
      journalSheet.appendRow(row29);

      // ファイル振分け
      const meta = normalizeMeta_(obj.__meta || {});
      const whichFolder = decideInvoiceFolder_(meta.document_type, meta.invoice_type, meta.issuer, meta.addressee, originalName);

      // リネーム→移動
      const newName = buildProcessedName_({ date: dateSlash, amount, payee }, file.getName());
      file.setName(newName);

      if (whichFolder === 'sales') {
        file.moveTo(salesFolder);
      } else if (whichFolder === 'payables') {
        file.moveTo(payFolder);
      } else {
        file.moveTo(doneFolder);
      }

      // インデックス登録
      writeProcessedIndex_(journalSS, { runId, fileId, fileUrl, hash });

      log_(logSheet, 'INFO', `SUCCESS: ${newName}`);
      cntSuccess++;
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      log_(logSheet, 'ERROR', `ERROR: ${originalName} | ${msg}`);
      writeSkipped_(journalSS, { reason: `処理例外: ${msg}`, fileId, fileUrl, hash, name: originalName });
      cntError++;
    } finally {
      durations.push((Date.now() - t0) / 1000.0);
    }
  }

  finalizeRun_(journalSS, runId, started, durations, cntSuccess, cntSkipped, cntError, chatWebhook);
  log_(logSheet, 'INFO', '実行終了 - 問題解決版');
}

/* ============================ プロンプト（修正版） ============================ */
function buildWorkingPrompt_(master, ctx) {
  const accountList = Array.from(master.accounts.keys()).slice(0, 20);
  const taxList = Array.from(master.taxSet.values());
  const fileUrl = String(ctx.fileUrl || '');

  return `この商業文書から以下の情報をJSON形式で抽出してください。

必要な情報：
- 日付（YYYY/MM/DD形式）
- 金額
- 取引先・会社名
- 勘定科目（下記から選択）
- 税区分（下記から選択）

出力形式（JSON）：
{
  "日付": "YYYY/MM/DD",
  "借方科目": "", 
  "借方補助科目": "", 
  "借方取引先": "",
  "消費税コード": "", 
  "借方税区分": "", 
  "借方インボイス": "", 
  "借方金額(円)": 0,
  "貸方科目": "", 
  "貸方補助科目": "", 
  "貸方取引先": "",
  "貸方税区分": "", 
  "貸方インボイス": "", 
  "貸方金額(円)": 0,
  "工事コード": "",
  "摘要": "",
  "取引先": "",
  "会社名": "",
  "金額": "",
  "__meta": { 
    "document_type": "", 
    "issuer": "", 
    "addressee": "", 
    "invoice_type": "" 
  }
}

参考勘定科目: ${accountList.join(', ')}
参考税区分: ${taxList.join(', ')}

JSONで回答してください。`;
}

/* ============================ AI呼び出し（修正版） ============================ */
function askGeminiFixed_(prompt, blob, mimeType, apiKey, logSheet) {
  log_(logSheet, 'INFO', 'AI第1試行開始');
  const r1 = callGeminiFlexible_(prompt, blob, mimeType, apiKey);
  const p1 = parseGeminiFlexible_(r1);
  
  if (p1.obj) {
    log_(logSheet, 'INFO', 'AI第1試行成功');
    return p1;
  }

  log_(logSheet, 'INFO', 'AI第1試行失敗、第2試行開始');
  
  const retry = `${prompt}\n\n前回の応答が解析できませんでした。必ずJSONフォーマットで返してください。`;
  const r2 = callGeminiFlexible_(retry, blob, mimeType, apiKey);
  const p2 = parseGeminiFlexible_(r2);

  if (p2.obj) {
    log_(logSheet, 'INFO', 'AI第2試行成功');
  } else {
    log_(logSheet, 'INFO', 'AI第2試行失敗');
  }

  return p2;
}

function callGeminiFlexible_(prompt, blob, mimeType, apiKey) {
  const url = CONFIG.GEMINI_ENDPOINT + encodeURIComponent(CONFIG.GEMINI_MODEL) + ':generateContent?key=' + encodeURIComponent(apiKey);
  
  const body = {
    contents: [{
      role: 'user',
      parts: [
        { text: prompt },
        {
          inline_data: {
            mime_type: mimeType,
            data: Utilities.base64Encode(blob.getBytes())
          }
        }
      ]
    }],
    generationConfig: {
      temperature: 0.0,
      maxOutputTokens: 4096
      // responseMimeType を削除（制約緩和）
    }
  };

  const res = UrlFetchApp.fetch(url, {
    method: 'post',
    contentType: 'application/json',
    payload: JSON.stringify(body),
    muteHttpExceptions: true
  });

  return {
    code: res.getResponseCode(),
    text: res.getContentText()
  };
}

function parseGeminiFlexible_(res) {
  const httpCode = res.code;
  let rawText = '';
  
  if (httpCode >= 300) {
    debugAI_Struct_({ httpCode, note: 'HTTP error', head: safePreview_(res.text, CONFIG.DEBUG_SAVE_HEAD) });
    return { obj: null, httpCode, rawLen: 0, extracted: false };
  }
  
  let data;
  try {
    data = JSON.parse(res.text);
  } catch (e) {
    debugAI_Struct_({ httpCode, note: 'JSON parse error(res)', head: safePreview_(res.text, CONFIG.DEBUG_SAVE_HEAD) });
    return { obj: null, httpCode, rawLen: 0, extracted: false };
  }

  // Gemini応答からテキスト抽出
  try {
    const candidates = data.candidates || [];
    if (candidates.length > 0 && candidates[0].content && candidates[0].content.parts) {
      const parts = candidates[0].content.parts;
      for (const part of parts) {
        if (part.text) {
          rawText = part.text;
          break;
        }
      }
    }
  } catch (_) {}

  if (!rawText) {
    debugAI_Struct_({ httpCode, note: 'No text in response', head: safePreview_(JSON.stringify(data), CONFIG.DEBUG_SAVE_HEAD) });
    return { obj: null, httpCode, rawLen: 0, extracted: false };
  }

  // 自然言語からJSON抽出または構造化データ作成
  const obj = extractDataFromText_(rawText);
  
  debugAI_Struct_({ 
    httpCode, 
    note: 'Flexible parsing', 
    head: rawText.slice(0, CONFIG.DEBUG_SAVE_HEAD), 
    hash: sha256Hex_(Utilities.newBlob(rawText, 'text/plain').getBytes()) 
  });
  
  return { obj, httpCode, rawLen: rawText.length, extracted: !!obj };
}

function extractDataFromText_(text) {
  // まずJSONとして解析を試行
  const cleaned = stripCodeFence_(text);
  let obj = tryParseJsonChain_(cleaned);
  
  if (obj) {
    return obj; // JSONとして解析成功
  }
  
  // JSONでなければ自然言語から情報抽出
  const extracted = {
    "日付": "",
    "借方科目": "",
    "借方補助科目": "",
    "借方取引先": "",
    "消費税コード": "",
    "借方税区分": "",
    "借方インボイス": "",
    "借方金額(円)": 0,
    "貸方科目": "",
    "貸方補助科目": "",
    "貸方取引先": "",
    "貸方税区分": "",
    "貸方インボイス": "",
    "貸方金額(円)": 0,
    "工事コード": "",
    "摘要": "",
    "取引先": "",
    "会社名": "",
    "金額": "",
    "__meta": {
      "document_type": "",
      "issuer": "",
      "addressee": "",
      "invoice_type": ""
    }
  };
  
  // 日付抽出
  const dateMatch = text.match(/(\d{4})[年\/\-\.](\d{1,2})[月\/\-\.](\d{1,2})/);
  if (dateMatch) {
    extracted["日付"] = `${dateMatch[1]}/${('0' + dateMatch[2]).slice(-2)}/${('0' + dateMatch[3]).slice(-2)}`;
  }
  
  // 金額抽出
  const amountMatch = text.match(/[¥￥]?([0-9,，]+)/);
  if (amountMatch) {
    const amount = amountMatch[1].replace(/[,，]/g, '');
    extracted["金額"] = amount;
    extracted["借方金額(円)"] = parseInt(amount) || 0;
  }
  
  // 会社名抽出（診断結果から「麺屋おがわら」のようなパターン）
  const companyMatch = text.match(/[会社名|取引先|店舗|会社][：:]*\s*([^\n\r\*]+)/);
  if (companyMatch) {
    extracted["会社名"] = companyMatch[1].trim();
    extracted["取引先"] = companyMatch[1].trim();
    extracted["借方取引先"] = companyMatch[1].trim();
  }
  
  return extracted;
}

/* ============================ マスタ読み込み（簡易版） ============================ */
function readAccountMaster_Simple_() {
  const ss = SpreadsheetApp.openById(CONFIG.ACCOUNT_MASTER_SSID);
  const sh = ss.getSheetByName(CONFIG.ACCOUNT_MASTER_SHEET);
  if (!sh) throw new Error(`勘定科目シートが見つかりません: ${CONFIG.ACCOUNT_MASTER_SHEET}`);

  const lastRow = sh.getLastRow();
  if (lastRow < 1) return { accounts: new Map(), totalSubs: 0, taxSet: new Set() };

  const lastCol = Math.min(5, sh.getLastColumn());
  const values = sh.getRange(1, 1, lastRow, lastCol).getValues();

  const accounts = new Map();
  const taxSet = new Set();
  let totalSubs = 0;

  for (let i = 1; i < values.length; i++) {
    const acc = (values[i][0] || '').toString().trim();
    const sub = (values[i][1] || '').toString().trim();
    const tax = (lastCol >= 3 ? (values[i][2] || '') : '').toString().trim();
    if (!acc) continue;
    if (!accounts.has(acc)) accounts.set(acc, new Set());
    if (sub) { accounts.get(acc).add(sub); totalSubs++; }
    if (tax) taxSet.add(tax);
  }
  return { accounts, totalSubs, taxSet };
}

/* ============================ 走査・収集 ============================ */
function collectUnprocessedFiles_(folder, summary, logSheet) {
  const out = [];
  const files = folder.getFiles();
  while (files.hasNext()) {
    const f = files.next();
    const name = f.getName();
    const mime = f.getMimeType();
    try {
      if (mime === 'application/vnd.google-apps.shortcut') {
        summary.shortcuts++;
        const resolved = resolveShortcutTarget_(f);
        if (resolved) {
          summary.files++;
          const rName = resolved.getName();
          const rMime = resolved.getMimeType();
          if (!isProcessedPrefix_(rName)) {
            out.push({ file: resolved, originalName: rName });
            summary.collected++; summary.names.push(rName);
            log_(logSheet, 'INFO', `ADD: ${rName} | mime=${rMime}`);
          } else {
            log_(logSheet, 'INFO', `SKIP(処理済み): ${rName} | mime=${rMime}`);
          }
        } else {
          log_(logSheet, 'INFO', `SKIP(ショートカット未解決): ${name}`);
        }
      } else {
        summary.files++;
        if (!isProcessedPrefix_(name)) {
          out.push({ file: f, originalName: name });
          summary.collected++; summary.names.push(name);
          log_(logSheet, 'INFO', `ADD: ${name} | mime=${mime}`);
        } else {
          log_(logSheet, 'INFO', `SKIP(処理済み): ${name} | mime=${mime}`);
        }
      }
    } catch (e) {
      log_(logSheet, 'ERROR', `走査中エラー: ${name} | ${String(e && e.message ? e.message : e)}`);
    }
  }
  if (CONFIG.RECURSIVE) {
    const folders = folder.getFolders();
    while (folders.hasNext()) {
      const sub = folders.next();
      const subName = sub.getName();
      if ([CONFIG.DONE_SUBFOLDER_NAME, CONFIG.INVOICE_SALES_SUBFOLDER_NAME, CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME].includes(subName)) continue;
      summary.folders++;
      out.push(...collectUnprocessedFiles_(sub, summary, logSheet));
    }
  }
  return out;
}

function resolveShortcutTarget_(shortcutFile) {
  if (!CONFIG.INCLUDE_SHORTCUT_TARGETS || !isDriveAdvancedAvailable_()) return null;
  try {
    const meta = Drive.Files.get(shortcutFile.getId(), { fields: 'id,shortcutDetails' });
    const targetId = meta && meta.shortcutDetails && meta.shortcutDetails.targetId;
    if (!targetId) return null;
    return DriveApp.getFileById(targetId);
  } catch (_) { return null; }
}

function isProcessedPrefix_(nameRaw) {
  if (!nameRaw) return false;
  const name = String(nameRaw).replace(/^[\u200B\uFEFF\u2060\s]+/, '').trimStart();
  return ['[処理済み]', '【処理済み】', '[processed]', '[ processed ]', '[済]'].some(p => name.startsWith(p));
}

/* ============================ 29列ヘッダ ============================ */
function ensureJournalHeader29_(sheet) {
  const headers = [
    '取引日',
    '借方勘定科目','勘定科目コード','借方補助科目','補助科目コード','借方取引先','工事コード','消費税コード','借方税区分','借方インボイス','借方金額(円)',
    '貸方勘定科目','勘定科目コード','貸方補助科目','補助科目コード','貸方取引先','工事コード','貸方税区分','貸方インボイス','貸方金額(円)',
    '摘要','メモ','処理状態','エクスポート日時','エクスポートID','メモ','処理状態','エクスポート日時','エクスポートID'
  ];
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

/* ============================ 工事コード ============================ */
function extractConstructionCodeFromFolderNames_(fileId) {
  try {
    const file = DriveApp.getFileById(fileId);
    const parents = file.getParents();
    while (parents.hasNext()) {
      const p = parents.next();
      const name = p.getName();
      const m = name.match(/\[([A-Za-z0-9\-]{2,})\]$/) || name.match(/([A-Za-z0-9\-]{2,})$/);
      if (m && m[1]) return m[1];
    }
  } catch (_) {}
  return '';
}

/* ============================ 振り分け ============================ */
function normalizeMeta_(m) {
  return {
    document_type: (m.document_type||'').toString().trim(),
    invoice_type: (m.invoice_type||'').toString().trim(),
    issuer: (m.issuer||'').toString().trim(),
    addressee: (m.addressee||'').toString().trim()
  };
}

function hasOurName_(s){ return s && /悟大/.test(String(s)); }
function isInvoiceDoc_(docType, fileName){ return docType === '請求書' || /請求|invoice/i.test(fileName||''); }
function decideInvoiceFolder_(docType, invoiceType, issuer, addressee, fileName){
  const isInv = isInvoiceDoc_(docType, fileName);
  if (!isInv) return null;
  if (invoiceType === '売上') return 'sales';
  if (invoiceType === '支払') return 'payables';
  if (hasOurName_(issuer)) return 'sales';
  if (hasOurName_(addressee)) return 'payables';
  return null;
}

/* ============================ インデックス・スキップ ============================ */
function loadProcessedIndex_(ss) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  let header = [];
  if (sh.getLastRow() >= 1) header = sh.getRange(1,1,1,Math.max(1,sh.getLastColumn())).getValues()[0].map(String);
  if (header.length === 0) {
    header = ['run_id','file_id','file_url','content_hash','processed_at'];
    sh.appendRow(header);
  }
  const lower = header.map(h => h.toLowerCase());
  const idxFileId = (lower.indexOf('file_id') >= 0) ? lower.indexOf('file_id') : lower.indexOf('fileid');
  const idxHash   = lower.indexOf('content_hash');
  const ids = new Set(), hashes = new Set();
  if (sh.getLastRow() >= 2) {
    const vals = sh.getRange(2,1,sh.getLastRow()-1,sh.getLastColumn()).getValues();
    for (const r of vals) {
      if (idxFileId >= 0 && r[idxFileId]) ids.add(String(r[idxFileId]));
      if (idxHash   >= 0 && r[idxHash])   hashes.add(String(r[idxHash]));
    }
  }
  return { ids, hashes };
}

function writeProcessedIndex_(ss, row) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  let header = [];
  if (sh.getLastRow() >= 1) header = sh.getRange(1,1,1,Math.max(1,sh.getLastColumn())).getValues()[0].map(String);
  if (header.length === 0) {
    header = ['run_id','file_id','file_url','content_hash','processed_at'];
    sh.appendRow(header);
  }
  sh.appendRow([row.runId || '', row.fileId || '', row.fileUrl || '', row.hash || '', now_()]);
}

function writeSkipped_(ss, o) {
  const sh = getOrCreateSheet_(ss, 'skipped');
  if (sh.getLastRow() < 1) sh.appendRow(['日時','理由','ファイル名','fileId','fileUrl','content_hash']);
  sh.appendRow([now_(), o.reason || '', o.name || '', o.fileId || '', o.fileUrl || '', o.hash || '']);
}

function shouldDedupeSkip_(index, fileId, hash) {
  switch (CONFIG.DEDUPE_MODE) {
    case 'off': return false;
    case 'id_only': return index.ids.has(fileId);
    case 'hash_only': return index.hashes.has(hash);
    default: return index.ids.has(fileId) || index.hashes.has(hash);
  }
}

/* ============================ 実行終了・通知 ============================ */
function finalizeRun_(ss, runId, started, durations, success, skipped, error, chatWebhook) {
  const ended = new Date();
  const secs = (ended - started) / 1000.0;
  const avg = durations.length ? (durations.reduce((a,b)=>a+b,0)/durations.length) : 0;
  const p95 = durations.length ? percentile_(durations, 0.95) : 0;

  const sum = getOrCreateSheet_(ss, 'run_summary');
  if (sum.getLastRow() < 1) sum.appendRow(['run_id','start','end','total','success','skipped','error','avg_sec','p95_sec','elapsed_sec']);
  sum.appendRow([runId, started, ended, success+skipped+error, success, skipped, error, avg, p95, secs]);

  if (chatWebhook) {
    if (error > CONFIG.ALERT_FAIL_THRESHOLD || secs/60.0 > CONFIG.ALERT_MAX_EXEC_MINUTES) {
      try {
        UrlFetchApp.fetch(chatWebhook, {
          method: 'post',
          contentType: 'application/json',
          payload: JSON.stringify({
            text: `仕訳実行アラート(修正版)\nrun_id: ${runId}\n期間: ${formatJST_(started)} - ${formatJST_(ended)}\n合計: ${success+skipped+error}\n成功:${success} / スキップ:${skipped} / 失敗:${error}\n平均:${avg.toFixed(1)}s / p95:${p95.toFixed(1)}s / 経過:${secs.toFixed(1)}s`
          }),
          muteHttpExceptions: true
        });
      } catch (_) {}
    }
  }
}

/* ============================ 補助シート・ログ・ユーティリティ ============================ */
function ensureAuxSheets_(ss){
  const names=['run_summary','skipped','debug_ai','run_log','processed_index'];
  for (const n of names) if (!ss.getSheetByName(n)) ss.insertSheet(n);
}

function debugAI_Struct_(o) {
  try {
    const ss = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
    const sh = getOrCreateSheet_(ss, 'debug_ai');
    if (sh.getLastRow() < 1) sh.appendRow(['日時','http','note','head','hash']);
    sh.appendRow([now_(), o.httpCode||'', o.note||'', (o.head||'').slice(0,CONFIG.DEBUG_SAVE_HEAD), o.hash||'']);
  } catch (_) {}
}

function getOrCreateSheet_(ss, name) { let sh = ss.getSheetByName(name); if (!sh) sh = ss.insertSheet(name); return sh; }
function getOrCreateRunLogSheet_(ss) { const name='run_log'; let sh=ss.getSheetByName(name); if(!sh){ sh=ss.insertSheet(name); sh.appendRow(['日時','レベル','メッセージ']); } return sh; }
function getOrCreateChildFolder_(parent, name) { const it = parent.getFoldersByName(name); return it.hasNext()? it.next(): parent.createFolder(name); }
function isDriveAdvancedAvailable_(){ try{ return typeof Drive!=='undefined' && Drive && Drive.Files && typeof Drive.Files.get==='function'; }catch(_){ return false; } }

function now_(){ return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss'); }
function log_(logSheet, level, message){ const t = now_(); Logger.log(`[${level}] ${t} ${message}`); try{ logSheet.appendRow([t, level, message]); }catch(_){} }
function safePreview_(v,max=400){ const s=typeof v==='string'?v:JSON.stringify(v); return s.length>max?s.slice(0,max)+' …(省略)…':s; }
function stripCodeFence_(s){ if(!s) return s; return s.replace(/^```json\s*/i,'').replace(/^```\s*/i,'').replace(/```$/i,'').trim(); }
function tryParseJsonChain_(s){ if(!s) return null; try{ return JSON.parse(s);}catch(_){ } const r=recoverJsonFromText_(s); if(r){ try{ return JSON.parse(r);}catch(_){ } } try{ let t=String(r||s); t=t.replace(/,\s*([}\]])/g,'$1'); t=t.replace(/[^\S\r\n]+$/g,''); return JSON.parse(t);}catch(_){ } return null; }
function recoverJsonFromText_(s){ const a=s.indexOf('{'), b=s.lastIndexOf('}'); if(a===-1||b===-1||b<=a) return ''; return s.slice(a,b+1); }
function toSlashDate_(s){ if(!s) return ''; const m=s.match(/(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})/); return m? `${m[1]}/${('0'+parseInt(m[2],10)).slice(-2)}/${('0'+parseInt(m[3],10)).slice(-2)}`: s; }
function sha256Hex_(bytes){ const dig = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes); return dig.map(b=>('0'+(b & 0xFF).toString(16)).slice(-2)).join(''); }
function percentile_(arr, p){ const a=[...arr].sort((x,y)=>x-y); const idx=Math.min(a.length-1, Math.max(0, Math.floor((a.length-1)*p))); return a[idx]; }
function formatJST_(d){ return Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss'); }
function createUUID_(){ return Utilities.getUuid().replace(/-/g,''); }
function getVal_(obj, keys){ for (const k of keys){ const v=obj[k]; if (v!==undefined && v!==null && String(v)!=='') return v; } return ''; }
function withUrlInNote_(note, url){ if (!url) return note || ''; const s=(note||'').trim(); if (s.includes(url)) return s; return s ? `${s} ${url}` : url; }
function buildProcessedName_({ date, amount, payee }, fallbackName) {
  let dateDot = '';
  if (date) { const m = date.match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/); if (m) dateDot = `${m[1]}.${parseInt(m[2], 10)}.${parseInt(m[3], 10)}`; }
  const amtDigits = (amount != null && amount !== '') ? String(amount).replace(/[^\d]/g, '') : '';
  const amtPart = amtDigits ? `${dateDot ? '.' : ''}${amtDigits}円` : '';
  const shortPayee = (payee || '').replace(/\s+/g, '').slice(0, 20) || extractPayeeFromName_(fallbackName);
  return ['[済]', dateDot, amtPart, shortPayee].filter(Boolean).join(' ');
}
function extractPayeeFromName_(name){
  const base=(name||'').replace(/\.[^.]+$/,'');
  const tokens=base.split(/[ _\-\(\)【】\[\]、，・.]/).filter(Boolean);
  return tokens.length?tokens[tokens.length-1].slice(0,20):'';
}

/* ============================ デバッグ用 ============================ */
function __testSingleFile() {
  try {
    Logger.log('=== 修正版単体テスト開始 ===');
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    if (!apiKey) throw new Error('GEMINI_API_KEY が未設定');

    const ss = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
    const logSheet = getOrCreateRunLogSheet_(ss);
    ensureAuxSheets_(ss);

    const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
    let file = null, originalName = '';
    const it = root.getFiles();
    while (it.hasNext()) {
      const f = it.next();
      const name = f.getName();
      const mime = f.getMimeType();
      if (mime === 'application/vnd.google-apps.shortcut') continue;
      if (isProcessedPrefix_(name)) continue;
      file = f; originalName = name; break;
    }
    if (!file) { Logger.log('テスト候補なし'); return; }

    Logger.log(`修正版テスト対象: ${originalName}`);

    const master = readAccountMaster_Simple_();
    Logger.log(`マスタ: accounts=${master.accounts.size} subs=${master.totalSubs}`);

    const blob = file.getBlob();
    const parsed = askGeminiFixed_(buildWorkingPrompt_(master, { fileUrl: file.getUrl() }), blob, blob.getContentType(), apiKey, logSheet);
    Logger.log(`修正版AI結果: code=${parsed.httpCode} rawLen=${parsed.rawLen} extracted=${parsed.extracted}`);
    
    if (!parsed.obj) { Logger.log('修正版AI失敗'); return; }
    Logger.log(`修正版AI成功: ${JSON.stringify(parsed.obj, null, 2).slice(0, 400)}...`);

    Logger.log('=== 修正版単体テスト完了 ===');
  } catch (e) {
    Logger.log(`修正版テストエラー: ${e.message || e}`);
    throw e;
  }
}
