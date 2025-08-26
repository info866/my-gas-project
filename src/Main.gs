/*******************************************************
 * 安定化会計自動仕訳システム（動作実績準拠版）
 * ベース：動作していた実績コード（document 3）
 * 改善：ヘッダ検出強化 + エラーハンドリング強化
 *******************************************************/

const CONFIG = {
  // === 必要ID ===
  VOUCHER_FOLDER_ID: '1awl5sHMstUZ8CpM2XBZTk205ELrDNrT8',
  ACCOUNT_MASTER_SSID: '1sa9SFTjQUD29zK720CRbCpuAyS96mZ1kQ8gsED_KrQQ',
  ACCOUNT_MASTER_SHEET: 'account_master',
  JOURNAL_SSID: '1MkPlJuPL74iWCWEws6gIwkOP2QkPekxIUuTYBA3sMfo',
  JOURNAL_SHEET_NAME: '悟大仕訳帳',

  // === Gemini ===
  GEMINI_MODEL: 'gemini-2.5-pro',
  GEMINI_ENDPOINT: 'https://generativelanguage.googleapis.com/v1beta/models/',
  MAX_FILE_BYTES: 48 * 1024 * 1024,

  // === 設定 ===
  RECURSIVE: true,
  INCLUDE_SHORTCUT_TARGETS: true,
  DONE_SUBFOLDER_NAME: '完了',
  INVOICE_SALES_SUBFOLDER_NAME: '売上請求書',
  INVOICE_PAYABLES_SUBFOLDER_NAME: '支払請求書',

  // === 工事系科目 ===
  CONSTRUCTION_ACCOUNTS: ['完成工事高','未成工事支出金','完成工事未収入金','外注費','工事仮勘定'],

  // === 制御 ===
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

  // 必須シートを事前作成
  ensureAuxSheets_(journalSS);
  ensureJournalHeader29_(journalSheet);

  log_(logSheet, 'INFO', '実行開始');

  // 重複防止インデックス
  const index = loadProcessedIndex_(journalSS);

  // マスタ読み込み（強化版ヘッダ検出）
  const master = readAccountMasterRobust_();
  log_(logSheet, 'INFO', `マスタ読み込み完了: 勘定科目=${master.accounts.size} 補助科目=${master.totalSubs} 税区分=${master.taxSet.size} 勘定科目コード=${master.accCodeCount} 補助科目コード=${master.subCodeCount}`);

  // 対象ファイル収集
  const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
  const summary = { files: 0, folders: 0, shortcuts: 0, collected: 0, names: [] };
  const targets = collectUnprocessedFiles_(root, summary, logSheet);

  log_(logSheet, 'INFO', `走査完了: ファイル=${summary.files} フォルダ=${summary.folders} ショートカット=${summary.shortcuts} 処理対象=${summary.collected}`);

  if (!targets.length) {
    finalizeRun_(journalSS, runId, started, [], 0, 0, 0, chatWebhook);
    log_(logSheet, 'INFO', '処理対象ファイルなし。実行終了');
    return;
  }

  // フォルダ準備
  const doneFolder = getOrCreateChildFolder_(root, CONFIG.DONE_SUBFOLDER_NAME);
  const salesFolder = getOrCreateChildFolder_(root, CONFIG.INVOICE_SALES_SUBFOLDER_NAME);
  const payFolder = getOrCreateChildFolder_(root, CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME);

  let cntSuccess = 0, cntSkipped = 0, cntError = 0;
  const durations = [];

  // メインループ
  for (const { file, originalName } of targets) {
    const t0 = Date.now();
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    let hash = '';

    try {
      log_(logSheet, 'INFO', `処理開始: ${originalName}`);

      const blob = file.getBlob();
      const mimeType = blob.getContentType();
      const sizeBytes = blob.getBytes().length;

      if (sizeBytes > CONFIG.MAX_FILE_BYTES) {
        throw new Error(`ファイルサイズ上限超過: ${sizeBytes} bytes`);
      }

      // 重複チェック
      hash = sha256Hex_(blob.getBytes());
      if (shouldDedupeSkip_(index, fileId, hash)) {
        log_(logSheet, 'INFO', `スキップ（重複）: ${originalName}`);
        writeSkipped_(journalSS, { 
          reason: `重複スキップ(${CONFIG.DEDUPE_MODE})`, 
          fileId, fileUrl, hash, name: originalName 
        });
        cntSkipped++;
        continue;
      }

      // フォルダから工事コード抽出
      const folderCode = extractConstructionCodeFromFolderNames_(fileId);

      // AI呼び出し（動作実績パターン）
      const prompt = buildWorkingPrompt_(master, { fileUrl });
      const parsed = askGeminiOneShotRobust_(prompt, blob, mimeType, apiKey, logSheet);
      
      log_(logSheet, 'INFO', `AI応答: code=${parsed.httpCode} cand=${parsed.candCount} rawLen=${parsed.rawLen} block=${parsed.blockReason || '-'}`);

      if (!parsed.obj || typeof parsed.obj !== 'object') {
        const reason = `AI応答解析失敗(code=${parsed.httpCode}, rawLen=${parsed.rawLen}, cand=${parsed.candCount})`;
        log_(logSheet, 'INFO', `スキップ（解析失敗）: ${originalName} | ${reason}`);
        writeSkipped_(journalSS, { reason, fileId, fileUrl, hash, name: originalName });
        cntSkipped++;
        continue;
      }

      const obj = parsed.obj;

      // エラー応答チェック
      if (obj['エラー']) {
        log_(logSheet, 'INFO', `スキップ（AIエラー）: ${originalName} | ${obj['エラー']}`);
        writeSkipped_(journalSS, { 
          reason: `AIエラー: ${obj['エラー']}`, 
          fileId, fileUrl, hash, name: originalName 
        });
        cntSkipped++;
        continue;
      }

      // 必須データ抽出
      const dateSlash = toSlashDate_(getVal_(obj, ['日付']));
      const payee = (getVal_(obj, ['借方取引先','貸方取引先']) || '').toString().trim();
      const amount = (getVal_(obj, ['借方金額(円)','貸方金額(円)']) || '').toString().replace(/[^\d]/g, '');
      
      let note = (getVal_(obj, ['摘要']) || '').toString().trim();
      if (!note || !note.includes(payee)) {
        note = `${payee} ${note}`.trim();
      }
      note = withUrlInNote_(note, fileUrl);

      // 必須チェック
      const missing = [];
      if (!dateSlash) missing.push('日付');
      if (!amount) missing.push('金額');
      if (!payee) missing.push('取引先');

      if (missing.length) {
        const reason = `必須データ不足: ${missing.join(', ')}`;
        log_(logSheet, 'INFO', `スキップ（データ不足）: ${originalName} | ${reason}`);
        writeSkipped_(journalSS, { reason, fileId, fileUrl, hash, name: originalName });
        cntSkipped++;
        continue;
      }

      // 借方・貸方データ
      let dAcc = norm_(getVal_(obj, ['借方科目', '借方勘定科目']));
      let dSub = norm_(getVal_(obj, ['借方補助科目']));
      let cAcc = norm_(getVal_(obj, ['貸方科目', '貸方勘定科目']));
      let cSub = norm_(getVal_(obj, ['貸方補助科目']));

      // 工事コード（JSON優先、フォルダ次点）
      let constructionCode = norm_(getVal_(obj, ['工事コード'])) || folderCode;

      // 工事系科目の強制補助科目設定
      if (constructionCode) {
        if (isConstructionAccountName_(dAcc)) dSub = constructionCode;
        if (isConstructionAccountName_(cAcc)) cSub = constructionCode;
      }

      // 科目コード・補助コード解決（AI応答優先、マスタ次点）
      let dAccCode = norm_(getVal_(obj, ['借方勘定科目コード'])) || getAccountCode_(master, dAcc) || '';
      let cAccCode = norm_(getVal_(obj, ['貸方勘定科目コード'])) || getAccountCode_(master, cAcc) || '';
      let dSubCode = norm_(getVal_(obj, ['借方補助科目コード'])) || getSubCode_(master, dAcc, dSub) || '';
      let cSubCode = norm_(getVal_(obj, ['貸方補助科目コード'])) || getSubCode_(master, cAcc, cSub) || '';

      // 補助コードが未定義で、補助名が英数字コードらしい場合は採用
      if (!dSubCode && /^[0-9A-Za-z\-]{2,}$/.test(dSub)) dSubCode = dSub;
      if (!cSubCode && /^[0-9A-Za-z\-]{2,}$/.test(cSub)) cSubCode = cSub;

      // 税関連
      const taxCode = norm_(getVal_(obj, ['消費税コード']));
      const dTaxCat = norm_(getVal_(obj, ['借方税区分']));
      const cTaxCat = norm_(getVal_(obj, ['貸方税区分']));
      const dInv = norm_(getVal_(obj, ['借方インボイス', '借方インボイス番号']));
      const cInv = norm_(getVal_(obj, ['貸方インボイス', '貸方インボイス番号']));
      const dAmt = norm_(getVal_(obj, ['借方金額(円)']));
      const cAmt = norm_(getVal_(obj, ['貸方金額(円)']));

      // 29列データ構築
      const row29 = [
        dateSlash,          // 1: 取引日
        dAcc,               // 2: 借方勘定科目
        dAccCode,           // 3: 勘定科目コード（借方）
        dSub,               // 4: 借方補助科目
        dSubCode,           // 5: 補助科目コード（借方）
        norm_(getVal_(obj, ['借方取引先'])), // 6: 借方取引先
        constructionCode || '',             // 7: 工事コード
        taxCode || '',       // 8: 消費税コード
        dTaxCat || '',       // 9: 借方税区分
        dInv || '',          // 10: 借方インボイス
        dAmt || '',          // 11: 借方金額(円)
        cAcc,               // 12: 貸方勘定科目
        cAccCode,           // 13: 勘定科目コード（貸方）
        cSub,               // 14: 貸方補助科目
        cSubCode,           // 15: 補助科目コード（貸方）
        norm_(getVal_(obj, ['貸方取引先'])), // 16: 貸方取引先
        constructionCode || '',             // 17: 工事コード
        cTaxCat || '',       // 18: 貸方税区分
        cInv || '',          // 19: 貸方インボイス
        cAmt || '',          // 20: 貸方金額(円)
        note || '',          // 21: 摘要
        fileUrl || '',       // 22: メモ
        '',                  // 23: 処理状態
        '',                  // 24: エクスポート日時
        '',                  // 25: エクスポートID
        fileUrl || '',       // 26: メモ（重複）
        '',                  // 27: 処理状態（重複）
        '',                  // 28: エクスポート日時（重複）
        ''                   // 29: エクスポートID（重複）
      ];

      journalSheet.appendRow(row29);

      // ファイル振り分け
      const meta = normalizeMeta_(obj.__meta || {});
      const whichFolder = decideInvoiceFolder_(meta.document_type, meta.invoice_type, meta.issuer, meta.addressee, originalName);

      // ファイル処理
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

      log_(logSheet, 'INFO', `処理完了: ${newName} | 振分=${whichFolder || '完了'}`);
      cntSuccess++;

    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      log_(logSheet, 'ERROR', `処理エラー: ${originalName} | ${msg}`);
      writeSkipped_(journalSS, { 
        reason: `処理例外: ${msg}`, 
        fileId, fileUrl, hash, name: originalName 
      });
      cntError++;
    } finally {
      durations.push((Date.now() - t0) / 1000.0);
    }
  }

  // 実行終了
  finalizeRun_(journalSS, runId, started, durations, cntSuccess, cntSkipped, cntError, chatWebhook);
  log_(logSheet, 'INFO', '実行終了');
}

/* ============================ プロンプト（動作実績準拠） ============================ */
function buildWorkingPrompt_(master, ctx) {
  const accountList = Array.from(master.accounts.keys());
  const subMapLines = [];
  for (const [acc, meta] of master.accounts.entries()) {
    const subs = Array.from(meta.subs.keys());
    if (subs.length) {
      subMapLines.push(`- ${acc}: ${subs.join(' | ')}`);
    }
  }
  const taxList = Array.from(master.taxSet.values());
  const fileUrl = String(ctx.fileUrl || '');

  return `
あなたは当社専用の会計仕訳AIです。添付の商業文書（請求書/領収書/注文書/見積書/レシート等）を解析し、**登録済みの【科目】【補助科目】【税区分】のみ**を使って、以下の日本語キーの**厳密JSON**を1個だけ返してください（説明文・余計な文字・コードフェンスは禁止）。

【売上/支払 判定（ファイル振分け用）】
- 「発行者（issuer）」に「株式会社悟大」または「悟大」が含まれる → 請求書の種別（invoice_type）は「売上」
- それ以外で「宛先（addressee）」に「株式会社悟大」または「悟大」が含まれる → invoice_type は「支払」
- 判定不能なら空文字 "" とする
- ドキュメント種別（document_type）も返す（請求書/領収書/見積書/注文書/納品書/その他 から最も適切な1つ）

【日付変換】和暦・簡略を YYYY/MM/DD へ。2桁年で元号記号なしは西暦20YYとみなす。

【出力（キー名と順序固定／追加禁止／数値は整数）】
{
  "日付": "YYYY/MM/DD",
  "借方科目": "", "借方勘定科目コード": "", "借方補助科目": "", "借方補助科目コード": "", "借方取引先": "",
  "消費税コード": "",
  "借方税区分": "", "借方インボイス": "", "借方金額(円)": 0,
  "貸方科目": "", "貸方勘定科目コード": "", "貸方補助科目": "", "貸方補助科目コード": "", "貸方取引先": "",
  "貸方税区分": "", "貸方インボイス": "", "貸方金額(円)": 0,
  "工事コード": "",
  "摘要": "",
  "__meta": {
    "document_type": "請求書|領収書|見積書|注文書|納品書|その他",
    "issuer": "",
    "addressee": "",
    "invoice_type": ""
  }
}

【登録済みマスタ（名称のみ）】
- 勘定科目:
${accountList.map(a => `  - ${a}`).join('\n') || '(なし)'}
- 補助科目（科目ごと）:
${subMapLines.join('\n') || '(なし)'}
- 税区分:
${taxList.join(' | ') || '(未定義)'}

注意：
- ファイルURLはスプレッドシート側で保持（URL: ${fileUrl}）。出力JSONへ含めない。
- 工事コードは書類から抽出できない場合がある。出力に入れなくてよい（システムがフォルダ名から補完する）。
`.trim();
}

/* ============================ AI呼び出し（動作実績準拠） ============================ */
function askGeminiOneShotRobust_(prompt, blob, mimeType, apiKey, logSheet) {
  // 第1試行
  log_(logSheet, 'INFO', 'AI第1試行開始');
  const r1 = callGemini_(prompt, blob, mimeType, apiKey);
  const p1 = parseGeminiResponse_(r1);
  
  if (p1.obj) {
    log_(logSheet, 'INFO', 'AI第1試行成功');
    return p1;
  }

  log_(logSheet, 'INFO', 'AI第1試行失敗、第2試行開始');
  
  // 第2試行（リトライプロンプト）
  const retry = `${prompt}\n\n上記のとおりですが、前回は取得できませんでした。今度は**厳密JSONのみ**を返してください（説明・余計な文字・コードフェンス禁止）。`;
  const r2 = callGemini_(retry, blob, mimeType, apiKey);
  const p2 = parseGeminiResponse_(r2);

  if (p2.obj) {
    log_(logSheet, 'INFO', 'AI第2試行成功');
  } else {
    log_(logSheet, 'INFO', 'AI第2試行失敗');
  }

  return p2;
}

function callGemini_(prompt, blob, mimeType, apiKey) {
  const url = CONFIG.GEMINI_ENDPOINT + encodeURIComponent(CONFIG.GEMINI_MODEL) + ':generateContent?key=' + encodeURIComponent(apiKey);
  const body = {
    contents: [{ 
      role: 'user', 
      parts: [
        { text: prompt },
        { inline_data: { mime_type: mimeType, data: Utilities.base64Encode(blob.getBytes()) } }
      ]
    }],
    generationConfig: { 
      temperature: 0.0, 
      maxOutputTokens: 4096, 
      responseMimeType: 'application/json' 
    }
  };
  
  const res = UrlFetchApp.fetch(url, { 
    method: 'post', 
    contentType: 'application/json', 
    payload: JSON.stringify(body), 
    muteHttpExceptions: true 
  });
  
  return { code: res.getResponseCode(), text: res.getContentText() };
}

function parseGeminiResponse_(res) {
  const httpCode = res.code;
  let candCount = 0, blockReason = '', partsKinds = '', rawText = '';
  
  if (httpCode >= 300) {
    debugAI_Struct_({ httpCode, note: 'HTTP error', head: safePreview_(res.text, CONFIG.DEBUG_SAVE_HEAD) });
    return { obj: null, httpCode, candCount: 0, rawLen: 0, blockReason: `HTTP ${httpCode}`, partsKinds };
  }
  
  let data;
  try {
    data = JSON.parse(res.text);
  } catch (e) {
    debugAI_Struct_({ httpCode, note: 'JSON parse error(res)', head: safePreview_(res.text, CONFIG.DEBUG_SAVE_HEAD) });
    return { obj: null, httpCode, candCount: 0, rawLen: 0, blockReason: 'JSON parse error', partsKinds };
  }

  try {
    blockReason = (data.promptFeedback && (data.promptFeedback.blockReason || data.promptFeedback.block_reason)) || '';
  } catch (_) {}

  try {
    const cands = data.candidates || [];
    candCount = cands.length;
    const kinds = new Set();
    for (const c of cands) {
      const parts = (c && c.content && c.content.parts) || [];
      for (const p of parts) {
        for (const k of Object.keys(p || {})) {
          kinds.add(k);
        }
      }
    }
    partsKinds = Array.from(kinds).join(',');
  } catch (_) {}

  rawText = pickTextFromGeminiDeep_(data) || '';
  const cleaned = stripCodeFence_(rawText);
  
  debugAI_Struct_({ 
    httpCode, candCount, blockReason, partsKinds, 
    head: cleaned.slice(0, CONFIG.DEBUG_SAVE_HEAD), 
    hash: cleaned ? sha256Hex_(Utilities.newBlob(cleaned, 'text/plain').getBytes()) : '' 
  });
  
  const obj = tryParseJsonChain_(cleaned);
  return { obj, httpCode, candCount, rawLen: cleaned.length, blockReason, partsKinds };
}

function pickTextFromGeminiDeep_(resp) {
  try {
    const cands = resp.candidates || [];
    for (const c of cands) {
      const parts = (c && c.content && c.content.parts) || [];
      for (const p of parts) {
        if (typeof p.text === 'string' && p.text.trim()) {
          return p.text;
        }
      }
      for (const p of parts) {
        if (typeof p.functionCall === 'object') {
          const s = JSON.stringify(p.functionCall);
          if (s) return s;
        }
        if (typeof p.executable_code === 'string' && p.executable_code.trim()) {
          return p.executable_code;
        }
        if (typeof p.code === 'string' && p.code.trim()) {
          return p.code;
        }
      }
    }
  } catch (_) {}
  return '';
}

/* ============================ マスタ読み込み強化版 ============================ */
function readAccountMasterRobust_() {
  const ss = SpreadsheetApp.openById(CONFIG.ACCOUNT_MASTER_SSID);
  const sh = ss.getSheetByName(CONFIG.ACCOUNT_MASTER_SHEET);
  if (!sh) throw new Error(`勘定科目シートが見つかりません: ${CONFIG.ACCOUNT_MASTER_SHEET}`);

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 1 || lastCol < 1) {
    return { accounts: new Map(), totalSubs: 0, taxSet: new Set(), accCodeCount: 0, subCodeCount: 0 };
  }

  const values = sh.getRange(1, 1, lastRow, lastCol).getValues();
  const rawHeader = values[0].map(v => String(v || '').trim());
  
  // ヘッダ正規化（不可視文字除去）
  const normalizedHeader = rawHeader.map(h => normalizeHeaderName_(h));
  
  // 柔軟ヘッダマッチング
  const findHeaderIdx = (candidates) => {
    for (const cand of candidates) {
      const normalizedCand = normalizeHeaderName_(cand);
      const idx = normalizedHeader.findIndex(h => h === normalizedCand);
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const idxAccName = findHeaderIdx(['勘定科目', '科目', 'アカウント', 'account']);
  const idxSubName = findHeaderIdx(['補助科目', 'サブ科目', 'subaccount', 'サブ', '補助']);
  const idxTax = findHeaderIdx(['税区分', '消費税区分', '税', 'tax']);
  const idxAccCode = findHeaderIdx(['勘定科目コード', '科目コード', 'account_code', 'アカウントコード']);
  const idxSubCode = findHeaderIdx(['補助科目コード', 'サブ科目コード', 'subaccount_code', 'サブコード', '補助コード']);

  const accounts = new Map();
  const taxSet = new Set();
  let totalSubs = 0, accCodeCount = 0, subCodeCount = 0;

  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const acc = norm_(idxAccName >= 0 ? row[idxAccName] : '');
    if (!acc) continue;

    const sub = norm_(idxSubName >= 0 ? row[idxSubName] : '');
    const tax = norm_(idxTax >= 0 ? row[idxTax] : '');
    const aCode = norm_(idxAccCode >= 0 ? row[idxAccCode] : '');
    const sCode = norm_(idxSubCode >= 0 ? row[idxSubCode] : '');

    if (!accounts.has(acc)) {
      accounts.set(acc, { code: '', subs: new Map() });
    }
    const meta = accounts.get(acc);

    if (aCode) {
      meta.code = aCode;
      accCodeCount++;
    }

    if (sub) {
      if (!meta.subs.has(sub)) {
        meta.subs.set(sub, { code: '' });
      }
      if (sCode) {
        meta.subs.get(sub).code = sCode;
        subCodeCount++;
      }
      totalSubs++;
    }

    if (tax) {
      taxSet.add(tax);
    }
  }

  return { accounts, totalSubs, taxSet, accCodeCount, subCodeCount };
}

function normalizeHeaderName_(header) {
  return String(header || '')
    .replace(/[\u200B\uFEFF\u2060]/g, '') // ゼロ幅文字除去
    .replace(/[\u3000\s]/g, '') // 全角・半角スペース除去
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, function(s) { // 全角→半角
      return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
    })
    .toLowerCase();
}

function getAccountCode_(master, accName) {
  if (!accName) return '';
  const meta = master.accounts.get(accName);
  return meta && meta.code ? meta.code : '';
}

function getSubCode_(master, accName, subName) {
  if (!accName || !subName) return '';
  const meta = master.accounts.get(accName);
  if (!meta) return '';
  const sub = meta.subs.get(subName);
  return sub && sub.code ? sub.code : '';
}

/* ============================ 走査・収集 ============================ */
function collectUnprocessedFiles_(folder, summary, logSheet) {
  const out = [];
  
  try {
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
            if (!isProcessedPrefix_(rName)) {
              out.push({ file: resolved, originalName: rName });
              summary.collected++;
              summary.names.push(rName);
            }
          }
        } else {
          summary.files++;
          if (!isProcessedPrefix_(name)) {
            out.push({ file: f, originalName: name });
            summary.collected++;
            summary.names.push(name);
          }
        }
      } catch (e) {
        log_(logSheet, 'ERROR', `ファイル走査エラー: ${name} | ${e.message || e}`);
      }
    }

    if (CONFIG.RECURSIVE) {
      const folders = folder.getFolders();
      while (folders.hasNext()) {
        const sub = folders.next();
        const subName = sub.getName();
        
        // 処理済みフォルダは除外
        if ([CONFIG.DONE_SUBFOLDER_NAME, CONFIG.INVOICE_SALES_SUBFOLDER_NAME, CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME].includes(subName)) {
          continue;
        }
        
        summary.folders++;
        out.push(...collectUnprocessedFiles_(sub, summary, logSheet));
      }
    }
  } catch (e) {
    log_(logSheet, 'ERROR', `フォルダ走査エラー: ${folder.getName()} | ${e.message || e}`);
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
  } catch (_) {
    return null;
  }
}

function isProcessedPrefix_(nameRaw) {
  if (!nameRaw) return false;
  
  const name = String(nameRaw).replace(/^[\u200B\uFEFF\u2060\s]+/, '').trimStart();
  const prefixes = ['[処理済み]', '【処理済み】', '[processed]', '[ processed ]', '[済]', '[済] '];
  
  return prefixes.some(p => name.startsWith(p));
}

/* ============================ 29列ヘッダ ============================ */
function ensureJournalHeader29_(sheet) {
  const headers = [
    '取引日',
    '借方勘定科目', '勘定科目コード', '借方補助科目', '補助科目コード', '借方取引先',
    '工事コード', '消費税コード', '借方税区分', '借方インボイス', '借方金額(円)',
    '貸方勘定科目', '勘定科目コード', '貸方補助科目', '補助科目コード', '貸方取引先',
    '工事コード', '貸方税区分', '貸方インボイス', '貸方金額(円)',
    '摘要', 'メモ', '処理状態', 'エクスポート日時', 'エクスポートID',
    'メモ', '処理状態', 'エクスポート日時', 'エクスポートID'
  ];
  
  const lastRow = sheet.getLastRow();
  if (lastRow === 0) {
    sheet.appendRow(headers);
    return;
  }
  
  const currentHeader = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
  const needsUpdate = !currentHeader.every((val, idx) => String(val || '') === headers[idx]);
  
  if (needsUpdate) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

/* ============================ 工事コード関連 ============================ */
function extractConstructionCodeFromFolderNames_(fileId) {
  try {
    const file = DriveApp.getFileById(fileId);
    const parents = file.getParents();
    
    while (parents.hasNext()) {
      const parent = parents.next();
      const name = (parent.getName() || '').trim();
      const match = name.match(/^[\[\(]?([0-9A-Za-z\-]{2,})[\]\)]?[ 　]+/);
      if (match && match[1]) {
        return match[1];
      }
    }
  } catch (_) {}
  
  return '';
}

function isConstructionAccountName_(accName) {
  if (!accName) return false;
  return CONFIG.CONSTRUCTION_ACCOUNTS.some(key => accName.includes(key));
}

/* ============================ 振り分け関連 ============================ */
function normalizeMeta_(meta) {
  const m = meta || {};
  return {
    document_type: String(m.document_type || m.doc_type || '').trim(),
    invoice_type: String(m.invoice_type || '').trim(),
    issuer: String(m.issuer || '').trim(),
    addressee: String(m.addressee || '').trim()
  };
}

function hasOurName_(str) {
  if (!str) return false;
  return /悟大/.test(String(str));
}

function isInvoiceDoc_(docType, fileName) {
  if (docType === '請求書') return true;
  const name = String(fileName || '');
  return /請求|invoice/i.test(name);
}

function decideInvoiceFolder_(docType, invoiceType, issuer, addressee, fileName) {
  const isInvoice = isInvoiceDoc_(docType, fileName);
  if (!isInvoice) return null;
  
  if (invoiceType === '売上') return 'sales';
  if (invoiceType === '支払') return 'payables';
  if (hasOurName_(issuer)) return 'sales';
  if (hasOurName_(addressee)) return 'payables';
  
  return null;
}

/* ============================ インデックス・スキップ管理 ============================ */
function loadProcessedIndex_(ss) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  
  if (sh.getLastRow() === 0) {
    sh.appendRow(['run_id', 'file_id', 'file_url', 'content_hash', 'processed_at']);
    return { ids: new Set(), hashes: new Set() };
  }
  
  const lastRow = sh.getLastRow();
  const lastCol = Math.max(1, sh.getLastColumn());
  const header = sh.getRange(1, 1, 1, lastCol).getValues()[0];
  const lower = header.map(h => String(h || '').toLowerCase());
  
  const idxFileId = Math.max(lower.indexOf('file_id'), lower.indexOf('fileid'));
  const idxHash = lower.indexOf('content_hash');
  
  const ids = new Set();
  const hashes = new Set();
  
  if (lastRow >= 2) {
    const values = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();
    for (const row of values) {
      if (idxFileId >= 0 && row[idxFileId]) {
        ids.add(String(row[idxFileId]));
      }
      if (idxHash >= 0 && row[idxHash]) {
        hashes.add(String(row[idxHash]));
      }
    }
  }
  
  return { ids, hashes };
}

function writeProcessedIndex_(ss, data) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  
  if (sh.getLastRow() === 0) {
    sh.appendRow(['run_id', 'file_id', 'file_url', 'content_hash', 'processed_at']);
  }
  
  sh.appendRow([
    data.runId || '',
    data.fileId || '',
    data.fileUrl || '',
    data.hash || '',
    now_()
  ]);
}

function writeSkipped_(ss, data) {
  const sh = getOrCreateSheet_(ss, 'skipped');
  
  if (sh.getLastRow() === 0) {
    sh.appendRow(['日時', '理由', 'ファイル名', 'fileId', 'fileUrl', 'content_hash']);
  }
  
  sh.appendRow([
    now_(),
    data.reason || '',
    data.name || '',
    data.fileId || '',
    data.fileUrl || '',
    data.hash || ''
  ]);
}

function shouldDedupeSkip_(index, fileId, hash) {
  const mode = CONFIG.DEDUPE_MODE;
  
  switch (mode) {
    case 'off':
      return false;
    case 'id_only':
      return index.ids.has(fileId);
    case 'hash_only':
      return index.hashes.has(hash);
    default: // 'on'
      return index.ids.has(fileId) || index.hashes.has(hash);
  }
}

/* ============================ 実行サマリー・通知 ============================ */
function finalizeRun_(ss, runId, started, durations, success, skipped, error, chatWebhook) {
  const ended = new Date();
  const totalSeconds = (ended - started) / 1000.0;
  const avgDuration = durations.length ? durations.reduce((a, b) => a + b, 0) / durations.length : 0;
  const p95Duration = durations.length ? percentile_(durations, 0.95) : 0;
  
  // サマリー記録
  const summarySheet = getOrCreateSheet_(ss, 'run_summary');
  if (summarySheet.getLastRow() === 0) {
    summarySheet.appendRow([
      'run_id', 'start_time', 'end_time', 'total_files', 'success_count',
      'skipped_count', 'error_count', 'avg_duration_sec', 'p95_duration_sec', 'total_duration_sec'
    ]);
  }
  
  summarySheet.appendRow([
    runId, started, ended, success + skipped + error, success,
    skipped, error, avgDuration, p95Duration, totalSeconds
  ]);
  
  // アラート送信
  if (chatWebhook && (error > CONFIG.ALERT_FAIL_THRESHOLD || totalSeconds / 60 > CONFIG.ALERT_MAX_EXEC_MINUTES)) {
    try {
      UrlFetchApp.fetch(chatWebhook, {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify({
          text: `🚨 仕訳実行アラート\nrun_id: ${runId}\n期間: ${formatJST_(started)} - ${formatJST_(ended)}\n` +
                `結果: 成功${success} / スキップ${skipped} / 失敗${error}\n` +
                `処理時間: ${totalSeconds.toFixed(1)}秒 (平均: ${avgDuration.toFixed(1)}s)`
        }),
        muteHttpExceptions: true
      });
    } catch (_) {}
  }
}

/* ============================ デバッグ・ログ ============================ */
function debugAI_Struct_(data) {
  try {
    const ss = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
    const sh = getOrCreateSheet_(ss, 'debug_ai');
    if (sh.getLastRow() < 1) {
      sh.appendRow(['日時', 'http', 'cand', 'block', 'parts', 'head', 'hash', 'note']);
    }
    sh.appendRow([
      now_(), 
      data.httpCode || '', 
      data.candCount || '', 
      data.blockReason || '', 
      data.partsKinds || '', 
      (data.head || '').slice(0, CONFIG.DEBUG_SAVE_HEAD), 
      data.hash || '', 
      data.note || ''
    ]);
  } catch (_) {}
}

/* ============================ ユーティリティ ============================ */
function ensureAuxSheets_(ss) {
  const sheetNames = ['run_log', 'run_summary', 'processed_index', 'skipped', 'debug_ai'];
  sheetNames.forEach(name => getOrCreateSheet_(ss, name));
}

function getOrCreateRunLogSheet_(ss) {
  const name = 'run_log';
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    sh.appendRow(['日時', 'レベル', 'メッセージ']);
  }
  return sh;
}

function getOrCreateSheet_(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
  }
  return sheet;
}

function getOrCreateChildFolder_(parent, name) {
  const existing = parent.getFoldersByName(name);
  return existing.hasNext() ? existing.next() : parent.createFolder(name);
}

function isDriveAdvancedAvailable_() {
  try {
    return typeof Drive !== 'undefined' && Drive && Drive.Files && typeof Drive.Files.get === 'function';
  } catch (_) {
    return false;
  }
}

function buildProcessedName_(extracted, fallbackName) {
  let dateDot = '';
  if (extracted.date) {
    const match = extracted.date.match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/);
    if (match) {
      dateDot = `${match[1]}.${parseInt(match[2], 10)}.${parseInt(match[3], 10)}`;
    }
  }
  
  const amountPart = extracted.amount ? `.${extracted.amount}円` : '';
  const payeePart = (extracted.payee || '').replace(/\s+/g, '').slice(0, 20) || 
                   extractPayeeFromName_(fallbackName);
  
  return ['[済]', dateDot, amountPart, payeePart].filter(Boolean).join(' ');
}

function extractPayeeFromName_(name) {
  if (!name) return '';
  
  const base = String(name).replace(/\.[^.]+$/, '');
  const tokens = base.split(/[ _\-\(\)【】\[\]、，・.]/).filter(Boolean);
  
  return tokens.length ? tokens[tokens.length - 1].slice(0, 20) : '';
}

function now_() {
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

function log_(logSheet, level, message) {
  const timestamp = now_();
  Logger.log(`[${level}] ${timestamp} ${message}`);
  
  try {
    logSheet.appendRow([timestamp, level, message]);
  } catch (_) {}
}

function norm_(value) {
  return value == null ? '' : String(value).trim();
}

function stripCodeFence_(text) {
  if (!text) return text;
  return text
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/i, '')
    .trim();
}

function tryParseJsonChain_(text) {
  if (!text) return null;
  
  // 直接パース試行
  try {
    return JSON.parse(text);
  } catch (_) {}
  
  // JSONブロック抽出してパース
  const recovered = recoverJsonFromText_(text);
  if (recovered) {
    try {
      return JSON.parse(recovered);
    } catch (_) {}
  }
  
  // 軽微な修正を加えてパース
  try {
    let fixed = String(recovered || text);
    fixed = fixed.replace(/,\s*([}\]])/g, '$1'); // 末尾カンマ除去
    fixed = fixed.replace(/[^\S\r\n]+$/g, ''); // 末尾空白除去
    return JSON.parse(fixed);
  } catch (_) {}
  
  return null;
}

function recoverJsonFromText_(text) {
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  
  if (start === -1 || end === -1 || end <= start) {
    return '';
  }
  
  return text.slice(start, end + 1);
}

function sha256Hex_(bytes) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes);
  return digest.map(byte => ('0' + (byte & 0xFF).toString(16)).slice(-2)).join('');
}

function createUUID_() {
  return Utilities.getUuid().replace(/-/g, '');
}

function toSlashDate_(dateStr) {
  if (!dateStr) return '';
  
  const match = String(dateStr).match(/(\d{4})[年\/\-\.](\d{1,2})[月\/\-\.](\d{1,2})/);
  if (match) {
    return `${match[1]}/${('0' + parseInt(match[2], 10)).slice(-2)}/${('0' + parseInt(match[3], 10)).slice(-2)}`;
  }
  
  return String(dateStr);
}

function getVal_(obj, keys) {
  for (const key of keys) {
    const value = obj[key];
    if (value !== undefined && value !== null && String(value) !== '') {
      return value;
    }
  }
  return '';
}

function withUrlInNote_(note, url) {
  if (!url) return note || '';
  
  const noteStr = String(note || '').trim();
  if (noteStr.includes(url)) return noteStr;
  
  return noteStr ? `${noteStr} ${url}` : url;
}

function percentile_(arr, p) {
  if (!arr.length) return 0;
  
  const sorted = [...arr].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.floor((sorted.length - 1) * p)));
  
  return sorted[index];
}

function formatJST_(date) {
  return Utilities.formatDate(new Date(date), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

function safePreview_(value, max = 400) {
  const str = typeof value === 'string' ? value : JSON.stringify(value);
  return str.length > max ? str.slice(0, max) + ' …(省略)…' : str;
}

/* ============================ デバッグ用単体テスト ============================ */
function __testSingleFile() {
  try {
    Logger.log('=== 単体テスト開始 ===');
    
    const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
    if (!apiKey) throw new Error('GEMINI_API_KEY が未設定');
    
    const ssJournal = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
    const logSheet = getOrCreateRunLogSheet_(ssJournal);
    ensureAuxSheets_(ssJournal);
    
    // マスタ読み込みテスト
    const master = readAccountMasterRobust_();
    Logger.log(`マスタ読み込み: 勘定科目=${master.accounts.size} 勘定科目コード=${master.accCodeCount}`);
    
    // 候補ファイル検索
    const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
    let testFile = null, originalName = '';
    
    const files = root.getFiles();
    while (files.hasNext()) {
      const f = files.next();
      const name = f.getName();
      if (f.getMimeType() === 'application/vnd.google-apps.shortcut') continue;
      if (isProcessedPrefix_(name)) continue;
      
      testFile = f;
      originalName = name;
      break;
    }
    
    if (!testFile) {
      Logger.log('テスト対象ファイルが見つかりません');
      return;
    }
    
    Logger.log(`テスト対象: ${originalName}`);
    
    // AI処理テスト
    const blob = testFile.getBlob();
    const parsed = askGeminiOneShotRobust_(
      buildWorkingPrompt_(master, { fileUrl: testFile.getUrl() }), 
      blob, 
      blob.getContentType(), 
      apiKey, 
      logSheet
    );
    
    if (!parsed.obj) {
      Logger.log('AI処理失敗');
      return;
    }
    
    Logger.log(`AI処理成功`);
    Logger.log(`応答内容: ${JSON.stringify(parsed.obj, null, 2).slice(0, 500)}...`);
    
    Logger.log('=== 単体テスト完了 ===');
    
  } catch (error) {
    Logger.log(`テストエラー: ${error.message || error}`);
    throw error;
  }
}
