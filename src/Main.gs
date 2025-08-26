/*******************************************************
 * コード.gs（安定版・一括置換）
 * 目的
 *  - マスタは実行ごとに1回だけ読み込み
 *  - 勘定科目/補助科目は「完全一致 → 片方向部分一致 → 双方向部分一致 → 距離（近似）」で解決
 *  - 勘定科目コード/補助科目コードを確実に出力（見出しゆらぎ：勘定科目コード/科目コード に対応）
 *  - 29列の出力順を固定（列ズレ防止）
 *  - Gemini応答が空でも再試行＆最低限(必須3点)抽出にフォールバック
 *  - ファイル振分け（請求書のみ：売上請求書/支払請求書/未判定は完了）
 *  - ログは最小限（processed_index / skipped のみ）
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

  // === 走査 ===
  RECURSIVE: true,
  INCLUDE_SHORTCUT_TARGETS: true,

  // === フォルダ ===
  DONE_SUBFOLDER_NAME: '完了',
  INVOICE_SALES_SUBFOLDER_NAME: '売上請求書',
  INVOICE_PAYABLES_SUBFOLDER_NAME: '支払請求書',

  // === 必須値 ===
  REQUIRED_FIELDS: ['日付','金額','取引先'],

  // 工事系科目の判定キー（部分一致）
  CONSTRUCTION_ACCOUNTS: ['完成工事高','未成工事支出金','完成工事未収入金','外注費','工事仮勘定'],

  // 重複制御
  DEDUPE_MODE: 'on', // 'on' | 'off' | 'id_only' | 'hash_only'
};

/* ============================ エントリ ============================ */
function processNewInvoices() {
  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) throw new Error('GEMINI_API_KEY が未設定です。');

  const ssJournal = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
  const shJournal = getOrCreateSheet_(ssJournal, CONFIG.JOURNAL_SHEET_NAME);
  ensureJournalHeader29_(shJournal);

  const index = loadProcessedIndex_(ssJournal); // 空安全版

  const master = readAccountMaster_Map_();      // 1回だけ読込
  const searchIndex = buildSearchIndex_(master);// 近似検索用の配列

  const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
  const doneFolder = getOrCreateChildFolder_(root, CONFIG.DONE_SUBFOLDER_NAME);
  const salesFolder = getOrCreateChildFolder_(root, CONFIG.INVOICE_SALES_SUBFOLDER_NAME);
  const payFolder   = getOrCreateChildFolder_(root, CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME);

  const targets = collectUnprocessedFiles_(root);
  if (!targets.length) return;

  for (const { file, originalName } of targets) {
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    let hash = '';
    try {
      const blob = file.getBlob();
      const mimeType = blob.getContentType();
      const sizeBytes = blob.getBytes().length;
      if (sizeBytes > CONFIG.MAX_FILE_BYTES) {
        writeSkipped_(ssJournal, { reason:`サイズ上限超過: ${sizeBytes} bytes`, fileId, fileUrl, hash:'', name: originalName });
        continue;
      }

      // 重複制御
      hash = sha256Hex_(blob.getBytes());
      if (shouldDedupeSkip_(index, fileId, hash)) {
        writeSkipped_(ssJournal, { reason:`重複スキップ(${CONFIG.DEDUPE_MODE})`, fileId, fileUrl, hash, name: originalName });
        continue;
      }

      const folderCode = extractConstructionCodeFromFolderNames_(fileId);

      // === AI呼び出し：本命 → サブ → 最小抽出 ===
      let parsed = askGeminiOneShotRobust_(buildPrompt_Main_(master, { fileUrl }), blob, mimeType, apiKey);
      if (!parsed.obj) parsed = askGeminiOneShotRobust_(buildPrompt_Sub_(master, { fileUrl }), blob, mimeType, apiKey);
      if (!parsed.obj) parsed = askGeminiOneShotRobust_(buildPrompt_Min_({ fileUrl }), blob, mimeType, apiKey);

      if (!parsed.obj || typeof parsed.obj !== 'object') {
        writeSkipped_(ssJournal, { reason:`AI応答の解析に失敗(rawLen=${parsed.rawLen||0}, cand=${parsed.candCount||0}, block=${parsed.blockReason||'-'})`, fileId, fileUrl, hash, name: originalName });
        continue;
      }
      const obj = parsed.obj;

      // === 必須抽出（フォールバックを多段）===
      const meta = normalizeMeta_(obj.__meta || {});
      let dateSlash = toSlashDate_(getVal_(obj, ['日付']));
      if (!dateSlash) dateSlash = guessDateFromFileName_(originalName);

      let payee = (getVal_(obj, ['借方取引先','貸方取引先'])||'').toString().trim();
      if (!payee) {
        // 自社名は除外して推定
        const issuer = (meta.issuer||'').trim(), addressee=(meta.addressee||'').trim();
        payee = hasOurName_(issuer) ? addressee : issuer;
      }
      if (!payee) payee = extractPayeeFromName_(originalName);

      let amount = (getVal_(obj, ['借方金額(円)','貸方金額(円)'])||'').toString().replace(/[^\d]/g,'');
      if (!amount) amount = guessAmountFromFileName_(originalName);

      // 必須ゲート
      const missing = [];
      if (!dateSlash) missing.push('日付');
      if (!amount)    missing.push('金額');
      if (!payee)     missing.push('取引先');
      if (missing.length) {
        writeSkipped_(ssJournal, { reason:`必須欠落: ${missing.join(', ')}`, fileId, fileUrl, hash, name: originalName });
        continue;
      }

      // 借貸（名称）
      let dAcc = norm_(getVal_(obj, ['借方科目','借方勘定科目']));
      let dSub = norm_(getVal_(obj, ['借方補助科目']));
      let cAcc = norm_(getVal_(obj, ['貸方科目','貸方勘定科目']));
      let cSub = norm_(getVal_(obj, ['貸方補助科目']));

      // 工事コード（JSON > フォルダ）
      let constructionCode = norm_(getVal_(obj, ['工事コード'])) || folderCode;

      // 税系
      const taxCode = norm_(getVal_(obj, ['消費税コード'])); // 共通
      const dTaxCat = norm_(getVal_(obj, ['借方税区分']));
      const cTaxCat = norm_(getVal_(obj, ['貸方税区分']));
      const dInv = norm_(getVal_(obj, ['借方インボイス','借方インボイス番号']));
      const cInv = norm_(getVal_(obj, ['貸方インボイス','貸方インボイス番号']));
      const dAmt = norm_(getVal_(obj, ['借方金額(円)']));
      const cAmt = norm_(getVal_(obj, ['貸方金額(円)']));

      // 工事フォルダ強制（補助＝工事コード）
      ({ dSub, cSub } = forceConstructionSubIfNeeded_(dAcc, cAcc, dSub, cSub, constructionCode));

      // === 勘定科目/補助科目の解決（近似許容）===
      const { accName: dAccFix, accCode: dAccCode } = resolveAccount_(master, searchIndex, dAcc);
      const { subName: dSubFix, subCode: dSubCode } = resolveSub_(master, searchIndex, dAccFix, dSub, constructionCode);

      const { accName: cAccFix, accCode: cAccCode } = resolveAccount_(master, searchIndex, cAcc);
      const { subName: cSubFix, subCode: cSubCode } = resolveSub_(master, searchIndex, cAccFix, cSub, constructionCode);

      // 摘要（URL付与）
      let note = (getVal_(obj, ['摘要']) || '').toString().trim();
      if (!note || !note.includes(payee)) note = `${payee} ${note}`.trim();
      note = withUrlInNote_(note, fileUrl);

      // === 29列 出力 ===
      const row = [
        dateSlash,
        dAccFix, dAccCode, dSubFix, dSubCode, norm_(getVal_(obj, ['借方取引先'])), constructionCode || '', taxCode || '',
        dTaxCat || '', dInv || '', dAmt || '',
        cAccFix, cAccCode, cSubFix, cSubCode, norm_(getVal_(obj, ['貸方取引先'])), constructionCode || '',
        cTaxCat || '', cInv || '', cAmt || '',
        note || '', fileUrl || '', '', '', '', fileUrl || '', '', '', ''
      ];
      shJournal.appendRow(row);

      // ファイル振分け
      const which = decideInvoiceFolder_(meta.document_type, meta.invoice_type, meta.issuer, meta.addressee, originalName);
      const newName = buildProcessedName_({ date: dateSlash, amount, payee }, file.getName());
      file.setName(newName);
      if (which === 'sales')      file.moveTo(salesFolder);
      else if (which === 'payables') file.moveTo(payFolder);
      else                        file.moveTo(doneFolder);

      // インデックス登録
      writeProcessedIndex_(ssJournal, { runId:createUUID_(), fileId, fileUrl, hash });
    } catch (e) {
      writeSkipped_(ssJournal, { reason:`処理例外: ${e && e.message ? e.message : e}`, fileId, fileUrl, hash, name: originalName });
      // 続行
    }
  }
}

/* ============================ プロンプト ============================ */
function buildPrompt_Main_(master, ctx) {
  const accounts = Array.from(master.accounts.keys());
  const subMapLines = [];
  for (const [acc, meta] of master.accounts.entries()) {
    const subs = Array.from(meta.subs.keys());
    if (subs.length) subMapLines.push(`- ${acc}: ${subs.join(' | ')}`);
  }
  const taxList = Array.from(master.taxSet.values());
  const fileUrl = String(ctx.fileUrl || '');
  return `
あなたは会計仕訳アシスタントです。添付（PDF/JPG/PNG 等）の商業文書を解析し、以下の日本語キーの**厳密JSON**を1個だけ返してください（説明・余分な文字・コードフェンスは禁止）。

【日付】和暦/略式を YYYY/MM/DD に正規化（不明なら空）
【数値】金額は整数（,や¥等は除去）
【科目】下記の【登録済みマスタ】の科目/補助/税区分から選ぶ（無ければ空）
【請求書の種別】"__meta" に document_type/issuer/addressee/invoice_type を返す（invoice_type は "売上" | "支払" | ""）

【出力（キー名と順序固定／追加禁止）】
{
  "日付": "YYYY/MM/DD",
  "借方科目": "", "借方補助科目": "", "借方取引先": "",
  "消費税コード": "",
  "借方税区分": "", "借方インボイス": "", "借方金額(円)": 0,
  "貸方科目": "", "貸方補助科目": "", "貸方取引先": "",
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
${accounts.map(a => '  - ' + a).join('\n') || '(なし)'}
- 補助科目（科目ごと）:
${subMapLines.join('\n') || '(なし)'}
- 税区分:
${taxList.join(' | ') || '(未定義)'}

注意：
- ファイルURLはシステム側で保持（URL: ${fileUrl}）。出力JSONに含めない。
`.trim();
}

function buildPrompt_Sub_(master, ctx) {
  const accounts = Array.from(master.accounts.keys());
  const taxList = Array.from(master.taxSet.values());
  const fileUrl = String(ctx.fileUrl || '');
  return `
以下の日本語キーの**厳密JSON**を1個だけ返してください。説明やコードフェンスは一切禁止。

{
  "日付": "YYYY/MM/DD",
  "借方科目": "", "借方補助科目": "", "借方取引先": "",
  "消費税コード": "", "借方税区分": "", "借方インボイス": "", "借方金額(円)": 0,
  "貸方科目": "", "貸方補助科目": "", "貸方取引先": "",
  "貸方税区分": "", "貸方インボイス": "", "貸方金額(円)": 0,
  "工事コード": "",
  "摘要": "",
  "__meta": { "document_type":"", "issuer":"", "addressee":"", "invoice_type":"" }
}

- 科目は次から最も近いものを選択（無ければ空）:
${accounts.join(' | ') || '(なし)'}
- 税区分は次から（無ければ空）:
${taxList.join(' | ') || '(未定義)'}
- URL: ${fileUrl}（出力へ含めない）
`.trim();
}

function buildPrompt_Min_(ctx) {
  const fileUrl = String(ctx.fileUrl || '');
  return `
厳密JSONのみを返してください。キーは固定です。説明やコードフェンスは禁止。

{
  "日付": "YYYY/MM/DD",
  "借方科目": "", "借方補助科目": "", "借方取引先": "",
  "消費税コード": "", "借方税区分": "", "借方インボイス": "", "借方金額(円)": 0,
  "貸方科目": "", "貸方補助科目": "", "貸方取引先": "",
  "貸方税区分": "", "貸方インボイス": "", "貸方金額(円)": 0,
  "工事コード": "",
  "摘要": "",
  "__meta": { "document_type":"", "issuer":"", "addressee":"", "invoice_type":"" }
}

- 出力できない項目は空文字または0
- URL: ${fileUrl}（出力へ含めない）
`.trim();
}

/* ============================ AI呼び出し ============================ */
function askGeminiOneShotRobust_(prompt, blob, mimeType, apiKey) {
  const tries = [
    { p: prompt, t: 0.0 },
    { p: prompt + '\n\n出力は**厳密JSON1個**のみ。説明・余計な文字・コードフェンス禁止。', t: 0.0 },
    { p: prompt + '\n\nJSONのみを返してください。', t: 0.0 },
  ];
  let last = { obj:null, httpCode:0, candCount:0, rawLen:0, blockReason:'', partsKinds:'' };
  for (let i=0;i<tries.length;i++){
    const r = callGemini_(tries[i].p, blob, mimeType, apiKey, tries[i].t);
    const parsed = parseGeminiResponse_(r);
    last = parsed;
    if (parsed.obj) return parsed;
    Utilities.sleep(300*(i+1));
  }
  return last;
}
function callGemini_(prompt, blob, mimeType, apiKey, temperature) {
  const url = CONFIG.GEMINI_ENDPOINT + encodeURIComponent(CONFIG.GEMINI_MODEL) + ':generateContent?key=' + encodeURIComponent(apiKey);
  const body = {
    contents: [{ role:'user', parts:[
      { text: prompt },
      { inline_data: { mime_type: mimeType, data: Utilities.base64Encode(blob.getBytes()) } }
    ]}],
    generationConfig: { temperature: temperature, maxOutputTokens: 4096, responseMimeType: 'application/json' }
  };
  const res = UrlFetchApp.fetch(url, { method:'post', contentType:'application/json', payload: JSON.stringify(body), muteHttpExceptions:true });
  return { code: res.getResponseCode(), text: res.getContentText() };
}
function parseGeminiResponse_(res) {
  const httpCode = res.code;
  let candCount = 0, blockReason = '', partsKinds = '';
  if (httpCode >= 300) return { obj:null, httpCode, candCount:0, rawLen:0, blockReason, partsKinds };
  let data; try { data = JSON.parse(res.text); } catch (_){ return { obj:null, httpCode, candCount:0, rawLen:0, blockReason, partsKinds }; }
  try { blockReason = (data.promptFeedback && (data.promptFeedback.blockReason || data.promptFeedback.block_reason)) || ''; } catch(_){}
  try {
    const cands = data.candidates || [];
    candCount = cands.length;
    const kinds = new Set();
    for (const c of cands) {
      const parts = (c && c.content && c.content.parts) || [];
      for (const p of parts) for (const k of Object.keys(p || {})) kinds.add(k);
    }
    partsKinds = Array.from(kinds).join(',');
  } catch(_){}
  const rawText = pickTextFromGeminiDeep_(data) || '';
  const cleaned = stripCodeFence_(rawText);
  const obj = tryParseJsonChain_(cleaned);
  return { obj, httpCode, candCount, rawLen: cleaned.length, blockReason, partsKinds };
}
function pickTextFromGeminiDeep_(resp) {
  try {
    const cands = resp.candidates || [];
    for (const c of cands) {
      const parts = (c && c.content && c.content.parts) || [];
      for (const p of parts) if (typeof p.text === 'string' && p.text.trim()) return p.text;
      for (const p of parts) {
        if (typeof p.functionCall === 'object') { const s = JSON.stringify(p.functionCall); if (s) return s; }
        if (typeof p.executable_code === 'string' && p.executable_code.trim()) return p.executable_code;
        if (typeof p.code === 'string' && p.code.trim()) return p.code;
      }
    }
  } catch(_){}
  return '';
}

/* ============================ マスタ読み込み＆近似解決 ============================ */
/**
 * account_master 見出し候補：
 *  勘定科目 / 補助科目 / 税区分 / 勘定科目コード / 補助科目コード / 取引先コード / 工事コード / 備考
 */
function readAccountMaster_Map_() {
  const ss = SpreadsheetApp.openById(CONFIG.ACCOUNT_MASTER_SSID);
  const sh = ss.getSheetByName(CONFIG.ACCOUNT_MASTER_SHEET);
  if (!sh) throw new Error('勘定科目シートが見つかりません: ' + CONFIG.ACCOUNT_MASTER_SHEET);

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 1 || lastCol < 1) {
    return { accounts:new Map(), totalSubs:0, taxSet:new Set(), accCodeCount:0, subCodeCount:0 };
  }

  const values = sh.getRange(1,1,lastRow,lastCol).getValues();
  const header = values[0].map(v => String(v||'').trim());
  const H = (cands) => header.findIndex(h => cands.includes(h));

  const idxAccName = H(['勘定科目','科目','account']);
  const idxSubName = H(['補助科目','サブ科目','subaccount','サブ']);
  const idxTax     = H(['税区分','消費税区分','税','tax']);
  const idxAccCode = H(['勘定科目コード','科目コード','account_code']); // 「MJS科目コード」は不使用
  const idxSubCode = H(['補助科目コード','サブ科目コード','subaccount_code','サブコード']);

  const accounts = new Map(); // accName => { code:'', subs: Map(subName=>{code:''}) }
  const taxSet = new Set();
  let totalSubs=0, accCodeCount=0, subCodeCount=0;

  for (let r=1;r<values.length;r++){
    const row = values[r];
    const acc = norm_(idxAccName>=0?row[idxAccName]:''); if(!acc) continue;
    const sub = norm_(idxSubName>=0?row[idxSubName]:'');
    const tax = norm_(idxTax    >=0?row[idxTax]    :'');
    const aCd = norm_(idxAccCode>=0?row[idxAccCode]:'');
    const sCd = norm_(idxSubCode>=0?row[idxSubCode]:'');
    if (!accounts.has(acc)) accounts.set(acc,{ code:'', subs:new Map() });
    const meta = accounts.get(acc);
    if (aCd){ meta.code=aCd; accCodeCount++; }
    if (sub){
      if(!meta.subs.has(sub)) meta.subs.set(sub,{ code:'' });
      if (sCd){ meta.subs.get(sub).code=sCd; subCodeCount++; }
      totalSubs++;
    }
    if (tax) taxSet.add(tax);
  }
  return { accounts, totalSubs, taxSet, accCodeCount, subCodeCount };
}

/** 近似検索用インデックス */
function buildSearchIndex_(master){
  const accNames = Array.from(master.accounts.keys());
  const accKeys  = accNames.map(n => normalizeKey_(n));
  const subDict  = new Map(); // accName => { names:[], keys:[] }
  for (const [acc, meta] of master.accounts.entries()){
    const subs = Array.from(meta.subs.keys());
    subDict.set(acc, { names: subs, keys: subs.map(s => normalizeKey_(s)) });
  }
  return { accNames, accKeys, subDict };
}

/** 勘定科目の解決（完全一致→部分一致→距離）。未決なら元名を返しコードは空でOK。 */
function resolveAccount_(master, searchIndex, aiAcc){
  const a = norm_(aiAcc);
  if (!a) return { accName:'', accCode:'' };
  const m = master.accounts;
  if (m.has(a)) return { accName:a, accCode:(m.get(a).code||'') };

  const key = normalizeKey_(a);
  // 片方向部分一致（候補が ai を含む / ai が候補を含む）
  let bestName = '';
  for (let i=0;i<searchIndex.accNames.length;i++){
    const cand = searchIndex.accNames[i];
    const ck = searchIndex.accKeys[i];
    if (ck.includes(key) || key.includes(ck)) {
      if (!bestName || cand.length > bestName.length) bestName = cand; // 長い方を優先
    }
  }
  if (bestName) return { accName: bestName, accCode: (m.get(bestName).code||'') };

  // 距離（レーベンシュタイン類似度）
  let bestScore = 0, best = '';
  for (let i=0;i<searchIndex.accNames.length;i++){
    const cand = searchIndex.accNames[i];
    const score = similarity_(key, searchIndex.accKeys[i]);
    if (score > bestScore){ bestScore = score; best = cand; }
  }
  if (best && bestScore >= 0.75) return { accName: best, accCode: (m.get(best).code||'') };

  return { accName:a, accCode:'' }; // 未決（名称は残す／コード空）
}

/** 補助科目の解決（上と同様）。補助が空でも許容。工事コードはコードとして採用可。 */
function resolveSub_(master, searchIndex, accName, aiSub, constructionCode){
  const sub = norm_(aiSub);
  if (!accName) {
    // 補助のみ提示され、コードっぽければ採用
    if (!sub && constructionCode && /^[0-9A-Za-z\-]{2,}$/.test(constructionCode)) {
      return { subName: constructionCode, subCode: constructionCode };
    }
    if (/^[0-9A-Za-z\-]{2,}$/.test(sub)) return { subName: sub, subCode: sub };
    return { subName: sub, subCode: '' };
  }

  const meta = master.accounts.get(accName);
  if (!meta) {
    if (!sub && constructionCode && /^[0-9A-Za-z\-]{2,}$/.test(constructionCode)) {
      return { subName: constructionCode, subCode: constructionCode };
    }
    if (/^[0-9A-Za-z\-]{2,}$/.test(sub)) return { subName: sub, subCode: sub };
    return { subName: sub, subCode: '' };
  }

  // 工事コード強制
  if (constructionCode && isConstructionAccountName_(accName)) {
    const code = /^[0-9A-Za-z\-]{2,}$/.test(constructionCode) ? constructionCode : '';
    return { subName: constructionCode, subCode: code };
  }

  if (!sub) return { subName:'', subCode:'' };

  // 完全一致
  if (meta.subs.has(sub)) return { subName: sub, subCode: (meta.subs.get(sub).code||'') };

  const dict = searchIndex.subDict.get(accName) || { names:[], keys:[] };
  const key = normalizeKey_(sub);

  // 部分一致（両方向）
  let bestName = '';
  for (let i=0;i<dict.names.length;i++){
    const cand = dict.names[i], ck = dict.keys[i];
    if (ck.includes(key) || key.includes(ck)) {
      if (!bestName || cand.length > bestName.length) bestName = cand;
    }
  }
  if (bestName) return { subName: bestName, subCode: (meta.subs.get(bestName).code||'') };

  // 距離
  let bestScore=0, best='';
  for (let i=0;i<dict.names.length;i++){
    const cand = dict.names[i];
    const score = similarity_(key, dict.keys[i]);
    if (score>bestScore){ bestScore=score; best=cand; }
  }
  if (best && bestScore>=0.75) return { subName: best, subCode:(meta.subs.get(best).code||'') };

  // 未決：コードらしければそれを採用
  if (/^[0-9A-Za-z\-]{2,}$/.test(sub)) return { subName: sub, subCode: sub };
  return { subName: sub, subCode: '' };
}

/* ============================ 走査 ============================ */
function collectUnprocessedFiles_(folder){
  const out = [];
  const files = folder.getFiles();
  while (files.hasNext()){
    const f = files.next();
    const name = f.getName();
    const mime = f.getMimeType();
    if (mime === 'application/vnd.google-apps.shortcut') {
      const resolved = resolveShortcutTarget_(f);
      if (resolved && !isProcessedPrefix_(resolved.getName())) {
        out.push({ file: resolved, originalName: resolved.getName() });
      }
    } else {
      if (!isProcessedPrefix_(name)) out.push({ file:f, originalName:name });
    }
  }
  if (CONFIG.RECURSIVE){
    const folders = folder.getFolders();
    while (folders.hasNext()){
      const sub = folders.next();
      const n = sub.getName();
      if (n === CONFIG.DONE_SUBFOLDER_NAME) continue;
      if (n === CONFIG.INVOICE_SALES_SUBFOLDER_NAME) continue;
      if (n === CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME) continue;
      out.push(...collectUnprocessedFiles_(sub));
    }
  }
  return out;
}
function resolveShortcutTarget_(shortcutFile) {
  if (!CONFIG.INCLUDE_SHORTCUT_TARGETS || !isDriveAdvancedAvailable_()) return null;
  try {
    const meta = Drive.Files.get(shortcutFile.getId(), { fields:'id,shortcutDetails' });
    const targetId = meta && meta.shortcutDetails && meta.shortcutDetails.targetId;
    if (!targetId) return null;
    return DriveApp.getFileById(targetId);
  } catch(_){ return null; }
}
function isProcessedPrefix_(nameRaw){
  if (!nameRaw) return false;
  const name = String(nameRaw).replace(/^[\u200B\uFEFF\u2060\s]+/,'').trimStart();
  return ['[処理済み]','【処理済み】','[processed]','[ processed ]','[済]','[済] '].some(p => name.startsWith(p));
}

/* ============================ 出力（29列） ============================ */
function ensureJournalHeader29_(sheet){
  const headers = [
    '取引日',
    '借方勘定科目','勘定科目コード','借方補助科目','補助科目コード','借方取引先','工事コード','消費税コード','借方税区分','借方インボイス','借方金額(円)',
    '貸方勘定科目','勘定科目コード','貸方補助科目','補助科目コード','貸方取引先','工事コード','貸方税区分','貸方インボイス','貸方金額(円)',
    '摘要','メモ','処理状態','エクスポート日時','エクスポートID','メモ','処理状態','エクスポート日時','エクスポートID'
  ];
  const lastRow = sheet.getLastRow();
  if (lastRow === 0) { sheet.appendRow(headers); return; }
  const width = headers.length;
  const cur = sheet.getRange(1,1,1,width).getValues()[0];
  const same = cur.length === width && cur.every((v,i)=>String(v||'')===headers[i]);
  if (!same) sheet.getRange(1,1,1,width).setValues([headers]);
}

/* ============================ インデックス ============================ */
function loadProcessedIndex_(ss){
  const sh = getOrCreateSheet_(ss, 'processed_index');
  const lastCol = Math.max(1, sh.getLastColumn());
  let lastRow = sh.getLastRow();
  if (lastRow === 0) { sh.appendRow(['run_id','file_id','file_url','content_hash','processed_at']); lastRow = 1; }

  const header = sh.getRange(1,1,1,lastCol).getValues()[0].map(v=>String(v||''));
  const lower  = header.map(h=>h.toLowerCase());
  const idxFileId = (lower.indexOf('file_id')>=0) ? lower.indexOf('file_id') : lower.indexOf('fileid');
  const idxHash   = lower.indexOf('content_hash');

  const ids=new Set(), hashes=new Set();
  if (lastRow >= 2){
    const vals = sh.getRange(2,1,lastRow-1,lastCol).getValues();
    for (const r of vals){
      if (idxFileId>=0 && r[idxFileId]) ids.add(String(r[idxFileId]));
      if (idxHash  >=0 && r[idxHash])   hashes.add(String(r[idxHash]));
    }
  }
  return { ids, hashes };
}
function writeProcessedIndex_(ss, row){
  const sh = getOrCreateSheet_(ss, 'processed_index');
  if (sh.getLastRow() === 0) sh.appendRow(['run_id','file_id','file_url','content_hash','processed_at']);
  sh.appendRow([row.runId||'', row.fileId||'', row.fileUrl||'', row.hash||'', now_()]);
}
function writeSkipped_(ss, o){
  const sh = getOrCreateSheet_(ss, 'skipped');
  if (sh.getLastRow() === 0) sh.appendRow(['日時','理由','ファイル名','fileId','fileUrl','content_hash']);
  sh.appendRow([now_(), o.reason||'', o.name||'', o.fileId||'', o.fileUrl||'', o.hash||'']);
}
function shouldDedupeSkip_(index, fileId, hash){
  const mode = CONFIG.DEDUPE_MODE;
  if (mode === 'off') return false;
  if (mode === 'id_only')   return index.ids.has(fileId);
  if (mode === 'hash_only') return index.hashes.has(hash);
  return index.ids.has(fileId) || index.hashes.has(hash);
}

/* ============================ 振分け判定 ============================ */
function normalizeMeta_(m){
  const dt = (m.document_type||m.doc_type||'').toString().trim();
  const it = (m.invoice_type||'').toString().trim();
  const iss = (m.issuer||'').toString().trim();
  const adr = (m.addressee||'').toString().trim();
  return { document_type:dt, invoice_type:it, issuer:iss, addressee:adr };
}
function hasOurName_(s){ if (!s) return false; return /悟大/.test(String(s)); }
function isInvoiceDoc_(docType, fileName){
  if (docType === '請求書') return true;
  const n = (fileName||'').toString();
  return /請求|invoice/i.test(n);
}
function decideInvoiceFolder_(docType, invoiceType, issuer, addressee, fileName){
  const isInv = isInvoiceDoc_(docType, fileName);
  if (!isInv) return null;
  if (invoiceType === '売上') return 'sales';
  if (invoiceType === '支払') return 'payables';
  if (hasOurName_(issuer)) return 'sales';
  if (hasOurName_(addressee)) return 'payables';
  return null;
}

/* ============================ 工事コードルール ============================ */
function extractConstructionCodeFromFolderNames_(fileId){
  try {
    const file = DriveApp.getFileById(fileId);
    const parents = file.getParents();
    while (parents.hasNext()){
      const p = parents.next();
      const name = (p.getName()||'').trim();
      const m = name.match(/^[\[\(]?([0-9A-Za-z\-]{2,})[\]\)]?[ 　]+/);
      if (m && m[1]) return m[1];
    }
  } catch(_){}
  return '';
}
function isConstructionAccountName_(accName){
  if (!accName) return false;
  return CONFIG.CONSTRUCTION_ACCOUNTS.some(k => accName.indexOf(k) !== -1);
}
function forceConstructionSubIfNeeded_(dAcc, cAcc, dSub, cSub, code){
  if (code){
    if (isConstructionAccountName_(dAcc)) dSub = code;
    if (isConstructionAccountName_(cAcc)) cSub = code;
  }
  return { dSub, cSub };
}

/* ============================ ユーティリティ ============================ */
function getOrCreateSheet_(ss, name){ let sh = ss.getSheetByName(name); if (!sh) sh = ss.insertSheet(name); return sh; }
function getOrCreateChildFolder_(parent, name){ const it=parent.getFoldersByName(name); return it.hasNext()? it.next(): parent.createFolder(name); }
function isDriveAdvancedAvailable_(){ try{ return typeof Drive!=='undefined' && Drive && Drive.Files && typeof Drive.Files.get==='function'; }catch(_){ return false; } }

function now_(){ return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss'); }
function norm_(v){ return (v==null)?'':String(v).trim(); }
function stripCodeFence_(s){ if(!s) return s; return s.replace(/^```json\s*/i,'').replace(/^```\s*/i,'').replace(/```$/i,'').trim(); }
function tryParseJsonChain_(s){
  if(!s) return null;
  try{ return JSON.parse(s);}catch(_){}
  const r = recoverJsonFromText_(s);
  if (r){ try{ return JSON.parse(r);}catch(_){} }
  try{
    let t = String(r||s);
    t = t.replace(/,\s*([}\]])/g,'$1');
    t = t.replace(/[^\S\r\n]+$/g,'');
    return JSON.parse(t);
  }catch(_){}
  return null;
}
function recoverJsonFromText_(s){ const a=s.indexOf('{'), b=s.lastIndexOf('}'); if(a===-1||b===-1||b<=a) return ''; return s.slice(a,b+1); }

function sha256Hex_(bytes){ const dig = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes); return dig.map(b=>('0'+(b & 0xFF).toString(16)).slice(-2)).join(''); }
function createUUID_(){ return Utilities.getUuid().replace(/-/g,''); }

function withUrlInNote_(note, url){
  if (!url) return note||'';
  const s=(note||'').trim();
  if (s.includes(url)) return s;
  return s? `${s} ${url}` : url;
}
function buildProcessedName_({ date, amount, payee }, fallbackName){
  let dateDot='';
  if (date){ const m = date.match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/); if (m) dateDot = `${m[1]}.${parseInt(m[2],10)}.${parseInt(m[3],10)}`; }
  const amtDigits = (amount!=null && amount!=='') ? String(amount).replace(/[^\d]/g,'') : '';
  const amtPart = amtDigits ? `${dateDot?'.':''}${amtDigits}円` : '';
  const shortPayee = (payee||'').replace(/\s+/g,'').slice(0,20) || extractPayeeFromName_(fallbackName);
  return ['[済]', dateDot, amtPart, shortPayee].filter(Boolean).join(' ');
}
function extractPayeeFromName_(name){
  const base=(name||'').replace(/\.[^.]+$/,'');
  const tokens=base.split(/[ _\-\(\)【】\[\]、，・.]/).filter(Boolean);
  return tokens.length? tokens[tokens.length-1].slice(0,20) : '';
}

function guessDateFromFileName_(name){
  if (!name) return '';
  const s = String(name);
  let m = s.match(/(20\d{2})[\/\.\-年](\d{1,2})[\/\.\-月](\d{1,2})/);
  if (m) return `${m[1]}/${('0'+parseInt(m[2],10)).slice(-2)}/${('0'+parseInt(m[3],10)).slice(-2)}`;
  m = s.match(/(20\d{2})[\/\.\-](\d{1,2})[\/\.\-](\d{1,2})/);
  if (m) return `${m[1]}/${('0'+parseInt(m[2],10)).slice(-2)}/${('0'+parseInt(m[3],10)).slice(-2)}`;
  return '';
}
function guessAmountFromFileName_(name){
  if (!name) return '';
  const s = String(name);
  const m = s.replace(/[,，]/g,'').match(/(\d{3,})/);
  return m ? m[1] : '';
}
function toSlashDate_(s){
  if(!s) return '';
  const m = String(s).match(/(\d{4})[\/\-\.年](\d{1,2})[\/\-\.月](\d{1,2})/);
  if (m) return `${m[1]}/${('0'+parseInt(m[2],10)).slice(-2)}/${('0'+parseInt(m[3],10)).slice(-2)}`;
  return String(s);
}
function extractFileIdFromUrl_(url){ if(!url) return ''; const m=String(url).match(/[-\w]{25,}/); return m?m[0]:''; }

/* ============================ 近似用 正規化＆距離 ============================ */
function normalizeKey_(s){
  const z = zen2han_(String(s||''));
  return z.replace(/[ \u3000\t\r\n]/g,'').replace(/[（）()［］\[\]【】・,，．.]/g,'').replace(/費/g,'費'); // 体裁のみ
}
/** 全角→半角（英数・記号の主要どころ） */
function zen2han_(s){
  return s.replace(/[！-～]/g, ch => String.fromCharCode(ch.charCodeAt(0) - 0xFEE0))
          .replace(/　/g,' ');
}
function similarity_(a,b){
  const la=a.length, lb=b.length;
  if (la===0 && lb===0) return 1;
  const d = levenshtein_(a,b);
  return 1 - d / Math.max(1, Math.max(la,lb));
}
function levenshtein_(a,b){
  const m=a.length, n=b.length;
  const dp = new Array(m+1);
  for (let i=0;i<=m;i++){ dp[i]=new Array(n+1); dp[i][0]=i; }
  for (let j=0;j<=n;j++){ dp[0][j]=j; }
  for (let i=1;i<=m;i++){
    for (let j=1;j<=n;j++){
      const cost = a[i-1]===b[j-1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost);
    }
  }
  return dp[m][n];
}
