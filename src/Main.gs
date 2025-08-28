/*******************************************************
 * 税理士レベル会計自動仕訳システム - 完璧版
 * 業種判定とAI分析を組み合わせた高精度仕訳システム
 *******************************************************/

const SYSTEM_CONFIG = {
  VOUCHER_FOLDER_ID: '1awl5sHMstUZ8CpM2XBZTk205ELrDNrT8',
  ACCOUNT_MASTER_SSID: '1sa9SFTjQUD29zK720CRbCpuAyS96mZ1kQ8gsED_KrQQ',
  ACCOUNT_MASTER_SHEET: 'account_master',
  JOURNAL_SSID: '1MkPlJuPL74iWCWEws6gIwkOP2QkPekxIUuTYBA3sMfo',
  JOURNAL_SHEET_NAME: '悟大仕訳帳',

  GEMINI_MODEL: 'gemini-1.5-pro',
  GEMINI_ENDPOINT: 'https://generativelanguage.googleapis.com/v1beta/models/',

  RECURSIVE: true,
  INCLUDE_SHORTCUT_TARGETS: true,
  MAX_FILE_BYTES: 48 * 1024 * 1024,
  DONE_SUBFOLDER_NAME: '完了',
  INVOICE_SALES_SUBFOLDER_NAME: '売上請求書',
  INVOICE_PAYABLES_SUBFOLDER_NAME: '支払請求書',

  REQUIRED_FIELDS: ['日付', '金額', '取引先'],
  DEDUPE_MODE: 'hash_only',
  ALERT_FAIL_THRESHOLD: 5,
  ALERT_MAX_EXEC_MINUTES: 25,
  DEBUG_SAVE_HEAD: 500,
  CONFIDENCE_THRESHOLD: 0.7
};

/* ============================ エントリ ============================ */
function processNewInvoices() {
  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) throw new Error('GEMINI_API_KEY が未設定です。');

  const runId = createUUID_();
  const started = new Date();

  const journalSS = SpreadsheetApp.openById(SYSTEM_CONFIG.JOURNAL_SSID);
  const journalSheet = getOrCreateSheet_(journalSS, SYSTEM_CONFIG.JOURNAL_SHEET_NAME);
  const logSheet = getOrCreateRunLogSheet_(journalSS);

  ensureAuxSheets_(journalSS);
  ensureJournalHeader29_(journalSheet);

  log_(logSheet, 'INFO', '実行開始 - 税理士レベル完璧版');

  // マスタ読み込み
  const master = readAccountMaster_();
  log_(logSheet, 'INFO', `マスタ読み込み完了: 勘定科目=${master.accounts.size}件, コード解決可能=${master.accCodeCount}件`);

  // 重複防止インデックス
  const index = loadProcessedIndex_(journalSS);
  log_(logSheet, 'INFO', `重複チェック: 既処理ファイル=${index.hashes.size}件`);

  // 対象収集
  const root = DriveApp.getFolderById(SYSTEM_CONFIG.VOUCHER_FOLDER_ID);
  const summary = { files: 0, folders: 0, shortcuts: 0, collected: 0, names: [] };
  const targets = collectUnprocessedFiles_(root, summary, logSheet);
  log_(logSheet, 'INFO', `走査結果 files=${summary.files} folders=${summary.folders} shortcuts=${summary.shortcuts} collected=${summary.collected}`);
  
  if (!targets.length) {
    log_(logSheet, 'INFO', '未処理ファイル0件 - 実行終了');
    return;
  }

  // フォルダ準備
  const doneFolder = getOrCreateChildFolder_(root, SYSTEM_CONFIG.DONE_SUBFOLDER_NAME);
  const salesFolder = getOrCreateChildFolder_(root, SYSTEM_CONFIG.INVOICE_SALES_SUBFOLDER_NAME);
  const payFolder = getOrCreateChildFolder_(root, SYSTEM_CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME);

  let cntSuccess = 0, cntSkipped = 0, cntError = 0;
  const durations = [];

  for (const { file, originalName } of targets) {
    const t0 = Date.now();
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    let hash = '';
    
    try {
      log_(logSheet, 'INFO', `処理開始: ${originalName}`);

      const blob = file.getBlob();
      const sizeBytes = blob.getBytes().length;
      if (sizeBytes > SYSTEM_CONFIG.MAX_FILE_BYTES) {
        throw new Error(`ファイルサイズ上限超過: ${sizeBytes} bytes`);
      }

      // ハッシュ計算
      hash = sha256Hex_(blob.getBytes());
      if (index.hashes.has(hash)) {
        log_(logSheet, 'INFO', `重複スキップ: ${originalName}`);
        cntSkipped++; 
        continue;
      }

      // 工事コード抽出
      const folderCode = extractConstructionCodeFromFolderNames_(fileId);
      if (folderCode) {
        log_(logSheet, 'INFO', `工事コード検出: ${folderCode}`);
      }

      // AI呼び出し（税理士レベル分析）
      const aiResult = callProfessionalAI_(blob, apiKey, logSheet);
      if (!aiResult || aiResult.confidence < SYSTEM_CONFIG.CONFIDENCE_THRESHOLD) {
        log_(logSheet, 'INFO', `AI処理失敗または信頼度不足: ${originalName} (confidence: ${aiResult?.confidence || 0})`);
        cntSkipped++; 
        continue;
      }

      log_(logSheet, 'INFO', `AI分析成功: 日付=${aiResult.date} 金額=${aiResult.amount} 取引先=${aiResult.payee} 信頼度=${aiResult.confidence}`);

      // 必須項目チェック
      if (!aiResult.date || !aiResult.amount || !aiResult.payee) {
        log_(logSheet, 'INFO', `必須項目不足: ${originalName}`);
        cntSkipped++;
        continue;
      }

      // 税理士レベル仕訳生成
      const journalEntry = generateProfessionalJournal_(aiResult, folderCode, master, logSheet);
      
      // スプレッドシート出力
      const row29 = buildJournalRow29_(journalEntry, fileUrl);
      journalSheet.appendRow(row29);

      // ファイル処理
      const newName = buildProcessedName_(journalEntry, file.getName());
      file.setName(newName);
      
      // フォルダ振分け
      const folder = determineFolder_(journalEntry, originalName);
      if (folder === 'sales') {
        file.moveTo(salesFolder);
      } else if (folder === 'payables') {
        file.moveTo(payFolder);
      } else {
        file.moveTo(doneFolder);
      }

      // 重複防止登録
      index.hashes.add(hash);
      writeProcessedIndex_(journalSS, { runId, fileId, fileUrl, hash });

      log_(logSheet, 'INFO', `処理完了: ${newName} | 借方=${journalEntry.dAcc}(${journalEntry.dAccCode}) 貸方=${journalEntry.cAcc}(${journalEntry.cAccCode}) 信頼度=${aiResult.confidence}`);
      cntSuccess++;

    } catch (err) {
      const msg = err.message || String(err);
      log_(logSheet, 'ERROR', `処理エラー: ${originalName} | ${msg}`);
      cntError++;
    } finally {
      durations.push((Date.now() - t0) / 1000.0);
    }
  }

  // 実行結果
  const ended = new Date();
  const elapsed = (ended - started) / 1000.0;
  log_(logSheet, 'INFO', `実行完了: 成功=${cntSuccess} スキップ=${cntSkipped} エラー=${cntError} 実行時間=${elapsed.toFixed(1)}秒`);
}

/* ============================ 税理士レベルAI分析 ============================ */
function callProfessionalAI_(blob, apiKey, logSheet) {
  const prompt = buildProfessionalPrompt_();

  try {
    log_(logSheet, 'INFO', 'AI分析開始（税理士レベル）');
    
    const url = SYSTEM_CONFIG.GEMINI_ENDPOINT + encodeURIComponent(SYSTEM_CONFIG.GEMINI_MODEL) + ':generateContent?key=' + encodeURIComponent(apiKey);
    const body = {
      contents: [{
        role: 'user',
        parts: [
          { text: prompt },
          { 
            inline_data: { 
              mime_type: blob.getContentType(), 
              data: Utilities.base64Encode(blob.getBytes()) 
            } 
          }
        ]
      }],
      generationConfig: {
        temperature: 0.1, // 精度重視
        maxOutputTokens: 2048,
        candidateCount: 1
      }
    };

    const response = UrlFetchApp.fetch(url, {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify(body),
      muteHttpExceptions: true
    });

    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    log_(logSheet, 'INFO', `AI応答: HTTP=${responseCode} 長さ=${responseText.length}`);

    if (responseCode !== 200) {
      log_(logSheet, 'ERROR', `AI HTTPエラー: ${responseCode}`);
      return null;
    }

    // レスポンス解析
    let data;
    try {
      data = JSON.parse(responseText);
    } catch (e) {
      log_(logSheet, 'ERROR', 'AI応答のJSON解析失敗');
      return null;
    }

    // テキスト抽出
    let text = '';
    if (data.candidates && data.candidates[0] && data.candidates[0].content && data.candidates[0].content.parts) {
      text = data.candidates[0].content.parts[0].text || '';
    }

    if (!text) {
      log_(logSheet, 'ERROR', 'AI応答にテキストなし');
      return null;
    }

    log_(logSheet, 'INFO', `AI応答テキスト: ${text.slice(0, 300)}`);

    // JSON抽出と解析
    const result = extractProfessionalJSON_(text, logSheet);
    if (result) {
      log_(logSheet, 'INFO', `AI分析成功: 事業者=${result.merchant} 業種=${result.businessType} 勘定科目=${result.account} 信頼度=${result.confidence}`);
      return result;
    }

    log_(logSheet, 'ERROR', 'AI応答からの情報抽出失敗');
    return null;

  } catch (error) {
    log_(logSheet, 'ERROR', `AI処理例外: ${error.message}`);
    return null;
  }
}

function buildProfessionalPrompt_() {
  return `
あなたは日本の税理士として、証憑を詳細分析し正確な勘定科目を判定してください。

【分析手順】
1. 事業者の業種・業界を特定
2. 商品・サービス内容を詳細分析
3. 取引の性質を判断
4. 適切な勘定科目を決定
5. 信頼度を評価

【重要な業種判定基準】
- ガソリンスタンド: ENEOS, Shell, 出光, コスモ, Enejet等 → 車両費/旅費交通費
- タクシー会社: ○○タクシー, ○○交通, まるちく等 → 旅費交通費
- コンビニ: セブンイレブン, ファミマ, ローソン → 消耗品費/会議費
- 運送業: ヤマト運輸, 佐川急便等 → 荷造運賃/通信費
- 建設業: 材料・労務・外注・経費の4分類

【絶対に避けるべき誤判定】
- 会社名だけで勘定科目を判定しない
- 商品・サービス内容を無視しない  
- 事務用品費の過度な使用を避ける
- 業種を考慮しない分類をしない

以下のJSON形式で回答してください：
{
  "date": "YYYY-MM-DD",
  "amount": "金額数字のみ",
  "merchant": "事業者名",
  "businessType": "業種分類",
  "items": "商品・サービス内容",
  "account": "勘定科目",
  "subAccount": "補助科目",
  "reasoning": "判定理由の詳細説明",
  "confidence": 0.95,
  "paymentMethod": "支払方法"
}

【判定例】
- Enejet白石店 → ガソリンスタンド → 車両費
- まるちくタクシー → タクシー会社 → 旅費交通費  
- ヤマト運輸 → 運送業 → 荷造運賃
- セブンイレブン → コンビニ → 消耗品費
`;
}

function extractProfessionalJSON_(text, logSheet) {
  // 手法1: 直接JSON解析
  try {
    const cleaned = text.trim().replace(/```json|```/g, '');
    const parsed = JSON.parse(cleaned);
    return formatAIResult_(parsed);
  } catch (_) {}

  // 手法2: {}での抽出
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start !== -1 && end > start) {
    try {
      const jsonStr = text.slice(start, end + 1);
      const parsed = JSON.parse(jsonStr);
      return formatAIResult_(parsed);
    } catch (_) {}
  }

  // 手法3: 正規表現による値抽出
  const patterns = {
    date: /"date"\s*:\s*"([^"]+)"/,
    amount: /"amount"\s*:\s*"?([^",}]+)"?/,
    merchant: /"merchant"\s*:\s*"([^"]+)"/,
    businessType: /"businessType"\s*:\s*"([^"]+)"/,
    account: /"account"\s*:\s*"([^"]+)"/,
    confidence: /"confidence"\s*:\s*([0-9.]+)/
  };

  const extracted = {};
  let foundCount = 0;

  for (const [key, pattern] of Object.entries(patterns)) {
    const match = text.match(pattern);
    if (match) {
      extracted[key] = match[1].trim();
      foundCount++;
    }
  }

  if (foundCount >= 4) {
    return formatAIResult_(extracted);
  }

  return null;
}

function formatAIResult_(raw) {
  return {
    date: formatDate_(raw.date),
    amount: extractNumber_(raw.amount),
    payee: raw.merchant || '',
    merchant: raw.merchant || '',
    businessType: raw.businessType || '',
    items: raw.items || '',
    account: raw.account || '',
    subAccount: raw.subAccount || '',
    reasoning: raw.reasoning || '',
    confidence: parseFloat(raw.confidence) || 0.5,
    paymentMethod: raw.paymentMethod || ''
  };
}

function formatDate_(dateStr) {
  if (!dateStr) return '';
  const match = dateStr.match(/(\d{4})[\/\-\.]?(\d{1,2})[\/\-\.]?(\d{1,2})/);
  if (match) {
    const year = match[1];
    const month = ('0' + match[2]).slice(-2);
    const day = ('0' + match[3]).slice(-2);
    return `${year}/${month}/${day}`;
  }
  return dateStr;
}

function extractNumber_(str) {
  if (!str) return '';
  return String(str).replace(/[^\d]/g, '') || '';
}

/* ============================ 税理士レベル仕訳生成 ============================ */
function generateProfessionalJournal_(aiResult, folderCode, master, logSheet) {
  const { date, amount, payee, merchant, businessType, account, subAccount, reasoning, confidence } = aiResult;
  
  log_(logSheet, 'INFO', `仕訳生成開始: ${payee} | 業種=${businessType} | AI推奨=${account} | 信頼度=${confidence}`);

  // 基本エントリ
  const entry = {
    date: date,
    amount: amount,
    payee: payee,
    dAcc: '',
    dSub: '',
    dAccCode: '',
    dSubCode: '',
    cAcc: '',
    cSub: '',
    cAccCode: '',
    cSubCode: '',
    constructionCode: folderCode || '',
    note: `${payee} ${reasoning || ''}`.trim(),
    taxCode: '',
    dTaxCat: '課仕 10%',
    cTaxCat: '対象外',
    dInv: '',
    cInv: '',
    confidence: confidence
  };

  // === STEP 1: 業種・事業者による借方科目判定（AI結果を最優先） ===
  const professionalClassification = classifyByBusinessType_(merchant, businessType, account, logSheet);
  if (professionalClassification) {
    entry.dAcc = professionalClassification.account;
    entry.dSub = professionalClassification.subAccount;
    log_(logSheet, 'INFO', `業種判定適用: ${businessType} → ${entry.dAcc}/${entry.dSub}`);
  } else {
    // フォールバック判定
    entry.dAcc = account || determineAccountByMerchant_(merchant, logSheet);
    entry.dSub = subAccount || '';
    log_(logSheet, 'INFO', `フォールバック判定: ${entry.dAcc}/${entry.dSub}`);
  }

  // === STEP 2: 建設業科目の特別処理 ===
  if (entry.dAcc.startsWith('[')) {
    entry.dSub = ''; // 建設業科目は補助科目なし
    log_(logSheet, 'INFO', `建設業科目処理: ${entry.dAcc} 補助科目除去`);
  }

  // === STEP 3: 支払方法による貸方科目判定 ===
  const paymentClassification = classifyPaymentMethod_(payee, entry.note, aiResult.paymentMethod, logSheet);
  entry.cAcc = paymentClassification.account;
  entry.cSub = paymentClassification.subAccount;
  log_(logSheet, 'INFO', `支払方法判定: ${paymentClassification.method} → ${entry.cAcc}/${entry.cSub}`);

  // === STEP 4: マスタからのコード解決 ===
  entry.dAccCode = getAccountCode_(master, entry.dAcc) || '';
  entry.cAccCode = getAccountCode_(master, entry.cAcc) || '';
  entry.dSubCode = entry.dSub ? (getSubCode_(master, entry.dAcc, entry.dSub) || '') : '';
  entry.cSubCode = entry.cSub ? (getSubCode_(master, entry.cAcc, entry.cSub) || '') : '';

  // === STEP 5: 建設業科目の補助コード設定 ===
  if (entry.dAcc.startsWith('[') && entry.constructionCode && !entry.dSubCode) {
    entry.dSubCode = entry.constructionCode; // 工事コード = 補助科目コード
    log_(logSheet, 'INFO', `建設業補助コード設定: ${entry.constructionCode}`);
  }

  log_(logSheet, 'INFO', `仕訳完成: 借方=${entry.dAcc}(${entry.dAccCode})/${entry.dSub}(${entry.dSubCode}) 貸方=${entry.cAcc}(${entry.cAccCode})/${entry.cSub}(${entry.cSubCode})`);

  return entry;
}

// 業種別分類システム
function classifyByBusinessType_(merchant, businessType, suggestedAccount, logSheet) {
  const businessRules = {
    // ガソリンスタンド
    gasStation: {
      keywords: ['ガソリンスタンド', 'GS', 'エネオス', 'ENEOS', 'Enejet', 'エネジェット', 'Shell', 'シェル', '出光', 'コスモ'],
      account: '車両費',
      subAccount: 'ガソリン代'
    },
    
    // タクシー・交通業
    taxi: {
      keywords: ['タクシー', '交通', 'まるちく', '第一交通', '国際タクシー'],
      account: '旅費交通費', 
      subAccount: 'タクシー代'
    },
    
    // 運送業
    logistics: {
      keywords: ['運輸', '運送', 'ヤマト', '佐川', '日本郵便', 'クロネコ'],
      account: '荷造運賃',
      subAccount: '宅配便'
    },
    
    // コンビニ
    convenience: {
      keywords: ['セブンイレブン', 'ファミリーマート', 'ローソン', 'コンビニ'],
      account: '消耗品費',
      subAccount: ''
    },
    
    // 建設業（材料）
    construction_material: {
      keywords: ['建材', '資材', 'セメント', 'コンクリート'],
      account: '[材]材料費',
      subAccount: ''
    }
  };

  // 業種名による直接マッチ
  for (const [type, rule] of Object.entries(businessRules)) {
    if (rule.keywords.some(keyword => 
        merchant.includes(keyword) || 
        businessType.includes(keyword) ||
        suggestedAccount.includes(keyword)
    )) {
      log_(logSheet, 'INFO', `業種マッチ: ${type} → ${rule.account}`);
      return {
        type: type,
        account: rule.account,
        subAccount: rule.subAccount
      };
    }
  }

  return null;
}

function determineAccountByMerchant_(merchant, logSheet) {
  // 基本的な事業者名判定
  const merchantPatterns = {
    '車両費': ['enejet', 'eneos', 'shell', 'シェル', '出光', 'コスモ'],
    '旅費交通費': ['タクシー', 'まるちく', '交通', 'バス'],
    '荷造運賃': ['ヤマト', '佐川', '運輸', '運送'],
    '消耗品費': ['セブン', 'ファミマ', 'ローソン'],
    '接待交際費': ['居酒屋', 'レストラン', '料理']
  };

  const merchantLower = merchant.toLowerCase();
  
  for (const [account, patterns] of Object.entries(merchantPatterns)) {
    if (patterns.some(pattern => merchantLower.includes(pattern))) {
      log_(logSheet, 'INFO', `事業者名判定: ${merchant} → ${account}`);
      return account;
    }
  }

  log_(logSheet, 'INFO', `事業者名判定: ${merchant} → 消耗品費（デフォルト）`);
  return '消耗品費'; // 最後の手段
}

// 支払方法分類システム
function classifyPaymentMethod_(payee, note, paymentMethod, logSheet) {
  const cardPatterns = [
    { keywords: ['OCS', 'VISA'], name: 'OCS VISAカード', account: '未払金' },
    { keywords: ['JCB'], name: 'JCBカード', account: '未払金' },
    { keywords: ['Master'], name: 'Masterカード', account: '未払金' },
    { keywords: ['AMEX'], name: 'AMEXカード', account: '未払金' }
  ];

  // カード判定
  for (const pattern of cardPatterns) {
    if (pattern.keywords.some(k => payee.includes(k) || note.includes(k) || (paymentMethod && paymentMethod.includes(k)))) {
      return {
        method: 'credit_card',
        account: pattern.account,
        subAccount: pattern.name
      };
    }
  }

  // 一般的なカード判定
  const generalCardKeywords = ['カード', 'クレジット', 'CARD'];
  if (generalCardKeywords.some(k => payee.includes(k) || note.includes(k) || (paymentMethod && paymentMethod.includes(k)))) {
    return {
      method: 'credit_card',
      account: '未払金',
      subAccount: 'クレジットカード'
    };
  }

  // 銀行振込判定
  const bankKeywords = ['振込', '振替', '銀行', '支店'];
  if (bankKeywords.some(k => payee.includes(k) || note.includes(k) || (paymentMethod && paymentMethod.includes(k)))) {
    return {
      method: 'bank_transfer',
      account: '普通預金',
      subAccount: '銀行振込'
    };
  }

  // デフォルト：現金
  return {
    method: 'cash',
    account: '現金',
    subAccount: '小口現金'
  };
}

/* ============================ マスタ読み込み ============================ */
function readAccountMaster_() {
  try {
    const ss = SpreadsheetApp.openById(SYSTEM_CONFIG.ACCOUNT_MASTER_SSID);
    const sh = ss.getSheetByName(SYSTEM_CONFIG.ACCOUNT_MASTER_SHEET);
    if (!sh) throw new Error('マスタシート未発見');

    const values = sh.getRange(1, 1, sh.getLastRow(), sh.getLastColumn()).getValues();
    const accounts = new Map();
    let accCodeCount = 0;

    for (let i = 1; i < values.length; i++) {
      const row = values[i];
      const acc = String(row[0] || '').trim();
      const sub = String(row[1] || '').trim();  
      const accCode = String(row[3] || '').trim();
      const subCode = String(row[4] || '').trim();

      if (!acc) continue;

      if (!accounts.has(acc)) {
        accounts.set(acc, { code: '', subs: new Map() });
      }

      const meta = accounts.get(acc);
      if (accCode && accCode !== '0') {
        meta.code = accCode;
        accCodeCount++;
      }

      if (sub) {
        if (!meta.subs.has(sub)) {
          meta.subs.set(sub, { code: '' });
        }
        if (subCode && subCode !== '0') {
          meta.subs.get(sub).code = subCode;
        }
      }
    }

    return { accounts, accCodeCount };

  } catch (error) {
    Logger.log(`マスタ読み込みエラー: ${error.message}`);
    return { accounts: new Map(), accCodeCount: 0 };
  }
}

function getAccountCode_(master, accName) {
  if (!accName || !master.accounts.has(accName)) return '';
  return master.accounts.get(accName).code || '';
}

function getSubCode_(master, accName, subName) {
  if (!accName || !subName || !master.accounts.has(accName)) return '';
  const acc = master.accounts.get(accName);
  if (!acc.subs.has(subName)) return '';
  return acc.subs.get(subName).code || '';
}

/* ============================ 出力・ファイル処理 ============================ */
function buildJournalRow29_(entry, fileUrl) {
  return [
    entry.date,              // 1: 取引日
    entry.dAcc,              // 2: 借方勘定科目  
    entry.dAccCode,          // 3: 勘定科目コード（借方）
    entry.dSub,              // 4: 借方補助科目
    entry.dSubCode,          // 5: 補助科目コード（借方）
    entry.payee,             // 6: 借方取引先
    entry.constructionCode,  // 7: 工事コード
    entry.taxCode,           // 8: 消費税コード
    entry.dTaxCat,           // 9: 借方税区分  
    entry.dInv,              // 10: 借方インボイス
    entry.amount,            // 11: 借方金額(円)

    entry.cAcc,              // 12: 貸方勘定科目
    entry.cAccCode,          // 13: 勘定科目コード（貸方）
    entry.cSub,              // 14: 貸方補助科目
    entry.cSubCode,          // 15: 補助科目コード（貸方）
    entry.payee,             // 16: 貸方取引先
    entry.constructionCode,  // 17: 工事コード
    entry.cTaxCat,           // 18: 貸方税区分
    entry.cInv,              // 19: 貸方インボイス
    entry.amount,            // 20: 貸方金額(円)

    entry.note,              // 21: 摘要
    fileUrl,                 // 22: メモ
    '',                      // 23: 処理状態
    '',                      // 24: エクスポート日時
    '',                      // 25: エクスポートID
    fileUrl,                 // 26: メモ（重複）
    '',                      // 27: 処理状態（重複）
    '',                      // 28: エクスポート日時（重複）
    ''                       // 29: エクスポートID（重複）
  ];
}

function buildProcessedName_(entry, originalName) {
  const { date, amount, payee } = entry;
  let datePart = '';
  if (date) {
    const m = date.match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/);
    if (m) datePart = `${m[1]}.${parseInt(m[2])}.${parseInt(m[3])}`;
  }
  
  const amountPart = amount ? `${amount}円` : '';
  const payeePart = payee.replace(/\s+/g, '').slice(0, 15);
  
  return ['[済]', datePart, amountPart, payeePart].filter(Boolean).join(' ');
}

function determineFolder_(entry, fileName) {
  // 簡単な振分け判定
  if (/請求|invoice/i.test(fileName)) {
    return entry.payee.includes('悟大') ? 'sales' : 'payables';
  }
  return null;
}

/* ============================ その他のユーティリティ ============================ */
function collectUnprocessedFiles_(folder, summary, logSheet) {
  const out = [];
  const files = folder.getFiles();
  
  while (files.hasNext()) {
    const f = files.next();
    const name = f.getName();
    summary.files++;
    
    if (!isProcessedPrefix_(name)) {
      out.push({ file: f, originalName: name });
      summary.collected++;
      log_(logSheet, 'INFO', `対象追加: ${name}`);
    }
  }
  
  if (SYSTEM_CONFIG.RECURSIVE) {
    const folders = folder.getFolders();
    while (folders.hasNext()) {
      const sub = folders.next();
      const subName = sub.getName();
      if (![SYSTEM_CONFIG.DONE_SUBFOLDER_NAME, SYSTEM_CONFIG.INVOICE_SALES_SUBFOLDER_NAME, SYSTEM_CONFIG.INVOICE_PAYABLES_SUBFOLDER_NAME].includes(subName)) {
        summary.folders++;
        out.push(...collectUnprocessedFiles_(sub, summary, logSheet));
      }
    }
  }
  
  return out;
}

function isProcessedPrefix_(name) {
  if (!name) return false;
  const prefixes = ['[処理済み]', '【処理済み】', '[processed]', '[済]'];
  return prefixes.some(p => String(name).startsWith(p));
}

function extractConstructionCodeFromFolderNames_(fileId) {
  try {
    const file = DriveApp.getFileById(fileId);
    const parents = file.getParents();
    while (parents.hasNext()) {
      const parent = parents.next();
      const name = parent.getName();
      const match = name.match(/^[\[\(]?([A-Z0-9\-]{2,})[\]\)]?[\s　]/);
      if (match) return match[1];
    }
  } catch (_) {}
  return '';
}

function ensureJournalHeader29_(sheet) {
  const headers = [
    '取引日',
    '借方勘定科目','勘定科目コード','借方補助科目','補助科目コード','借方取引先','工事コード','消費税コード','借方税区分','借方インボイス','借方金額(円)',
    '貸方勘定科目','勘定科目コード','貸方補助科目','補助科目コード','貸方取引先','工事コード','貸方税区分','貸方インボイス','貸方金額(円)',
    '摘要','メモ','処理状態','エクスポート日時','エクスポートID','メモ','処理状態','エクスポート日時','エクスポートID'
  ];
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(headers);
  }
}

function loadProcessedIndex_(ss) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  const hashes = new Set();
  
  if (sh.getLastRow() > 1) {
    const values = sh.getRange(2, 1, sh.getLastRow() - 1, sh.getLastColumn()).getValues();
    values.forEach(row => {
      if (row[3]) hashes.add(String(row[3])); // content_hash列
    });
  } else {
    sh.appendRow(['run_id', 'file_id', 'file_url', 'content_hash', 'processed_at']);
  }
  
  return { hashes };
}

function writeProcessedIndex_(ss, data) {
  const sh = getOrCreateSheet_(ss, 'processed_index');
  sh.appendRow([data.runId, data.fileId, data.fileUrl, data.hash, now_()]);
}

function getOrCreateSheet_(ss, name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
  }
  return sheet;
}

function getOrCreateRunLogSheet_(ss) {
  const name = 'run_log';
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    sheet.appendRow(['日時', 'レベル', 'メッセージ']);
  }
  return sheet;
}

function getOrCreateChildFolder_(parent, name) {
  const folders = parent.getFoldersByName(name);
  return folders.hasNext() ? folders.next() : parent.createFolder(name);
}

function ensureAuxSheets_(ss) {
  const names = ['run_log', 'processed_index'];
  names.forEach(name => getOrCreateSheet_(ss, name));
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

function sha256Hex_(bytes) {
  const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes);
  return digest.map(byte => ('0' + (byte & 0xFF).toString(16)).slice(-2)).join('');
}

function createUUID_() {
  return Utilities.getUuid().replace(/-/g, '');
}

/* ============================ デバッグ・テスト ============================ */
function __testProfessionalSystem() {
  Logger.log('=== 税理士レベルシステム テスト開始 ===');
  
  // マスタテスト
  const master = readAccountMaster_();
  Logger.log(`マスタ読み込み: ${master.accounts.size}件, コード解決=${master.accCodeCount}件`);
  
  // 業種判定テスト
  const testCases = [
    { merchant: 'Enejet白石店', businessType: 'ガソリンスタンド', expected: '車両費' },
    { merchant: '株式会社まるちく', businessType: 'タクシー', expected: '旅費交通費' },
    { merchant: 'ヤマト運輸', businessType: '運送業', expected: '荷造運賃' },
    { merchant: 'セブンイレブン', businessType: 'コンビニ', expected: '消耗品費' }
  ];
  
  testCases.forEach((testCase, i) => {
    const classification = classifyByBusinessType_(testCase.merchant, testCase.businessType, '', null);
    const result = classification ? classification.account : 'デフォルト';
    Logger.log(`テスト${i+1}: ${testCase.merchant} → ${result} (期待値: ${testCase.expected}) ${result === testCase.expected ? '✓' : '✗'}`);
  });
  
  Logger.log('=== テスト完了 ===');
}

function __clearProcessedIndex() {
  const ss = SpreadsheetApp.openById(SYSTEM_CONFIG.JOURNAL_SSID);
  const sh = ss.getSheetByName('processed_index');
  if (sh) {
    sh.clear();
    sh.appendRow(['run_id', 'file_id', 'file_url', 'content_hash', 'processed_at']);
    Logger.log('重複防止インデックスをクリアしました');
  }
}
