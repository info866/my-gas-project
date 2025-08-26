/*******************************************************
 * コード.gs（Gemini応答問題解決版）
 * 修正内容：
 *  - responseMimeType削除でGemini応答取得を修正
 *  - 応答構造の完全探索
 *  - デバッグ機能強化
 *  - gemini-2.5-pro対応
 *******************************************************/
const CONFIG = {
  // === 必要ID ===
  VOUCHER_FOLDER_ID: '1awl5sHMstUZ8CpM2XBZTk205ELrDNrT8',  // 証憑ルート
  ACCOUNT_MASTER_SSID: '1sa9SFTjQUD29zK720CRbCpuAyS96mZ1kQ8gsED_KrQQ',
  ACCOUNT_MASTER_SHEET: 'account_master',
  JOURNAL_SSID: '1MkPlJuPL74iWCWEws6gIwkOP2QkPekxIUuTYBA3sMfo',
  JOURNAL_SHEET_NAME: '悟大仕訳帳',

  // === Gemini ===
  GEMINI_MODEL: 'gemini-2.5-pro',  // 現在使用中のモデル
  GEMINI_ENDPOINT: 'https://generativelanguage.googleapis.com/v1beta/models/',
  MAX_FILE_BYTES: 20 * 1024 * 1024,  // 20MBに削減

  // === 走査設定 ===
  RECURSIVE: true,
  INCLUDE_SHORTCUT_TARGETS: true,
  DONE_SUBFOLDER_NAME: '完了',

  // === 仕訳要件 ===
  REQUIRED_FIELDS: ['日付', '金額', '取引先'],

  // 工事系科目の判定キー（部分一致）
  CONSTRUCTION_ACCOUNTS: ['完成工事高','未成工事支出金','完成工事未収入金','外注費','工事仮勘定'],

  // 重複制御
  DEDUPE_MODE: 'on',

  // デバッグ
  DEBUG_MODE: true,  // デバッグログを出力
  DEBUG_SAVE_HEAD: 1000
};

/* ============================ エントリ ============================ */
function processNewInvoices() {
  const apiKey = PropertiesService.getScriptProperties().getProperty('GEMINI_API_KEY');
  if (!apiKey) throw new Error('GEMINI_API_KEY が未設定です。');

  const runId = createUUID_();
  const journalSS = SpreadsheetApp.openById(CONFIG.JOURNAL_SSID);
  const journalSheet = getOrCreateSheet_(journalSS, CONFIG.JOURNAL_SHEET_NAME);

  ensureJournalHeader29_(journalSheet);
  const index = loadProcessedIndex_(journalSS);

  // マスタ1回読み込み
  const master = readAccountMaster_Map_();
  
  // デバッグ: マスタ情報
  if (CONFIG.DEBUG_MODE) {
    console.log(`マスタ読み込み完了: 勘定科目${master.accounts.size}件, 補助科目${master.totalSubs}件, 税区分${master.taxSet.size}件`);
  }

  // 収集
  const root = DriveApp.getFolderById(CONFIG.VOUCHER_FOLDER_ID);
  const summary = { files: 0, folders: 0, shortcuts: 0, collected: 0, names: [] };
  const targets = collectUnprocessedFiles_(root, summary);

  if (!targets.length) {
    console.log('処理対象ファイルなし');
    return;
  }

  console.log(`処理対象: ${targets.length}ファイル`);
  const doneFolder = getOrCreateChildFolder_(root, CONFIG.DONE_SUBFOLDER_NAME);

  let cntSuccess = 0, cntSkipped = 0, cntError = 0;

  for (const { file, originalName } of targets) {
    const fileId = file.getId();
    const fileUrl = file.getUrl();
    let hash = '';
    
    try {
      const blob = file.getBlob();
      const mimeType = blob.getContentType();
      const sizeBytes = blob.getBytes().length;
      
      console.log(`処理開始: ${originalName} (${Math.round(sizeBytes/1024)}KB, ${mimeType})`);
      
      if (sizeBytes > CONFIG.MAX_FILE_BYTES) {
        throw new Error('サイズ上限超過: ' + Math.round(sizeBytes/1024/1024) + 'MB');
      }

      // 二重防止
      hash = sha256Hex_(blob.getBytes());
      if (shouldDedupeSkip_(index, fileId, hash)) {
        writeSkipped_(journalSS, { reason: '重複スキップ(' + CONFIG.DEDUPE_MODE + ')', fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // 親フォルダ名から工事コード候補
      const folderCode = extractConstructionCodeFromFolderNames_(fileId);

      // === AI呼び出し（修正版）===
      const prompt = buildOptimizedPrompt_(master, { fileUrl });
      const parsed = askGeminiFixed_(prompt, blob, mimeType, apiKey);
      
      if (!parsed.success) {
        writeSkipped_(journalSS, { 
          reason: `AI処理失敗: ${parsed.error}`, 
          fileId, fileUrl, hash, 
          name: originalName 
        });
        cntSkipped++; 
        continue;
      }
      
      const obj = parsed.data;
      
      if (obj['エラー']) {
        writeSkipped_(journalSS, { reason: `AIエラー: ${obj['エラー']}`, fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // === 必須の抽出＆ゲート ===
      const dateSlash = toSlashDate_(getVal_(obj, ['日付']));
      const payee = (getVal_(obj, ['借方取引先','貸方取引先']) || '').toString().trim();
      const amount = (getVal_(obj, ['借方金額(円)','貸方金額(円)']) || '').toString().replace(/[^\d]/g, '');
      let note = (getVal_(obj, ['摘要']) || '').toString().trim();

      const missing = [];
      if (!dateSlash) missing.push('日付');
      if (!amount)   missing.push('金額');
      if (!payee)    missing.push('取引先');
      if (missing.length) {
        writeSkipped_(journalSS, { reason: '必須欠落: ' + missing.join(', '), fileId, fileUrl, hash, name: originalName });
        cntSkipped++; continue;
      }

      // 摘要にURL付与
      if (!note || !note.includes(payee)) note = `${payee} ${note}`.trim();
      const noteWithUrl = withUrlInNote_(note, fileUrl);

      // === 借貸の名称（AI出力） ===
      let dAcc_ai = norm_(getVal_(obj, ['借方科目']));
      let dSub_ai = norm_(getVal_(obj, ['借方補助科目']));
      let cAcc_ai = norm_(getVal_(obj, ['貸方科目']));
      let cSub_ai = norm_(getVal_(obj, ['貸方補助科目']));

      // 工事コード（JSON>フォルダ）
      let constructionCode = norm_(getVal_(obj, ['工事コード'])) || folderCode;

      // 税系
      const taxCode = norm_(getVal_(obj, ['消費税コード']));
      const dTaxCat = norm_(getVal_(obj, ['借方税区分']));
      const cTaxCat = norm_(getVal_(obj, ['貸方税区分']));
      const dInv = norm_(getVal_(obj, ['借方インボイス','借方インボイス番号']));
      const cInv = norm_(getVal_(obj, ['貸方インボイス','貸方インボイス番号']));
      const dAmt = norm_(getVal_(obj, ['借方金額(円)']));
      const cAmt = norm_(getVal_(obj, ['貸方金額(円)']));

      // === 名称解決（厳密→部分一致） ===
      const dAcc_res = resolveAccountName_(master, dAcc_ai) || dAcc_ai;
      const cAcc_res = resolveAccountName_(master, cAcc_ai) || cAcc_ai;

      let dSub_res = dSub_ai || '';
      let cSub_res = cSub_ai || '';

      if (dSub_res) dSub_res = resolveSubName_(master, dAcc_res, dSub_res) || dSub_res;
      if (cSub_res) cSub_res = resolveSubName_(master, cAcc_res, cSub_res) || cSub_res;

      // 工事系フォルダ強制ルール
      ({ dSub: dSub_res, cSub: cSub_res } = forceConstructionSubIfNeeded_(dAcc_res, cAcc_res, dSub_res, cSub_res, constructionCode));

      // === コード解決（マスタ） ===
      const dAccCode = getAccountCode_(master, dAcc_res) || '';
      const cAccCode = getAccountCode_(master, cAcc_res) || '';
      let dSubCode = getSubCode_(master, dAcc_res, dSub_res) || '';
      let cSubCode = getSubCode_(master, cAcc_res, cSub_res) || '';

      // 補助コードが未定義で「コードっぽい」補助名は、その値を採用
      if (!dSubCode && /^[0-9A-Za-z\-]{2,}$/.test(dSub_res)) dSubCode = dSub_res;
      if (!cSubCode && /^[0-9A-Za-z\-]{2,}$/.test(cSub_res)) cSubCode = cSub_res;

      // === 29列 固定順 ===
      const row29 = [
        dateSlash,
        dAcc_res, dAccCode, dSub_res, dSubCode,
        norm_(getVal_(obj, ['借方取引先'])) || '',
        constructionCode || '',
        taxCode || '',
        dTaxCat || '',
        dInv || '',
        dAmt || '',
        cAcc_res, cAccCode, cSub_res, cSubCode,
        norm_(getVal_(obj, ['貸方取引先'])) || '',
        constructionCode || '',
        cTaxCat || '',
        cInv || '',
        cAmt || '',
        noteWithUrl || '',
        fileUrl || '',
        '', '', '', fileUrl || '', '', '', ''
      ];
      
      journalSheet.appendRow(row29);

      // リネーム→完了フォルダへ
      const newName = buildProcessedName_({ date: dateSlash, amount, payee }, file.getName());
      file.setName(newName);
      file.moveTo(doneFolder);

      // 二重防止登録
      writeProcessedIndex_(journalSS, { runId, fileId, fileUrl, hash });

      cntSuccess++;
      console.log(`処理成功: ${originalName}`);
      
    } catch (err) {
      const msg = err && err.message ? err.message : String(err);
      writeSkipped_(journalSS, { reason: '処理例外: ' + msg, fileId, fileUrl, hash, name: originalName });
      cntError++;
      console.error(`処理エラー: ${originalName} - ${msg}`);
    }
  }
  
  console.log(`処理完了: 成功${cntSuccess}, スキップ${cntSkipped}, エラー${cntError}`);
}

/* ============================ Gemini API修正版 ============================ */
function askGeminiFixed_(prompt, blob, mimeType, apiKey) {
  const maxRetries = 3;
  
  for (let retry = 0; retry < maxRetries; retry++) {
    try {
      // リトライごとにプロンプトを調整
      let adjustedPrompt = prompt;
      if (retry === 1) {
        adjustedPrompt = prompt + '\n\n出力は純粋なJSONのみ。説明文やコードブロックは絶対に含めない。';
      } else if (retry === 2) {
        adjustedPrompt = '以下の形式のJSONのみを出力:\n{"日付":"YYYY/MM/DD","借方科目":"","借方補助科目":"","借方取引先":"","消費税コード":"","借方税区分":"","借方インボイス":"","借方金額(円)":0,"貸方科目":"","貸方補助科目":"","貸方取引先":"","貸方税区分":"","貸方インボイス":"","貸方金額(円)":0,"工事コード":"","摘要":""}\n\n' + prompt;
      }
      
      const url = CONFIG.GEMINI_ENDPOINT + CONFIG.GEMINI_MODEL + ':generateContent?key=' + apiKey;
      
      const payload = {
        contents: [{
          role: 'user',
          parts: [
            { text: adjustedPrompt },
            {
              inline_data: {
                mime_type: mimeType,
                data: Utilities.base64Encode(blob.getBytes())
              }
            }
          ]
        }],
        generationConfig: {
          temperature: retry * 0.1,  // リトライごとに温度を上げる
          maxOutputTokens: 4096,
          topK: 1,
          topP: 0.95
          // responseMimeTypeは削除（これが原因）
        }
      };

      const response = UrlFetchApp.fetch(url, {
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      });

      const responseCode = response.getResponseCode();
      const responseText = response.getContentText();
      
      if (CONFIG.DEBUG_MODE && retry === 0) {
        console.log(`Gemini応答コード: ${responseCode}`);
        console.log(`Gemini応答(最初の500文字): ${responseText.substring(0, 500)}`);
      }
      
      if (responseCode !== 200) {
        console.error(`API Error ${responseCode}: ${responseText.substring(0, 200)}`);
        if (retry < maxRetries - 1) {
          Utilities.sleep(1000 * (retry + 1));
          continue;
        }
        return { success: false, error: `API Error ${responseCode}` };
      }

      // 応答をパース
      const responseData = JSON.parse(responseText);
      
      // テキスト抽出（完全版）
      const extractedText = extractTextComprehensive_(responseData);
      
      if (!extractedText) {
        console.error(`テキスト抽出失敗 (retry ${retry})`);
        if (CONFIG.DEBUG_MODE) {
          console.log('応答構造:', JSON.stringify(responseData).substring(0, 1000));
        }
        if (retry < maxRetries - 1) {
          Utilities.sleep(1000 * (retry + 1));
          continue;
        }
        return { success: false, error: 'AI応答からテキストを抽出できません' };
      }

      if (CONFIG.DEBUG_MODE) {
        console.log(`抽出テキスト(最初の200文字): ${extractedText.substring(0, 200)}`);
      }

      // JSON解析
      const jsonObj = parseJsonFromText_(extractedText);
      
      if (!jsonObj) {
        console.error(`JSON解析失敗 (retry ${retry}): ${extractedText.substring(0, 200)}`);
        if (retry < maxRetries - 1) {
          Utilities.sleep(1000 * (retry + 1));
          continue;
        }
        return { success: false, error: 'JSON解析エラー' };
      }

      return { success: true, data: jsonObj };

    } catch (err) {
      console.error(`Gemini呼び出しエラー (retry ${retry}): ${err.message || err}`);
      if (retry < maxRetries - 1) {
        Utilities.sleep(1000 * (retry + 1));
        continue;
      }
      return { success: false, error: `AI処理エラー: ${err.message || String(err)}` };
    }
  }
  
  return { success: false, error: '最大リトライ回数超過' };
}

/* ============================ 応答テキスト抽出（完全版） ============================ */
function extractTextComprehensive_(responseData) {
  // 1. candidates[].content.parts[].text (最も一般的)
  try {
    if (responseData.candidates && responseData.candidates.length > 0) {
      for (const candidate of responseData.candidates) {
        if (candidate.content && candidate.content.parts) {
          for (const part of candidate.content.parts) {
            if (part && part.text) {
              return part.text;
            }
          }
        }
      }
    }
  } catch (e) {}

  // 2. candidates[].content.text
  try {
    if (responseData.candidates && responseData.candidates.length > 0) {
      for (const candidate of responseData.candidates) {
        if (candidate.content && candidate.content.text) {
          return candidate.content.text;
        }
      }
    }
  } catch (e) {}

  // 3. candidates[].output
  try {
    if (responseData.candidates && responseData.candidates.length > 0) {
      for (const candidate of responseData.candidates) {
        if (candidate.output) {
          return candidate.output;
        }
      }
    }
  } catch (e) {}

  // 4. candidates[].text (直接)
  try {
    if (responseData.candidates && responseData.candidates.length > 0) {
      for (const candidate of responseData.candidates) {
        if (candidate.text) {
          return candidate.text;
        }
      }
    }
  } catch (e) {}

  // 5. text (トップレベル)
  try {
    if (responseData.text) {
      return responseData.text;
    }
  } catch (e) {}

  // 6. output (トップレベル)
  try {
    if (responseData.output) {
      return responseData.output;
    }
  } catch (e) {}

  // 7. result
  try {
    if (responseData.result) {
      return responseData.result;
    }
  } catch (e) {}

  // 8. response
  try {
    if (responseData.response) {
      return responseData.response;
    }
  } catch (e) {}

  // 9. その他のparts探索
  try {
    if (responseData.candidates && responseData.candidates.length > 0) {
      for (const candidate of responseData.candidates) {
        if (candidate.content && candidate.content.parts) {
          for (const part of candidate.content.parts) {
            if (part) {
              // executable_code, code, functionCall など
              if (part.executable_code) return part.executable_code;
              if (part.code) return part.code;
              if (part.functionCall) return JSON.stringify(part.functionCall);
              if (part.generatedText) return part.generatedText;
              if (part.completion) return part.completion;
            }
          }
        }
      }
    }
  } catch (e) {}

  return null;
}

/* ============================ JSON解析（堅牢版） ============================ */
function parseJsonFromText_(text) {
  if (!text) return null;
  
  // 前処理: コードフェンス、余計な文字を削除
  let cleaned = text
    .replace(/^```json\s*/i, '')
    .replace(/^```javascript\s*/i, '')
    .replace(/^```\s*/, '')
    .replace(/```\s*$/g, '')
    .trim();
  
  // 1. 通常のJSON解析
  try {
    return JSON.parse(cleaned);
  } catch (e) {}
  
  // 2. JSONオブジェクト部分を抽出
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    try {
      return JSON.parse(jsonMatch[0]);
    } catch (e) {}
  }
  
  // 3. 改行・空白の正規化
  try {
    const normalized = cleaned
      .replace(/[\r\n]+/g, ' ')
      .replace(/,(\s*[}\]])/g, '$1')  // 末尾カンマ削除
      .replace(/([{,]\s*)(\w+)(:)/g, '$1"$2"$3')  // キーにクォート追加
      .replace(/:\s*'([^']*)'/g, ': "$1"')  // シングルクォートをダブルに
      .trim();
    return JSON.parse(normalized);
  } catch (e) {}
  
  // 4. 最初と最後の { } を探す
  const firstBrace = text.indexOf('{');
  const lastBrace = text.lastIndexOf('}');
  if (firstBrace !== -1 && lastBrace !== -1 && lastBrace > firstBrace) {
    try {
      return JSON.parse(text.substring(firstBrace, lastBrace + 1));
    } catch (e) {}
  }
  
  return null;
}

/* ============================ プロンプト（精度重視版） ============================ */
function buildOptimizedPrompt_(master, ctx) {
  const accountList = Array.from(master.accounts.keys());
  const subMapLines = [];
  for (const [acc, meta] of master.accounts.entries()) {
    const subs = Array.from(meta.subs.keys());
    if (subs.length) {
      subMapLines.push(`${acc}: ${subs.join(', ')}`);
    }
  }
  const taxList = Array.from(master.taxSet.values());

  return `商業文書から仕訳データを抽出し、以下のJSON形式のみを出力してください。

【重要】
- 勘定科目・補助科目は必ず下記リストから選択
- インボイス番号（T+13桁）を必ず探索
- 出力はJSON形式のみ（説明文禁止）

【勘定科目リスト】
${accountList.join(', ')}

【補助科目リスト】
${subMapLines.join('\n')}

【税区分リスト】
${taxList.join(', ')}

【出力形式（この形式のみ）】
{"日付":"YYYY/MM/DD","借方科目":"","借方補助科目":"","借方取引先":"","消費税コード":"","借方税区分":"","借方インボイス":"","借方金額(円)":0,"貸方科目":"","貸方補助科目":"","貸方取引先":"","貸方税区分":"","貸方インボイス":"","貸方金額(円)":0,"工事コード":"","摘要":""}

【仕訳ルール】
- 請求書→買掛金/費用科目
- 領収書→現金or預金/費用科目
- 売上請求書→売掛金/売上高
- 日付は必須（YYYY/MM/DD形式）
- 金額は整数値
- 不明な項目は空文字`;
}

/* ============================ 名前解決 ============================ */
function normKeyName_(s) {
  if (s == null) return '';
  let t = String(s).normalize('NFKC').toLowerCase();
  t = t.replace(/\s+/g, '');
  t = t.replace(/[・･.\-_\(\)（）、，［］【】\[\]]/g, '');
  t = t.replace(/費/g, '');
  return t;
}

function resolveAccountName_(master, aiName) {
  const ai = normKeyName_(aiName);
  if (!ai) return '';
  const accNames = Array.from(master.accounts.keys());

  const exact = accNames.filter(n => normKeyName_(n) === ai);
  if (exact.length === 1) return exact[0];

  const part = accNames.filter(n => {
    const m = normKeyName_(n);
    return m.includes(ai) || ai.includes(m);
  });
  if (part.length === 1) return part[0];

  return '';
}

function resolveSubName_(master, accNameResolved, aiSubName) {
  const accMeta = master.accounts.get(accNameResolved);
  if (!accMeta) return '';
  const subs = Array.from(accMeta.subs.keys());
  if (!subs.length) return '';

  const ai = normKeyName_(aiSubName);

  const exact = subs.filter(n => normKeyName_(n) === ai);
  if (exact.length === 1) return exact[0];

  const part = subs.filter(n => {
    const m = normKeyName_(n);
    return m.includes(ai) || ai.includes(m);
  });
  if (part.length === 1) return part[0];

  return '';
}

/* ============================ 以下、既存の関数群（変更なし） ============================ */
function collectUnprocessedFiles_(folder, summary) {
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
          if (!isProcessedPrefix_(rName)) {
            out.push({ file: resolved, originalName: rName });
            summary.collected++; summary.names.push(rName);
          }
        }
      } else {
        summary.files++;
        if (!isProcessedPrefix_(name)) {
          out.push({ file: f, originalName: name });
          summary.collected++; summary.names.push(name);
        }
      }
    } catch (e) {
      writeSkipped_(SpreadsheetApp.openById(CONFIG.JOURNAL_SSID), { 
        reason:'走査中エラー: ' + String(e && e.message ? e.message : e), 
        fileId:f.getId(), fileUrl:f.getUrl(), hash:'', name 
      });
    }
  }
  if (CONFIG.RECURSIVE) {
    const folders = folder.getFolders();
    while (folders.hasNext()) {
      const sub = folders.next();
      const subName = sub.getName();
      if (subName === CONFIG.DONE_SUBFOLDER_NAME) continue;
      summary.folders++;
      out.push(...collectUnprocessedFiles_(sub, summary));
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
  return ['[処理済み]','【処理済み】','[processed]','[ processed ]','[済]','[済] '].some(p => name.startsWith(p));
}

function ensureJournalHeader29_(sheet) {
  const headers = [
    '取引日',
    '借方勘定科目','勘定科目コード','借方補助科目','補助科目コード','借方取引先','工事コード','消費税コード','借方税区分','借方インボイス','借方金額(円)',
    '貸方勘定科目','勘定科目コード','貸方補助科目','補助科目コード','貸方取引先','工事コード','貸方税区分','貸方インボイス','貸方金額(円)',
    '摘要','メモ','処理状態','エクスポート日時','エクスポートID','メモ','処理状態','エクスポート日時','エクスポートID'
  ];
  if (sheet.getLastRow() < 1) {
    sheet.appendRow(headers);
    return;
  }
  const range = sheet.getRange(1,1,1,headers.length);
  const existing = range.getValues()[0];
  const same = existing.length === headers.length && existing.every((v,i)=>String(v||'')===headers[i]);
  if (!same) range.setValues([headers]);
}

function readAccountMaster_Map_() {
  const ss = SpreadsheetApp.openById(CONFIG.ACCOUNT_MASTER_SSID);
  const sh = ss.getSheetByName(CONFIG.ACCOUNT_MASTER_SHEET);
  if (!sh) throw new Error('勘定科目シートが見つかりません: ' + CONFIG.ACCOUNT_MASTER_SHEET);

  const lastRow = sh.getLastRow();
  const lastCol = sh.getLastColumn();
  if (lastRow < 1 || lastCol < 1) {
    return { accounts: new Map(), totalSubs: 0, taxSet: new Set(), accCodeCount:0, subCodeCount:0 };
  }
  const values = sh.getRange(1, 1, lastRow, lastCol).getValues();
  const header = values[0].map(v => String(v||'').trim());

  const H = (cands) => header.findIndex(h => cands.includes(h));
  const idxAccName = H(['勘定科目','科目','アカウント','account']);
  const idxSubName = H(['補助科目','サブ科目','subaccount','サブ']);
  const idxTax     = H(['税区分','消費税区分','税','tax']);
  const idxAccCode = H(['勘定科目コード','科目コード','MJS科目コード','MJSコード','アカウントコード','account_code']);
  const idxSubCode = H(['補助科目コード','サブ科目コード','subaccount_code','サブコード']);

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

    if (!accounts.has(acc)) accounts.set(acc, { code: '', subs: new Map() });
    const meta = accounts.get(acc);

    if (aCode) { meta.code = aCode; accCodeCount++; }
    if (sub) {
      if (!meta.subs.has(sub)) meta.subs.set(sub, { code: '' });
      if (sCode) { meta.subs.get(sub).code = sCode; subCodeCount++; }
      totalSubs++;
    }
    if (tax) taxSet.add(tax);
  }
  return { accounts, totalSubs, taxSet, accCodeCount, subCodeCount };
}

function norm_(v){ return (v==null)?'':String(v).trim(); }
function getAccountCode_(master, accName) {
  const m = accName ? master.accounts.get(accName) : null;
  return (m && m.code) ? m.code : '';
}
function getSubCode_(master, accName, subName) {
  if (!accName || !subName) return '';
  const m = master.accounts.get(accName);
  if (!m) return '';
  const s = m.subs.get(subName);
  return (s && s.code) ? s.code : '';
}

function extractConstructionCodeFromFolderNames_(fileId) {
  try {
    const file = DriveApp.getFileById(fileId);
    const parents = file.getParents();
    while (parents.hasNext()) {
      const p = parents.next();
      const name = (p.getName() || '').trim();
      const m = name.match(/^[\[\(]?([0-9A-Za-z\-]{2,})[\]\)]?[ 　]+/);
      if (m && m[1]) return m[1];
    }
  } catch (_) {}
  return '';
}

function isConstructionAccountName_(accName) {
  if (!accName) return false;
  return CONFIG.CONSTRUCTION_ACCOUNTS.some(key => accName.indexOf(key) !== -1);
}

function forceConstructionSubIfNeeded_(dAcc, cAcc, dSub, cSub, constructionCode) {
  if (constructionCode) {
    if (isConstructionAccountName_(dAcc)) dSub = constructionCode;
    if (isConstructionAccountName_(cAcc)) cSub = constructionCode;
  }
  return { dSub, cSub };
}

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
  if (sh.getLastRow() < 1) sh.appendRow(['run_id','file_id','file_url','content_hash','processed_at']);
  sh.appendRow([row.runId || '', row.fileId || '', row.fileUrl || '', row.hash || '', now_()]);
}

function writeSkipped_(ss, o) {
  const sh = getOrCreateSheet_(ss, 'skipped');
  if (sh.getLastRow() < 1) sh.appendRow(['日時','理由','ファイル名','fileId','fileUrl','content_hash']);
  sh.appendRow([now_(), o.reason || '', o.name || '', o.fileId || '', o.fileUrl || '', o.hash || '']);
}

function shouldDedupeSkip_(index, fileId, hash) {
  const mode = CONFIG.DEDUPE_MODE;
  if (mode === 'off') return false;
  if (mode === 'id_only') return index.ids.has(fileId);
  if (mode === 'hash_only') return index.hashes.has(hash);
  return index.ids.has(fileId) || index.hashes.has(hash);
}

function getOrCreateSheet_(ss, name) { 
  let sh = ss.getSheetByName(name); 
  if (!sh) sh = ss.insertSheet(name); 
  return sh; 
}
function getOrCreateChildFolder_(parent, name) { 
  const it = parent.getFoldersByName(name); 
  return it.hasNext()? it.next(): parent.createFolder(name); 
}
function isDriveAdvancedAvailable_(){ 
  try{ return typeof Drive!=='undefined' && Drive && Drive.Files && typeof Drive.Files.get==='function'; }
  catch(_){ return false; } 
}
function now_(){ 
  return Utilities.formatDate(new Date(), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss'); 
}
function toSlashDate_(s){ 
  if(!s) return ''; 
  const m=s.match(/(\d{4})[\/\-\.](\d{1,2})[\/\-\.](\d{1,2})/); 
  return m? `${m[1]}/${('0'+parseInt(m[2],10)).slice(-2)}/${('0'+parseInt(m[3],10)).slice(-2)}`: s; 
}
function sha256Hex_(bytes){ 
  const dig = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes); 
  return dig.map(b=>('0'+(b & 0xFF).toString(16)).slice(-2)).join(''); 
}
function createUUID_(){ 
  const u=Utilities.getUuid(); 
  return u.replace(/-/g,''); 
}
function getVal_(obj, keys){ 
  for (const k of keys){ 
    const v=obj[k]; 
    if (v!==undefined && v!==null && String(v)!=='') return v; 
  } 
  return ''; 
}
function withUrlInNote_(note, url){ 
  if (!url) return note || ''; 
  const s=(note||'').trim(); 
  if (s.includes(url)) return s; 
  return s ? `${s} ${url}` : url; 
}
function buildProcessedName_({ date, amount, payee }, fallbackName) {
  let dateDot = '';
  if (date) { 
    const m = date.match(/(\d{4})\/(\d{1,2})\/(\d{1,2})/); 
    if (m) dateDot = `${m[1]}.${parseInt(m[2], 10)}.${parseInt(m[3], 10)}`; 
  }
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
