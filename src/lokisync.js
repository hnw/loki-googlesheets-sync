/**
 * @fileoverview LokiログをGoogleスプレッドシートに転記するGoogle Apps Script
 * @version 1.0.0
 * @license MIT (Specify your license if needed)
 */

// --- 定数定義 ---
/** スクリプトプロパティのキー */
const SCRIPT_PROPERTIES_KEYS = {
  SPREADSHEET_ID: 'SPREADSHEET_ID',
  LOKI_API_ENDPOINT: 'LOKI_API_ENDPOINT',
  LOKI_USERNAME: 'LOKI_USERNAME', // Basic認証用 (オプション)
  LOKI_PASSWORD: 'LOKI_PASSWORD', // Basic認証用 (オプション)
  LOKI_API_KEY: 'LOKI_API_KEY', // APIキー認証用 (オプション)
  LOKI_BASE_QUERY: 'LOKI_BASE_QUERY',
  LOKI_QUERY_LIMIT: 'LOKI_QUERY_LIMIT',
  LOKI_OVERLAP_SECONDS: 'LOKI_OVERLAP_SECONDS',
  TIMEZONE_OFFSET: 'TIMEZONE_OFFSET',
};

/** 無視するシート名のプレフィックス */
const IGNORED_SHEET_PREFIX = '_';

/** タイムスタンプ列の名前 */
const TIMESTAMP_COLUMN_NAME = '_timestamp'; // Lokiのタイムスタンプキーに合わせて変更が必要な場合あり

/** 初回実行時に遡る時間 (秒) */
const INITIAL_LOOKBACK_SECONDS = 60 * 60 // 1時間

/** 許可されるmetric_nameの文字種 (正規表現) */
const ALLOWED_METRIC_NAME_REGEX = /^[a-zA-Z0-9_-]+$/;

/**
 * メイン関数: スクリプトのエントリーポイント
 */
function main() {
  const config = getConfig_();
  if (!config) {
    Logger.log('Error: 設定が不十分なため処理を中断します。');
    return;
  }

  const ss = SpreadsheetApp.openById(config.spreadsheetId);
  if (!ss) {
    Logger.log(`Error: スプレッドシートが見つかりません。ID: ${config.spreadsheetId}`);
    return;
  }

  // 1. 最新処理タイムスタンプを特定 (UTCナノ秒)
  const lastProcessedNanoTs = findLastProcessedTimestamp_(ss, config.timezoneOffset);
  Logger.log(`最新処理タイムスタンプ (UTCナノ秒): ${lastProcessedNanoTs}`);

  // 2. Lokiクエリの時間範囲を決定
  const lastProcessedSeconds = Math.floor(Number(lastProcessedNanoTs / BigInt(1000000000)))
  const { startSeconds, endSeconds } = calculateLokiTimeRange_(lastProcessedSeconds, config.overlapSeconds);
  Logger.log(`Lokiクエリ時間範囲 (UTCナノ秒): start=${startSeconds}000000000, end=${endSeconds}000000000`);

  // 3. Lokiからログを取得
  const lokiLogs = fetchLogsFromLoki_(config, startSeconds, endSeconds);
  if (!lokiLogs) {
    Logger.log('Lokiからのログ取得に失敗したか、ログがありませんでした。');
    return;
  }
  Logger.log(`Lokiから ${lokiLogs.length} 件のログを取得しました。`);

  // 4. データ前処理とグループ化
  const groupedLogs = preprocessAndGroupLogs_(lokiLogs);
  if (Object.keys(groupedLogs).length === 0) {
    Logger.log('処理対象となる有効なログが見つかりませんでした。');
    return;
  }

  // 5. metric_name ごとにシート処理
  for (const metricName in groupedLogs) {
    if (metricName.startsWith(IGNORED_SHEET_PREFIX)) {
        Logger.log(`シート名が '${IGNORED_SHEET_PREFIX}' で始まるためスキップ: ${metricName}`);
        continue;
    }
    try {
      Logger.log(`処理開始: metric_name = ${metricName}`);
      processMetricGroup_(ss, metricName, groupedLogs[metricName], config.timezoneOffset);
      Logger.log(`処理完了: metric_name = ${metricName}`);
    } catch (e) {
      Logger.log(`Error: metric_name '${metricName}' の処理中にエラーが発生しました。詳細: ${e} \nStack: ${e.stack}`);
      // エラーが発生しても他の metric_name の処理を続行
    }
  }

  Logger.log('全ての処理が完了しました。');
}

// --- 設定関連 ---

/**
 * スクリプトプロパティから設定を読み込む
 * @returns {object|null} 設定オブジェクト。必須項目が欠けている場合はnull
 */
function getConfig_() {
  const props = PropertiesService.getScriptProperties();
  const config = {
    spreadsheetId: props.getProperty(SCRIPT_PROPERTIES_KEYS.SPREADSHEET_ID),
    lokiApiEndpoint: props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_API_ENDPOINT),
    lokiUsername: props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_USERNAME),
    lokiPassword: props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_PASSWORD),
    lokiApiKey: props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_API_KEY),
    baseQuery: props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_BASE_QUERY),
    queryLimit: parseInt(props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_QUERY_LIMIT) || '1000', 10),
    overlapSeconds: parseInt(props.getProperty(SCRIPT_PROPERTIES_KEYS.LOKI_OVERLAP_SECONDS) || '0', 10),
    timezoneOffset: props.getProperty(SCRIPT_PROPERTIES_KEYS.TIMEZONE_OFFSET) || '+00:00',
  };

  // 必須項目のチェック
  if (!config.spreadsheetId || !config.lokiApiEndpoint || !config.baseQuery) {
    Logger.log('Error: 必須の設定項目 (SPREADSHEET_ID, LOKI_API_ENDPOINT, LOKI_BASE_QUERY) が不足しています。');
    return null;
  }

  // Timezone Offset の形式チェック (簡易)
  if (!/^[\+\-]([01]\d|2[0-3]):([0-5]\d)$/.test(config.timezoneOffset)) {
      Logger.log(`Warning: TimeZoneOffset の形式が不正です ('${config.timezoneOffset}')。デフォルトの '+00:00' を使用します。`);
      config.timezoneOffset = '+00:00';
  }

  return config;
}

// --- スプレッドシート操作関連 ---

/**
 * 全シートを走査し、最新のタイムスタンプ (UTCナノ秒 BigInt) を見つける
 * @param {Spreadsheet} ss - 対象のスプレッドシート
 * @param {string} timezoneOffset - スプレッドシート記録時のタイムゾーンオフセット (+HH:MM)
 * @returns {BigInt|null} 最新のタイムスタンプ (UTCナノ秒)。見つからない場合はnull
 */
function findLastProcessedTimestamp_(ss, timezoneOffset) {
  let latestNanoTs = null;
  const sheets = ss.getSheets();

  for (const sheet of sheets) {
    const sheetName = sheet.getName();
    if (sheetName.startsWith(IGNORED_SHEET_PREFIX)) {
      continue; // 無視するシート
    }

    const lastRow = sheet.getLastRow();
    if (lastRow < 2) { // ヘッダーのみ、または空のシート
      continue;
    }

    const header = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const tsColIndex = header.indexOf(TIMESTAMP_COLUMN_NAME);
    if (tsColIndex === -1) {
        Logger.log(`Warning: シート '${sheetName}' にタイムスタンプ列 ('${TIMESTAMP_COLUMN_NAME}') が見つかりません。`);
        continue;
    }

    try {
      // 最終行のタイムスタンプ文字列を取得
      // getLastRow() が最終データ行を返すので、複数行読み取って空行でない最後の行を探す必要はないことが多い
      const lastTimestampStr = sheet.getRange(lastRow, tsColIndex + 1).getValue();

      if (lastTimestampStr && typeof lastTimestampStr === 'string') {
         // ISO文字列をUTCナノ秒にパース
        const currentNanoTs = TimestampUtil_.parseIsoString(lastTimestampStr, timezoneOffset);
        if (currentNanoTs !== null) {
          if (latestNanoTs === null || currentNanoTs > latestNanoTs) {
            latestNanoTs = currentNanoTs;
          }
        } else {
            Logger.log(`Warning: シート '${sheetName}' の最終行のタイムスタンプ文字列 '${lastTimestampStr}' のパースに失敗しました。`);
        }
      }
    } catch (e) {
        // BigInt変換エラーなども考慮
        Logger.log(`Error: シート '${sheetName}' の最終タイムスタンプ処理中にエラー。 ${e}`);
    }
  }
  return latestNanoTs;
}

/**
 * metric_name に対応するシートを取得または作成する
 * @param {Spreadsheet} ss - 対象のスプレッドシート
 * @param {string} metricName - シート名となる metric_name
 * @returns {Sheet} 対象のシートオブジェクト
 */
function getOrCreateSheet_(ss, metricName) {
  let sheet = ss.getSheetByName(metricName);
  if (!sheet) {
    sheet = ss.insertSheet(metricName);
    Logger.log(`シート '${metricName}' を新規作成しました。`);
    // 必要であればヘッダー行を初期化 (最初のデータ書き込み時に行う方が効率的)
    // sheet.appendRow([TIMESTAMP_COLUMN_NAME]); // 例
  }
  return sheet;
}

/**
 * ヘッダー行を更新し、新しいキーがあれば列を追加する
 * @param {Sheet} sheet - 対象のシート
 * @param {string[]} existingHeader - 現在のヘッダー配列
 * @param {string[]} newKeys - 新しいログに含まれる可能性のあるキーのセット
 * @returns {string[]} 更新後のヘッダー配列
 */
function updateHeaderIfNeeded_(sheet, existingHeader, newKeys) {
    const currentHeaderSet = new Set(existingHeader);
    const keysToAdd = newKeys.filter(key => !currentHeaderSet.has(key));

    if (keysToAdd.length > 0) {
        const firstEmptyCol = existingHeader.length + 1;
        sheet.getRange(1, firstEmptyCol, 1, keysToAdd.length).setValues([keysToAdd]);
        Logger.log(`シート '${sheet.getName()}' のヘッダーに新しいキーを追加: ${keysToAdd.join(', ')}`);
        return existingHeader.concat(keysToAdd);
    }
    return existingHeader; // 変更なし
}

/**
 * 整形されたデータをシートに追記する
 * @param {Sheet} sheet - 対象のシート
 * @param {Array<Array<any>>} dataToWrite - 書き込むデータ (2次元配列)
 */
function writeToSheet_(sheet, dataToWrite) {
    if (dataToWrite && dataToWrite.length > 0) {
        const startRow = sheet.getLastRow() + 1;
        const numRows = dataToWrite.length;
        const numCols = dataToWrite[0].length; // ヘッダーに基づいているはず
        sheet.getRange(startRow, 1, numRows, numCols).setValues(dataToWrite);
        Logger.log(`シート '${sheet.getName()}' に ${numRows} 件のログを追記しました。`);
    } else {
        Logger.log(`シート '${sheet.getName()}' への追記データはありません。`);
    }
}

// --- Loki API 関連 ---

/**
 * Lokiクエリの時間範囲 (unix秒) を計算する
 * @param {number|null} lastProcessedSeconds - 最新処理タイムスタンプ (unix秒)
 * @param {number} overlapSeconds - Overlap秒数
 * @returns {{startSeconds: number, endSeconds: number}} クエリの開始・終了時刻 (unix秒)
 */
function calculateLokiTimeRange_(lastProcessedSeconds, overlapSeconds) {
    const nowSeconds = Math.floor(Date.now() / 1000); // 現在時刻 (unix秒)
    let startSeconds = nowSeconds - INITIAL_LOOKBACK_SECONDS;

    if (lastProcessedSeconds !== null && lastProcessedSeconds > nowSeconds - INITIAL_LOOKBACK_SECONDS) {
        // 最後のログが1時間以内なら、最後のログを起点にログを検索する
        startSeconds = lastProcessedSeconds - overlapSeconds;
    }

    // end は指定しない (Lokiサーバーの現在時刻まで)
    // APIによっては end を指定する必要があるかもしれないが、Lokiは省略可能
    const endSeconds = nowSeconds; // 参考情報として返す

    return { startSeconds, endSeconds };
}

/**
 * Loki API からログを取得する
 * @param {object} config - 設定オブジェクト
 * @param {number} startSeconds - クエリ開始時刻 (unix秒)
 * @param {number} endSeconds - クエリ終了時刻 (unix秒、Loki APIでは通常 end は省略可能)
 * @returns {Array<object>|null} Lokiログの配列。失敗時はnull
 */
function fetchLogsFromLoki_(config, startSeconds, endSeconds) {
  // LogQLクエリの構築
  // end は指定しない例
  const query = encodeURIComponent(`${config.baseQuery}`);
  const url = `${config.lokiApiEndpoint}/loki/api/v1/query_range` +
              `?query=${query}` +
              `&start=${startSeconds}000000000` +
              // `&end=${endSeconds}000000000` + // 通常 end は省略可能
              `&limit=${config.queryLimit}` +
              `&direction=forward`; // 古いものから取得

  const options = {
    method: 'get',
    headers: {},
    muteHttpExceptions: true, // エラーレスポンスを例外ではなくオブジェクトとして受け取る
  };

  // 認証情報の設定
  if (config.lokiUsername && config.lokiPassword) {
    const encodedCredentials = Utilities.base64Encode(`${config.lokiUsername}:${config.lokiPassword}`);
    options.headers['Authorization'] = `Basic ${encodedCredentials}`;
  } else if (config.lokiApiKey) {
    options.headers['Authorization'] = `Bearer ${config.lokiApiKey}`; // または他のヘッダー名かも
    // options.headers['X-Api-Key'] = config.lokiApiKey; // Lokiの設定による
  }

  Logger.log(`Lokiクエリ実行: ${url}`);

  try {
    const response = UrlFetchApp.fetch(url, options);
    const responseCode = response.getResponseCode();
    const responseBody = response.getContentText();

    if (responseCode === 200) {
      const jsonResponse = JSON.parse(responseBody);
      if (jsonResponse.status === 'success' && jsonResponse.data && jsonResponse.data.result) {
        // Lokiのレスポンス形式 (query_range) に合わせてログデータを抽出・整形
        const logs = [];
        jsonResponse.data.result.forEach(stream => {
          stream.values.forEach(value => {
            try {
                // value[0] は Unix epoch nano seconds (string)
                // value[1] は ログメッセージ (string)
                const timestampNanoStr = value[0];
                const logLine = value[1];

                // ログメッセージがJSON形式であることを期待
                const logJson = JSON.parse(logLine);

                // Lokiのタイムスタンプをログオブジェクトに追加 (処理しやすいように)
                logJson[TIMESTAMP_COLUMN_NAME] = timestampNanoStr; // BigIntに変換するのは後段で行う
                logs.push(logJson);
            } catch (parseError) {
                Logger.log(`Warning: ログ行のJSONパースに失敗しました。スキップします。Line: ${value[1]}, Error: ${parseError}`);
            }
          });
        });
         // 時系列順 (古い->新しい) にソートされているはずだが、念のためソート
        logs.sort((a, b) => {
            const tsA = BigInt(a[TIMESTAMP_COLUMN_NAME]);
            const tsB = BigInt(b[TIMESTAMP_COLUMN_NAME]);
            if (tsA < tsB) return -1;
            if (tsA > tsB) return 1;
            return 0;
        });
        return logs;
      } else {
        Logger.log(`Error: Loki APIから成功ステータスでない応答がありました。Status: ${jsonResponse.status}, Message: ${jsonResponse.message || responseBody}`);
        return null;
      }
    } else {
      Logger.log(`Error: Loki APIへのリクエストに失敗しました。 Status Code: ${responseCode}, Response: ${responseBody}`);
      return null;
    }
  } catch (e) {
    Logger.log(`Error: Loki APIへの接続またはレスポンス処理中にエラーが発生しました。 ${e}\nStack: ${e.stack}`);
    return null;
  }
}

// --- データ処理関連 ---

/**
 * Lokiログの前処理とmetric_nameごとのグループ化
 * @param {Array<object>} lokiLogs - Lokiから取得したログの配列
 * @returns {object} metric_nameをキー、ログ配列を値とするオブジェクト
 */
function preprocessAndGroupLogs_(lokiLogs) {
  const groupedLogs = {};

  for (const log of lokiLogs) {
    // 1. metric_name の存在確認
    const metricName = log.metric_name;
    if (metricName === undefined || metricName === null || metricName === '') {
      Logger.log(`Warning: 'metric_name' が存在しないログをスキップします。Log: ${JSON.stringify(log)}`);
      continue;
    }

    // 2. metric_name の形式検証
    if (!ALLOWED_METRIC_NAME_REGEX.test(metricName)) {
        Logger.log(`Warning: 'metric_name' ("${metricName}") に許可されない文字が含まれるためスキップします。Log: ${JSON.stringify(log)}`);
        continue;
    }

    // 3. グループ化
    if (!groupedLogs[metricName]) {
      groupedLogs[metricName] = [];
    }
    // タイムスタンプを BigInt に変換して格納
    try {
        log[TIMESTAMP_COLUMN_NAME] = BigInt(log[TIMESTAMP_COLUMN_NAME]);
        groupedLogs[metricName].push(log);
    } catch (e) {
        Logger.log(`Warning: タイムスタンプのBigInt変換に失敗したログをスキップします。Timestamp: ${log[TIMESTAMP_COLUMN_NAME]}, Error: ${e}`);
    }
  }
  return groupedLogs;
}

/**
 * 特定の metric_name のロググループを処理する
 * @param {Spreadsheet} ss - 対象のスプレッドシート
 * @param {string} metricName - 処理対象の metric_name
 * @param {Array<object>} logs - 対象のログ配列 (タイムスタンプはBigInt)
 * @param {string} timezoneOffset - 記録用タイムゾーンオフセット (+HH:MM)
 */
function processMetricGroup_(ss, metricName, logs, timezoneOffset) {
  if (!logs || logs.length === 0) {
    Logger.log(`metric_name '${metricName}' に処理対象ログはありません。`);
    return;
  }

  const sheet = getOrCreateSheet_(ss, metricName);
  const lastRow = sheet.getLastRow();
  let header = [];
  let existingData = []; // シートの既存データ（比較用）

  if (lastRow > 0) {
    const lastCol = sheet.getLastColumn();
    if (lastCol > 0) {
        header = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
    }
  }
  // ヘッダーがない場合は、最初のログのキーから作成する準備
  const initialHeader = header.length > 0 ? header : [TIMESTAMP_COLUMN_NAME, ...Object.keys(logs[0])];

  // 重複排除と追記データの準備
  const { dataToWrite, updatedHeader } = deduplicateAndPrepareData_(sheet, initialHeader, logs, timezoneOffset);

  // ヘッダー更新 (必要な場合)
  if (updatedHeader.length > header.length || header.length === 0) {
      // ヘッダー行が存在しない or 更新が必要な場合
      if (sheet.getLastRow() < 1) {
          // シートが完全に空の場合、最初の行にヘッダーを書き込む
          sheet.getRange(1, 1, 1, updatedHeader.length).setValues([updatedHeader]);
      } else {
          // 既存のヘッダーを更新 (列追加)
          updateHeaderIfNeeded_(sheet, header, updatedHeader); // updatedHeaderには全てのキーが含まれる
      }
  }

  // データの追記
  writeToSheet_(sheet, dataToWrite);
}

/**
 * 重複排除ロジック (## 11.) を実行し、追記するデータを整形する
 * @param {Sheet} sheet - 対象シート
 * @param {string[]} initialHeader - シートの初期ヘッダー (なければ空配列)
 * @param {Array<object>} lokiLogs - Lokiから取得したログ (タイムスタンプはBigInt)
 * @param {string} timezoneOffset - 記録用タイムゾーンオフセット (+HH:MM)
 * @returns {{dataToWrite: Array<Array<any>>, updatedHeader: string[]}} 追記データと最終的なヘッダー
 */
function deduplicateAndPrepareData_(sheet, initialHeader, lokiLogs, timezoneOffset) {
    const sheetName = sheet.getName();
    const lastRow = sheet.getLastRow();
    let sheetLastTsNano = null; // シートの最後のタイムスタンプ (UTCナノ秒)
    let header = [...initialHeader]; // ヘッダーをコピーして使う

    // タイムスタンプ列のインデックス特定
    let tsColIndex = header.indexOf(TIMESTAMP_COLUMN_NAME);

    // シートの最終タイムスタンプを取得 (Case判定用)
    if (lastRow >= 2 && tsColIndex !== -1) {
        try {
            const lastTsStr = sheet.getRange(lastRow, tsColIndex + 1).getValue();
            if (lastTsStr && typeof lastTsStr === 'string') {
                sheetLastTsNano = TimestampUtil_.parseIsoString(lastTsStr, timezoneOffset);
            }
        } catch(e) {
            Logger.log(`Warning: シート '${sheetName}' 最終行タイムスタンプのパースに失敗。 ${e}`);
        }
    } else if (lastRow >= 1 && tsColIndex === -1) {
        // ヘッダーはあるがTS列がない場合 -> ヘッダーに追加
        header.push(TIMESTAMP_COLUMN_NAME);
        tsColIndex = header.length - 1;
        // この場合、実質的に初回書き込みと同じ扱いになる
        sheetLastTsNano = null;
    } else if (lastRow === 0) {
        // シートが完全に空の場合
        if (tsColIndex === -1) {
             header.push(TIMESTAMP_COLUMN_NAME);
             tsColIndex = header.length - 1;
        }
        sheetLastTsNano = null;
    }

    // Lokiログの最も古いタイムスタンプ
    const lokiOldestTsNano = lokiLogs.length > 0 ? lokiLogs[0][TIMESTAMP_COLUMN_NAME] : null; // ソート済み前提

    // Case 1: シートが空 or Lokiの最古ログ > シートの最新ログ
    if (sheetLastTsNano === null || (lokiOldestTsNano !== null && lokiOldestTsNano > sheetLastTsNano)) {
        Logger.log(`Case 1: シートが空か、Lokiログが全て新しい (${lokiOldestTsNano} > ${sheetLastTsNano})。全件追記対象。`);
        // ヘッダー更新チェック
        const allKeys = new Set(header);
        lokiLogs.forEach(log => Object.keys(log).forEach(key => allKeys.add(key)));
        const finalHeader = Array.from(allKeys);
        // データ整形
        const dataToWrite = formatDataForSheet_(lokiLogs, finalHeader, timezoneOffset);
        return { dataToWrite, updatedHeader: finalHeader };
    }

    // --- Case 2 と Case 3' の準備 ---
    // 比較対象のシートデータを読み込む
    let compareSheetData = []; // シートから読み込んだ比較対象行 [{nanoTs: BigInt, hash: string, rowNum: int}]
    let comparisonRange = null; // 読み込むシートの範囲
    let lokiLogsToCompare = []; // 比較対象のLokiログ

    const lokiLogHashes = new Set(); // 比較対象Lokiログのハッシュセット
    lokiLogs.forEach(log => lokiLogHashes.add(LogHasher_.calculateHash(log, header))); // ヘッダー順でハッシュ計算

    // Case 2: Loki最古 == シート最新
    if (lokiOldestTsNano !== null && lokiOldestTsNano === sheetLastTsNano) {
        Logger.log(`Case 2: Loki最古タイムスタンプ (${lokiOldestTsNano}) == シート最新タイムスタンプ。部分比較実行。`);
        lokiLogsToCompare = lokiLogs.filter(log => log[TIMESTAMP_COLUMN_NAME] >= sheetLastTsNano);
        // 比較対象シート範囲: 最終行とその周辺の同じタイムスタンプを持つ可能性のある行
        // 安全のため、少し前から読む (例: 10行前から)
        const startRow = Math.max(2, lastRow - 9); // 2行目から
        const numRows = lastRow - startRow + 1;
        if (numRows > 0 && sheet.getLastColumn() > 0) {
            comparisonRange = sheet.getRange(startRow, 1, numRows, sheet.getLastColumn());
        }
    }
    // Case 3': Loki最古 < シート最新
    else if (lokiOldestTsNano !== null && lokiOldestTsNano < sheetLastTsNano) {
        Logger.log(`Case 3': Loki最古タイムスタンプ (${lokiOldestTsNano}) < シート最新タイムスタンプ (${sheetLastTsNano})。広範囲比較実行。`);
        lokiLogsToCompare = lokiLogs; // 全てのLokiログが比較対象
        // 比較対象シート範囲: タイムスタンプが Loki最古 >= である全ての行
        // **効率化の注意点:** 全行読むのは非効率。バイナリサーチ等で開始行を特定するのが理想だが、
        // GASの制限内で実装が複雑になるため、ここではLoki最古TSを含む可能性のある範囲を読み込む。
        // まずシートのタイムスタンプ列を読み、開始行を特定する。
        let startRow = 2; // デフォルトは2行目
        if (tsColIndex !== -1 && lastRow >= 2) {
            const timestampsCol = sheet.getRange(2, tsColIndex + 1, lastRow - 1, 1).getValues();
            for (let i = 0; i < timestampsCol.length; i++) {
                try {
                    const rowTsStr = timestampsCol[i][0];
                    if (rowTsStr && typeof rowTsStr === 'string') {
                        const rowTsNano = TimestampUtil_.parseIsoString(rowTsStr, timezoneOffset);
                        if (rowTsNano !== null && rowTsNano >= lokiOldestTsNano) {
                            startRow = i + 2; // 発見した行番号 (1ベース)
                            break; // 最初に見つかった行から最後までが対象
                        }
                    }
                } catch (e) { /* パースエラーは無視して進む */ }
            }
            Logger.log(`Case 3': 比較対象シート開始行: ${startRow} (タイムスタンプ >= ${lokiOldestTsNano})`);
        }
        const numRows = lastRow - startRow + 1;
        if (numRows > 0 && sheet.getLastColumn() > 0) {
             comparisonRange = sheet.getRange(startRow, 1, numRows, sheet.getLastColumn());
        }
    } else {
         Logger.log(`Warning: 予期しないタイムスタンプ比較状態。Loki最古=${lokiOldestTsNano}, シート最新=${sheetLastTsNano}`);
         // この場合、安全のため全件比較を行う Case 3' 相当とする
         lokiLogsToCompare = lokiLogs;
         if (lastRow >= 2 && sheet.getLastColumn() > 0) {
              comparisonRange = sheet.getRange(2, 1, lastRow -1, sheet.getLastColumn());
         }
    }

    // シートデータの読み込みとハッシュ化
    const existingLogHashes = new Set();
    if (comparisonRange) {
        const sheetValues = comparisonRange.getValues();
        const currentHeader = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0]; // 最新のヘッダーで
        tsColIndex = currentHeader.indexOf(TIMESTAMP_COLUMN_NAME); // 再取得

        sheetValues.forEach((row, index) => {
            const rowNum = comparisonRange.getRow() + index;
            let rowTsNano = null;
            if (tsColIndex !== -1 && row[tsColIndex] && typeof row[tsColIndex] === 'string') {
                 try {
                     rowTsNano = TimestampUtil_.parseIsoString(row[tsColIndex], timezoneOffset);
                 } catch (e) {/* ignore */}
            }
             // シートの値を元にログオブジェクトを（近似的に）再構築してハッシュ化
            const sheetLog = {};
            currentHeader.forEach((h, colIdx) => {
                // スプレッドシートから読み取った値はそのまま使う
                // JSON文字列化されたものはそのまま文字列としてハッシュ計算に含める
                sheetLog[h] = row[colIdx];
            });
            // タイムスタンプはパースしたナノ秒値で上書き (比較のため)
            // ただし、ハッシュ計算時は元の文字列形式 or BigIntを使う必要がある。
            // ここでは、シートから読み込んだ値をそのまま使ってオブジェクトを再構築しハッシュを計算する
            // ※注意: スプレッドシートの数値型と元のJSONの数値型が完全一致しない可能性あり
            if(rowTsNano !== null) {
                sheetLog[TIMESTAMP_COLUMN_NAME] = rowTsNano; // ハッシュ計算用にBigIntを入れる場合
            }

            const hash = LogHasher_.calculateHashFromSheetRow(row, currentHeader, timezoneOffset); // シート行から直接ハッシュ計算
            existingLogHashes.add(hash);
        });
        Logger.log(`シート '${sheetName}' から比較用に ${existingLogHashes.size} 件のログハッシュを生成しました。`);
    }

    // 重複チェックと追記対象の選定
    const logsToWrite = [];
    const allKeys = new Set(header); // ヘッダー更新のためにキーを集める
    lokiLogs.forEach(log => {
        const logHash = LogHasher_.calculateHash(log, header); // ヘッダー順でハッシュ計算
        if (!existingLogHashes.has(logHash)) {
            logsToWrite.push(log);
            // 新しいログのキーもヘッダー候補に追加
            Object.keys(log).forEach(key => allKeys.add(key));
        }
    });

    Logger.log(`重複チェックの結果、${logsToWrite.length} 件の新しいログが見つかりました。`);

    const finalHeader = Array.from(allKeys).sort((a, b) => { // ソートして順序を安定させる
        if (a === TIMESTAMP_COLUMN_NAME) return -1;
        if (b === TIMESTAMP_COLUMN_NAME) return 1;
        return a.localeCompare(b);
    });

    // データ整形
    const dataToWrite = formatDataForSheet_(logsToWrite, finalHeader, timezoneOffset);

    return { dataToWrite, updatedHeader: finalHeader };
}


/**
 * ログデータをスプレッドシート書き込み用の2次元配列に整形する
 * @param {Array<object>} logs - 書き込むログの配列 (タイムスタンプはBigInt)
 * @param {string[]} header - 使用するヘッダー配列
 * @param {string} timezoneOffset - 記録用タイムゾーンオフセット (+HH:MM)
 * @returns {Array<Array<any>>} 整形後の2次元配列
 */
function formatDataForSheet_(logs, header, timezoneOffset) {
  const data = [];
  const tsColIndex = header.indexOf(TIMESTAMP_COLUMN_NAME);

  for (const log of logs) {
    const row = [];
    for (let i = 0; i < header.length; i++) {
      const key = header[i];
      const value = log[key];

      if (i === tsColIndex && typeof value === 'bigint') {
        // タイムスタンプ列: BigIntをISO文字列に変換
        row.push(TimestampUtil_.formatToIsoString(value, timezoneOffset));
      } else if (value === undefined || value === null) {
        row.push(""); // 欠損キーは空文字列
      } else if (typeof value === 'number') {
        row.push(value); // 数値はそのまま
      } else if (typeof value === 'boolean') {
        row.push(value); // 真偽値はそのまま
      } else if (typeof value === 'object') {
        // 配列やネストされたオブジェクトはJSON文字列として書き込む
        try {
          row.push(JSON.stringify(value));
        } catch (e) {
          Logger.log(`Warning: オブジェクトのJSON文字列化に失敗しました。Key: ${key}, Value: ${value}, Error: ${e}`);
          row.push("[stringify error]");
        }
      } else {
        // 文字列やその他の型はそのまま (BigIntはTS列以外では想定しない)
        row.push(String(value));
      }
    }
    data.push(row);
  }
  return data;
}


// --- ヘルパーモジュール ---

/**
 * タイムスタンプ処理ユーティリティ
 * @namespace
 */
const TimestampUtil_ = {
  /**
   * オフセット文字列 (+HH:MM) をナノ秒単位のオフセット値 (BigInt) に変換する
   * @param {string} offsetString - "+HH:MM" または "-HH:MM" 形式の文字列
   * @returns {BigInt} ナノ秒単位のオフセット値
   * @throws {Error} 不正な形式の場合
   */
  parseOffsetString(offsetString) {
    const match = offsetString.match(/^([\+\-])(\d{2}):(\d{2})$/);
    if (!match) {
      throw new Error(`Invalid timezone offset format: ${offsetString}`);
    }
    const sign = match[1] === '+' ? BigInt(1) : BigInt(-1);
    const hours = BigInt(parseInt(match[2], 10));
    const minutes = BigInt(parseInt(match[3], 10));
    const totalSeconds = (hours * BigInt(3600)) + (minutes * BigInt(60));
    return sign * totalSeconds * BigInt(1000000000); // ナノ秒に変換
  },

  /**
   * ISO 8601 文字列 (ナノ秒精度、オフセット付き) をパースし、UTC基準のUnixナノ秒 (BigInt) を返す
   * @param {string} isoString - "YYYY-MM-DDTHH:mm:ss.nnnnnnnnn+HH:MM" 形式の文字列
   * @param {string} defaultOffset - パース時に使用するデフォルトのオフセット（通常は記録時のもの）
   * @returns {BigInt|null} UTC基準のUnixナノ秒 (BigInt)。パース失敗時はnull
   */
  parseIsoString(isoString, defaultOffset) {
    try {
        // 例: "2023-10-27T10:30:05.123456789+09:00"
        const dateTimePart = isoString.substring(0, 29); // "YYYY-MM-DDTHH:mm:ss.nnnnnnnnn"
        const offsetPart = isoString.substring(29); // "+HH:MM" or "-HH:MM"

        // オフセットをパース（記録時のオフセットを使う）
        // スプレッドシート上の文字列自体に含まれるオフセットを使うべき
        const offsetNano = this.parseOffsetString(offsetPart || defaultOffset);

        // 日付時刻部分をパース
        const year = parseInt(dateTimePart.substring(0, 4), 10);
        const month = parseInt(dateTimePart.substring(5, 7), 10) - 1; // Dateは0-11
        const day = parseInt(dateTimePart.substring(8, 10), 10);
        const hour = parseInt(dateTimePart.substring(11, 13), 10);
        const minute = parseInt(dateTimePart.substring(14, 16), 10);
        const second = parseInt(dateTimePart.substring(17, 19), 10);
        const nanoseconds = parseInt(dateTimePart.substring(20, 29), 10);

        // Dateオブジェクトを使ってミリ秒を取得 (UTCとして解釈させるのが簡単)
        const dateForMs = new Date(Date.UTC(year, month, day, hour, minute, second, 0)); // ミリ秒以下は無視
        const msTimestamp = BigInt(dateForMs.getTime());

        // ミリ秒タイムスタンプにナノ秒部分を加算して、"擬似"ナノ秒タイムスタンプを生成
        const pseudoNanoTs = (msTimestamp * BigInt(1000000)) + BigInt(nanoseconds);

        // オフセットを減算してUTC基準のナノ秒タイムスタンプを得る
        const utcNanoTs = pseudoNanoTs - offsetNano;

        return utcNanoTs;
    } catch (e) {
      Logger.log(`Error parsing ISO string "${isoString}": ${e}`);
      return null;
    }
  },

  /**
   * UTC基準のUnixナノ秒 (BigInt) を、指定オフセット付きのISO 8601 文字列 (ナノ秒精度) に変換する
   * @param {BigInt} utcNanoTs - UTC基準のUnixナノ秒 (BigInt)
   * @param {string} offsetString - "+HH:MM" または "-HH:MM" 形式の文字列
   * @returns {string} "YYYY-MM-DDTHH:mm:ss.nnnnnnnnn+HH:MM" 形式の文字列
   */
  formatToIsoString(utcNanoTs, offsetString) {
    try {
        const offsetNano = this.parseOffsetString(offsetString);

        // UTCナノ秒にオフセットを加算して、指定タイムゾーンでのナノ秒タイムスタンプ（擬似ローカルタイム）を得る
        const localNanoTs = utcNanoTs + offsetNano;

        // ナノ秒タイムスタンプをミリ秒部分とナノ秒部分に分割
        const localMsTs = localNanoTs / BigInt(1000000);
        const nanoPart = Number(localNanoTs % BigInt(1000000)); // Numberにしないとゼロ埋めできない

        // ミリ秒部分からDateオブジェクトを生成 (UTCとして扱う)
        const date = new Date(Number(localMsTs)); // DateコンストラクタはNumberを要求

        // DateオブジェクトのUTC系メソッドで日付と時刻要素を取得
        const year = date.getUTCFullYear();
        const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
        const day = date.getUTCDate().toString().padStart(2, '0');
        const hour = date.getUTCHours().toString().padStart(2, '0');
        const minute = date.getUTCMinutes().toString().padStart(2, '0');
        const second = date.getUTCSeconds().toString().padStart(2, '0');
        const nanoseconds = nanoPart.toString().padStart(9, '0'); // 9桁ゼロ埋め

        // 文字列を組み立て
        const dateTimePart = `${year}-${month}-${day}T${hour}:${minute}:${second}.${nanoseconds}`;
        return `${dateTimePart}${offsetString}`;
    } catch (e) {
        Logger.log(`Error formatting nano timestamp ${utcNanoTs} with offset ${offsetString}: ${e}`);
        return `[Timestamp format error: ${utcNanoTs}]`; // エラーを示す文字列
    }
  },
};


/**
 * ログオブジェクトのハッシュ計算ユーティリティ
 * @namespace
 */
const LogHasher_ = {
  /**
   * ログオブジェクトから正規化された文字列を作成し、SHA-256ハッシュを計算する
   * @param {object} logObject - ログオブジェクト (timestampはBigIntの想定)
   * @param {string[]} headerOrder - ハッシュ計算に使用するキーの順序 (ヘッダー配列)
   * @returns {string} SHA-256ハッシュ値 (16進数文字列)
   */
  calculateHash(logObject, headerOrder) {
    try {
      const valuesToHash = headerOrder.map(key => {
        const value = logObject[key];
        if (value === undefined || value === null) {
          return ''; // 欠損値は空文字
        } else if (typeof value === 'bigint') {
          return value.toString(); // BigIntは文字列化
        } else if (typeof value === 'object') {
          try {
            // オブジェクト/配列はキーをソートしてJSON文字列化 (一貫性のため)
            return JSON.stringify(value, Object.keys(value || {}).sort());
          } catch (e) {
            return '[stringify error]'; // 文字列化失敗
          }
        } else {
          return String(value); // その他は文字列化
        }
      });

      const stringToHash = valuesToHash.join('||'); // 区切り文字で結合
      const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, stringToHash, Utilities.Charset.UTF_8);
      return digest.map(byte => {
        const hex = (byte & 0xFF).toString(16);
        return hex.length === 1 ? '0' + hex : hex;
      }).join('');
    } catch (e) {
        Logger.log(`Error calculating hash for log: ${e} \nLog Snippet: ${JSON.stringify(logObject).substring(0,100)}`);
        // エラー時はユニークな値を返して重複とみなされないようにする（あるいはその逆）
        // ここではランダムな文字列を返して、重複しないようにする
        return `error-hash-${Math.random()}`;
    }
  },

  /**
   * スプレッドシートの行データから直接ハッシュを計算する（効率化のため）
   * @param {Array<any>} rowData - シートの1行分のデータ配列
   * @param {string[]} header - 対応するヘッダー配列
   * @param {string} timezoneOffset - タイムスタンプパース用オフセット
   * @returns {string} SHA-256ハッシュ値 (16進数文字列)
   */
   calculateHashFromSheetRow(rowData, header, timezoneOffset) {
        try {
            const tsColIndex = header.indexOf(TIMESTAMP_COLUMN_NAME);
            const valuesToHash = rowData.map((value, index) => {
                if (index === tsColIndex && typeof value === 'string') {
                    // タイムスタンプ列はBigIntにパースして文字列化する
                    const nanoTs = TimestampUtil_.parseIsoString(value, timezoneOffset);
                    return nanoTs !== null ? nanoTs.toString() : ''; // パース失敗時は空文字
                } else if (value === null || value === undefined) {
                    return '';
                } else {
                    // 他の列はそのまま文字列化
                    // Note: スプレッドシート上の数値や真偽値も文字列として扱われる
                    return String(value);
                }
            });

            const stringToHash = valuesToHash.join('||');
            const digest = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, stringToHash, Utilities.Charset.UTF_8);
            return digest.map(byte => {
                const hex = (byte & 0xFF).toString(16);
                return hex.length === 1 ? '0' + hex : hex;
            }).join('');
        } catch (e) {
            Logger.log(`Error calculating hash from sheet row: ${e} \nRow Snippet: ${rowData.slice(0,5).join(', ')}`);
            return `error-hash-row-${Math.random()}`;
        }
    }
};

// --- clasp 用の設定ファイル (参考: project/.clasp.json) ---
/*
{
  "scriptId":"YOUR_SCRIPT_ID_HERE",
  "rootDir":"./src", // コードを置くディレクトリ
  "projectId":"YOUR_GCP_PROJECT_ID_HERE", // オプション
  "fileExtension":"js",
  "parentId": ["YOUR_PARENT_FOLDER_ID_HERE"] // オプション: Google DriveのフォルダID
}
*/

// --- スクリプトプロパティ設定方法 (参考) ---
/*
1. スクリプトエディタを開く
2. 左側のメニューから「プロジェクトの設定」（歯車アイコン）を選択
3. 「スクリプト プロパティ」セクションで「スクリプト プロパティを編集」をクリック
4. 以下のキーと対応する値を設定します:
   - SPREADSHEET_ID: 操作対象のスプレッドシートID
   - LOKI_API_ENDPOINT: LokiのクエリAPIエンドポイントURL (例: https://your-loki.com/loki/api/v1)
   - LOKI_USERNAME: (オプション) Basic認証のユーザー名
   - LOKI_PASSWORD: (オプション) Basic認証のパスワード
   - LOKI_API_KEY: (オプション) BearerトークンなどのAPIキー
   - LOKI_BASE_QUERY: 基本となるLogQLクエリ (例: {job="your-app"}) 時間範囲は含まない
   - LOKI_QUERY_LIMIT: 1回のクエリで取得する最大件数 (例: 1000)
   - LOKI_OVERLAP_SECONDS: Overlap秒数 (例: 300 で5分、0でOverlapなし)
   - TIMEZONE_OFFSET: 記録時のタイムゾーンオフセット (例: "+09:00", "-05:00", "+00:00")
5. 「保存」をクリック
*/
