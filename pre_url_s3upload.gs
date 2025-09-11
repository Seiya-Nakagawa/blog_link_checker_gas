/**
 * @fileoverview blog_link_checker_gasの事前処理を担当するGoogle Apps Scriptです。
 * 詳細は docs/【ブログリンクチェッカー】要件定義書.md を参照してください。
 */

// =============================================================================
// --- グローバル設定 ---
// =============================================================================

// NOTE: 本番環境ではスクリプトプロパティで管理することを推奨します。
const SPREADSHEET_ID_WORK = '1WlAmsQbPrjdJ-yux84ZUP8_8jbjOGA6qiiUrmTHmLaA'; // 作業用スプレッドシートID
const SPREADSHEET_ID_SOURCE = '1ZrK7n8QaQ3spmug-spKcK0vgMDDw-VDfRshwlGY0wyo'; // 原本スプレッドシートID
const S3_BUCKET_NAME = 'blog-prd-s3'; // アップロード先のS3バケット名
const S3_FILE_NAME = 'gas_urls/urls_list.json'; // S3に保存するファイル名（フォルダパスを含む）

// 通知先メールアドレスのリスト
// 複数のアドレスを指定する場合はカンマ区切り: ['email1@example.com', 'email2@example.com']
// 空のリスト[]にした場合、スクリプト実行者のメールアドレスに送信されます。
const EMAIL_RECIPIENTS = ['mvickey369@gmail.com', 'aibdlnew1.work@gmail.com'];

// シート名
const SHEET_NAME_SUMMARY = '前回URLチェック件数';
const SHEET_NAME_LOG = '実行結果ログ';
const SHEET_NAME_TARGET = 'ブログ一覧';


// =============================================================================
// --- メイン処理 ---
// =============================================================================

/**
 * AWS Lambdaでのリンクチェックに必要な事前処理を実行するメイン関数。
 */
function main() {
  try {
    // 1. 前回実行結果の取得
    const previousData = getPreviousExecutionData_();

    // 2. チェック対象リストの更新と取得
    const rawTargetUrls = getLatestTargetUrls_();

    // 3. URL件数の比較と通知
    const currentUrlCount = rawTargetUrls.length;
    const previousUrlCount = previousData.linkCount || 0;
    Logger.log(`URL件数の比較: 前回=${previousUrlCount}件, 今回=${currentUrlCount}件`);
    if (currentUrlCount !== previousUrlCount) {
      Logger.log(`URL件数に変動がありました。差分: ${currentUrlCount - previousUrlCount}件`);
      sendDifferenceReport_(previousUrlCount, currentUrlCount);
    } else {
      Logger.log('URL件数に変動はありませんでした。');
    }

    // 4. 作業用スプレッドシートの実行サマリーシートを更新
    updateSummarySheet_(currentUrlCount);

    // 5. Lambdaが期待する形式にデータを変換
    const formattedTargetUrls = rawTargetUrls.map(function(row) {
      return { "url": row[2] }; // 各行の3番目(インデックス2)の要素がURL
    });

    // 6. S3へのデータアップロード
    const uploadData = {
      latest_target_url_list: formattedTargetUrls,
      previous_error_details: previousData.errors,
    };
    uploadToS3_(S3_FILE_NAME, uploadData);

    Logger.log('事前処理が正常に完了しました。ファイル名: %s', S3_FILE_NAME);

  } catch (e) {
    Logger.log('事前処理中にエラーが発生しました: %s', e.message);
    sendErrorNotification_(e);
    throw e;
  }
}


// =============================================================================
// --- ヘルパー関数 ---
// =============================================================================

/**
 * 作業用スプレッドシートから前回実行時の各種データを取得します。
 * @returns {{linkCount: number, errors: string[][]}} 前回総リンク数とエラー詳細リスト
 * @private
 */
function getPreviousExecutionData_() {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID_WORK);

  const summarySheet = ss.getSheetByName(SHEET_NAME_SUMMARY);
  const linkCount = summarySheet ? summarySheet.getRange('A1').getValue() : 0;

  const logSheet = ss.getSheetByName(SHEET_NAME_LOG);
  const errors = (logSheet && logSheet.getLastRow() > 0) ? logSheet.getDataRange().getValues() : [];

  Logger.log('前回実行結果を取得しました。総リンク数: %s, エラー数: %s', linkCount, errors.length);

  return {
    linkCount: linkCount,
    errors: errors,
  };
}

/**
 * ★★★ 修正後の関数 ★★★
 * 原本スプレッドシートから「ブログ一覧」シートの全データを取得し、作業用シートにそのままコピーします。
 * 後続処理のために、チェック対象となるURLリストも返します。
 * @returns {string[][]} チェック対象のURLリスト（ヘッダー行を除き、URLが空でない行）
 * @private
 */
function getLatestTargetUrls_() {
  const sourceSs = SpreadsheetApp.openById(SPREADSHEET_ID_SOURCE);
  const sourceSheet = sourceSs.getSheetByName(SHEET_NAME_TARGET);
  if (!sourceSheet) {
    throw new Error(`原本スプレッドシートに '${SHEET_NAME_TARGET}' シートが見つかりません。`);
  }

  // コピー元のシートにデータがあるかチェック
  if (sourceSheet.getLastRow() === 0) {
    Logger.log(`'${SHEET_NAME_TARGET}' シートにデータがありません。作業用シートをクリアします。`);
    const workSsOnEmpty = SpreadsheetApp.openById(SPREADSHEET_ID_WORK);
    let workSheetOnEmpty = workSsOnEmpty.getSheetByName(SHEET_NAME_TARGET);
    if (workSheetOnEmpty) {
      workSheetOnEmpty.clear();
    }
    return [];
  }

  // 原本シートのデータ範囲全体を取得（ヘッダーや空行も含む）
  const sourceData = sourceSheet.getDataRange().getValues();

  const workSs = SpreadsheetApp.openById(SPREADSHEET_ID_WORK);
  let workSheet = workSs.getSheetByName(SHEET_NAME_TARGET);
  if (!workSheet) {
    workSheet = workSs.insertSheet(SHEET_NAME_TARGET);
  }

  // 作業用シートの内容を一旦すべてクリア（書式なども含む）
  workSheet.clear();

  // 取得したデータを作業用シートにそのままコピー
  if (sourceData.length > 0) {
    workSheet.getRange(1, 1, sourceData.length, sourceData[0].length).setValues(sourceData);
  }

  Logger.log(`'${SHEET_NAME_TARGET}' シートをそのままコピーしました。総行数: %s`, sourceData.length);

  // 後続の処理（URL件数比較やS3アップロード）のために、チェック対象となるURLリストを生成して返す。
  // 元のコードの挙動に合わせて、1行目のヘッダーを除き、3列目にURLが存在する行のみを対象とする。
  const targetUrls = sourceData.slice(1).filter(row => row[2] && String(row[2]).trim() !== '');
  Logger.log('チェック対象のURLを抽出しました。URL数: %s', targetUrls.length);

  return targetUrls;
}


/**
 * 作業用スプレッドシートのサマリーシートを更新します。
 * @param {number} currentUrlCount - 今回のURL件数
 * @private
 */
function updateSummarySheet_(currentUrlCount) {
  const ss = SpreadsheetApp.openById(SPREADSHEET_ID_WORK);
  const summarySheet = ss.getSheetByName(SHEET_NAME_SUMMARY);
  if (summarySheet) {
    summarySheet.getRange('A1').setValue(currentUrlCount);
    Logger.log(`実行サマリーシートのA1セルを現在のURL件数(${currentUrlCount}件)で更新しました。`);
  } else {
    Logger.log(`警告: '${SHEET_NAME_SUMMARY}' シートが見つかりませんでした。A1セルの更新をスキップします。`);
  }
}

/**
 * データをJSON形式でS3にアップロードします。
 * NOTE: 事前にGASライブラリ「S3」の導入が必要です。
 * (ライブラリID: 1V_l4xN3ICa0lAW315N-g23gGF3AF_x-wX_v1_p_s2m3x_A_6m38VnKsc)
 * @param {string} fileName - アップロードするファイル名
 * @param {object} data - アップロードするデータ（JSONオブジェクト）
 * @private
 */
function uploadToS3_(fileName, data) {
  const s3 = S3.getInstance(
    PropertiesService.getScriptProperties().getProperty('AWS_ACCESS_KEY_ID'),
    PropertiesService.getScriptProperties().getProperty('AWS_SECRET_ACCESS_KEY')
  );

  const blob = Utilities.newBlob(JSON.stringify(data, null, 2), 'application/json', fileName);
  const response = s3.putObject(S3_BUCKET_NAME, fileName, blob, { log: true });

  Logger.log('S3へのアップロードが完了しました。レスポンス: %s', response);
}

/**
 * URL件数の差分をメールで通知します。
 * @param {number} previousUrlCount - 前回のURL件数
 * @param {number} currentUrlCount - 今回のURL件数
 * @private
 */
function sendDifferenceReport_(previousUrlCount, currentUrlCount) {
  const recipients = EMAIL_RECIPIENTS && EMAIL_RECIPIENTS.length > 0 ? EMAIL_RECIPIENTS.join(',') : Session.getActiveUser().getEmail();
  const subject = 'ブログURL件数変動のお知らせ';
  const body = `ブログのURL件数に変動がありました。

- 前回の件数: ${previousUrlCount}件
- 今回の件数: ${currentUrlCount}件
- 差分: ${currentUrlCount - previousUrlCount}件

スプレッドシートをご確認ください。`;

  MailApp.sendEmail(recipients, subject, body);
  Logger.log('件数変動の通知メールを送信しました。宛先: %s', recipients);
}

/**
 * 実行時エラーをメールで通知します。
 * @param {Error} error - 発生したエラーオブジェクト
 * @private
 */
function sendErrorNotification_(error) {
  try {
    const recipients = EMAIL_RECIPIENTS && EMAIL_RECIPIENTS.length > 0 ? EMAIL_RECIPIENTS.join(',') : Session.getActiveUser().getEmail();
    const subject = '【エラー】ブログリンクチェッカー事前処理でエラーが発生しました';
    const body = `Google Apps Scriptの実行中にエラーが発生しました。

エラーメッセージ:
${error.message}

スタックトレース:
${error.stack}

ログをご確認ください。`;

    MailApp.sendEmail(recipients, subject, body);
    Logger.log('エラー通知メールを送信しました。');
  } catch (e) {
    Logger.log('エラー通知メールの送信自体に失敗しました: %s', e.message);
  }
}