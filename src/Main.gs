function myFunction() {
  // スプレッドシートが未紐付けでも動くように：無ければ自動作成
  const ss = SpreadsheetApp.getActiveSpreadsheet() || SpreadsheetApp.create('GAS Test');
  const sh = ss.getActiveSheet();
  sh.getRange(1,1).setValue('Hello from GAS @ ' + new Date());
}
function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Custom')
    .addItem('Run myFunction', 'myFunction')
    .addToUi();
}
