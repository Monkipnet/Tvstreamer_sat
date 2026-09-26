const fs = require('fs');
const assert = require('assert');

const http = fs.readFileSync('src/HttpServer.cpp', 'utf8');
const version = fs.readFileSync('src/AppVersion.h', 'utf8');

assert.match(version, /kProgramVersion\s*=\s*"203\.73"/,
  'numeric version must stay 203.73');
assert.match(http, /state\.program_version\|\|'203\.73'.*language === 'en' \? ' EN' : ''/s,
  'English UI must append EN only to displayed version');

for (const pair of [
  ["'Интерфейс вывода', 'Output interface'", 'output interface'],
  ["'Выходные форматы', 'Output formats'", 'output formats'],
  ["'URL для плеера', 'Player URL'", 'player URL'],
  ["'Транскодирование', 'Transcoding'", 'transcoding'],
  ["'Добавить канал — DVB-S/S2', 'Add channel — DVB-S/S2'", 'satellite modal'],
  ["'Качество потока', 'Stream quality'", 'quality modal'],
  ["'HTTP-предпросмотр', 'HTTP preview'", 'preview UI']
]) {
  assert.ok(http.includes(pair[0]), `missing English translation for ${pair[1]}`);
}

assert.ok(http.includes('applyRuntimeLocalization(document.body);'),
  'language switching must localize dynamically generated UI');
assert.ok(http.includes("window.alert = message => nativeUiAlert(translateUiText(message));"),
  'alerts must be localized');
assert.ok(http.includes("window.confirm = message => nativeUiConfirm(translateUiText(message));"),
  'confirm dialogs must be localized');
assert.ok(http.includes("document.documentElement.lang = language;"),
  'document lang must follow selected language');

const scriptStart = http.indexOf('<script>') + '<script>'.length;
const scriptEnd = http.indexOf('</script>', scriptStart);
assert.ok(scriptStart > 0 && scriptEnd > scriptStart, 'inline script not found');
const script = http.slice(scriptStart, scriptEnd);
new Function(script); // syntax-only compile; does not execute DOM code.

console.log('PASS: 203.73 English UI runtime localization and display-only EN suffix');
