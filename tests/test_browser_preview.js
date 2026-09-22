/* Temporary HTTP preview contract: run with node tests/test_browser_preview.js. */
'use strict';
const assert = require('assert').strict || require('assert');
const fs = require('fs');
const path = require('path');
const js = fs.readFileSync(path.join(__dirname, '../web/preview/preview-player.js'), 'utf8');
const cpp = fs.readFileSync(path.join(__dirname, '../src/HttpServer.cpp'), 'utf8');
const streamManager = fs.readFileSync(path.join(__dirname, '../src/StreamManager.cpp'), 'utf8');
const transcoder = fs.readFileSync(path.join(__dirname, '../src/GstTranscoderProcess.cpp'), 'utf8');
const httpOutput = fs.readFileSync(path.join(__dirname, '../src/protocols/outputs/GstHttpOutputProtocol.cpp'), 'utf8');
const css = fs.readFileSync(path.join(__dirname, '../web/preview/preview-player.css'), 'utf8');
const preview = require('../web/preview/preview-player.js');

const configured = {sources: [
  {kind: 'http', preview_kind: 'mpegts', preview_url: '/stream/stream-1.ts', label: 'HTTP MPEG-TS'}
]};
const temporary = {sources: [
  {kind: 'http', preview_kind: 'mpegts', preview_url: '/api/streams/stream-2/preview.ts', label: 'Временный HTTP MPEG-TS'}
]};
assert.deepEqual(preview.chooseHttpSource(configured), {label: 'HTTP MPEG-TS', url: '/stream/stream-1.ts'});
assert.deepEqual(preview.chooseHttpSource(temporary), {label: 'Временный HTTP MPEG-TS', url: '/api/streams/stream-2/preview.ts'});
assert.equal(preview.chooseHttpSource({sources: [{kind: 'hls', preview_kind: 'hls', preview_url: '/hls/x/video.m3u8'}]}), null);
assert.equal(preview.safeBrowserUrl('/api/streams/stream-2/preview.ts', 'https://host:8880/'), 'https://host:8880/api/streams/stream-2/preview.ts');
assert.throws(() => preview.safeBrowserUrl('srt://host:1234', 'https://host:8880/'), /HTTP/);
assert.throws(() => preview.safeBrowserUrl('http://other/stream.ts', 'http://host/'), /same-origin/);

assert.ok(cpp.includes(js), 'JS in src/HttpServer.cpp must match web/preview/preview-player.js');
assert.ok(cpp.includes(css), 'CSS in src/HttpServer.cpp must match web/preview/preview-player.css');
assert.ok(cpp.includes('resolvePrivatePreviewTarget'));
assert.ok(cpp.includes('"/api/streams/" + cleanId + "/preview.ts"'));
assert.ok(cpp.includes('if (target.rfind("/api/streams/", 0) == 0) return true;'));
assert.ok(cpp.includes('<script src="/preview/mpegts.min.js" defer></script>'));
assert.ok(!cpp.includes('<script src="/preview/hls.min.js" defer></script>'));
assert.ok(!js.includes('new window.Hls('));
assert.ok(!js.includes('tvp-choices'));
assert.ok(!css.includes('.tvp-choice'));

assert.ok(streamManager.includes('preview.outputHost = "127.0.0.1";'));
assert.ok(streamManager.includes('privateDemand->fetch_add(1, std::memory_order_relaxed);'));
assert.ok(streamManager.includes('privateDemand->fetch_sub(1, std::memory_order_relaxed);'));
assert.ok(streamManager.includes('GST_PAD_PROBE_DROP'));
assert.ok(streamManager.includes('const bool privatePreview = type == "http"'));
assert.ok(fs.readFileSync(path.join(__dirname, '../src/StreamManager.h'), 'utf8').includes('privatePreviewDemand'));
assert.ok(streamManager.includes('preview.outputPort = 0;'));
assert.ok(streamManager.includes('!found->second->active.load() || !found->second->running.load()'));
assert.ok(transcoder.includes('output.temporaryPreview ? 1000000000ULL : 3000000000ULL'));
assert.ok(httpOutput.includes('privatePreview ? 1000000000ULL : 8000000000ULL'));
assert.ok(httpOutput.includes('privatePreview)'));
console.log('PASS: idle in-process private HTTP preview is gated at tee, with active-client lifetime tracking; HTML/player contract preserved');
