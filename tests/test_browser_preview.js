/* Browser-preview contract smoke test: run with node tests/test_browser_preview.js. */
'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const src = fs.readFileSync(path.join(__dirname, '..', 'web/preview/preview-player.js'), 'utf8');
const cpp = fs.readFileSync(path.join(__dirname, '..', 'src/HttpServer.cpp'), 'utf8');
const preview = require('../web/preview/preview-player.js');
const example = preview.normalizeSources({sources: [
  {id:'output-0', kind:'srt', preview_kind:'hls', preview_url:'/hls/stream-1/video.m3u8', label:'SRT → общий HLS'},
  {id:'output-1', kind:'http', preview_kind:'mpegts', preview_url:'/stream/stream-1.ts', label:'HTTP MPEG-TS'},
  {id:'output-2', kind:'hls', preview_kind:'hls', preview_url:'/hls/stream-1/video.m3u8', label:'HLS'},
  {id:'output-3', kind:'srt', label:'SRT без HTTP/HLS', url:'srt://host:1234'}
]});
assert.equal(example.length, 4);
assert.equal(example.filter(preview.canPreview).length, 3);
assert.equal(example[3].url, null);
assert.equal(example[1].playKind, 'mpegts');
assert.equal(preview.safeBrowserUrl('/stream/stream-1.ts','https://host:9000/'), 'https://host:9000/stream/stream-1.ts');
assert.throws(() => preview.safeBrowserUrl('srt://host:1234','https://host:9000/'), /HTTP/);
assert.throws(() => preview.safeBrowserUrl('http://other/stream.ts','http://host/'), /same-origin/);
assert.ok(cpp.includes("tile.dataset.streamName = String(stream.name || stream.id);"));
assert.ok(cpp.includes('const auto outputs = streamOutputs(*cfg);'));
assert.ok(cpp.includes('manifest["sources"] = std::move(sources);'));
assert.ok(cpp.includes('window.streamPreview = TVStreammerPreview.install'));
assert.ok(cpp.includes('readPreviewVendorLibrary'));
assert.ok(cpp.includes(src));
assert.ok(cpp.includes('<script src="/preview/hls.min.js" defer></script>'));
assert.ok(cpp.includes('<script src="/preview/mpegts.min.js" defer></script>'));
console.log('PASS: integrated UI, four-output selection, HTTP TS, HLS shared preview, unavailable SRT and URL safety');
