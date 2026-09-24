'use strict';
const assert = require('node:assert/strict');
const fs = require('node:fs');
const html = fs.readFileSync('src/HttpServer.cpp', 'utf8');
const moduleCpp = fs.readFileSync('src/TranscoderModule.cpp', 'utf8');
const processCpp = fs.readFileSync('src/GstTranscoderProcess.cpp', 'utf8');
for (const [value, rate] of [['1024x576', 2500], ['720x576_16_9', 2000]]) {
  assert(html.includes(`<option value="${value}"`), `missing UI preset ${value}`);
  assert(html.includes(`'${value}':${rate}`), `missing UI bitrate preset ${value}`);
  assert(moduleCpp.includes(`if (value == "${value}") return ${rate * 1000};`));
}
assert(moduleCpp.includes('geometry.pixelAspectNum, geometry.pixelAspectDen'),
       'in-process transcoder must use configured SAR');
assert(processCpp.includes('scaledVideoCaps(geometry, encoderFactory)'),
       'shared transcoder must pass same geometry to video caps');
assert(html.includes('<option value="720x576"'), 'must preserve legacy SD preset');
console.log('PASS: UI SD 16:9 presets, bitrate defaults, both transcoder paths');
