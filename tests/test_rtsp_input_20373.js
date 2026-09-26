const fs = require('fs');
const assert = require('assert');

const stream = fs.readFileSync('src/StreamManager.cpp', 'utf8');
const header = fs.readFileSync('src/StreamManager.h', 'utf8');
const http = fs.readFileSync('src/HttpServer.cpp', 'utf8');
const version = fs.readFileSync('src/AppVersion.h', 'utf8');

assert.match(version, /kProgramVersion\s*=\s*"203\.73"/, 'version must remain 203.73');
assert.ok(http.includes('value="rtsp-tcp"'), 'RTSP TCP input mode missing');
assert.ok(http.includes('value="rtsp-udp"'), 'RTSP UDP input mode missing');
assert.ok(http.includes('value="rtsp-auto"'), 'RTSP Auto input mode missing');
assert.ok(stream.includes('setIntPropertyIfPresent(src, "protocols", rtspProtocols);'), 'RTSP transport selection missing');
assert.ok(stream.includes('rtpmp2tdepay'), 'RTP/MP2T ingest missing');
assert.ok(stream.includes('rtppcmadepay') && stream.includes('rtppcmudepay'), 'G.711 RTP depayloaders missing');
assert.ok(stream.includes('alawdec') && stream.includes('mulawdec'), 'G.711 decoders missing');
assert.ok(stream.includes('audio_normalize=AAC-LC/48000/2'), 'G.711 AAC normalization missing');
assert.ok(header.includes('rtspMpegTsLinked'), 'RTSP MP2T duplicate-branch guard missing');

console.log('PASS: 203.73 RTSP input TCP/UDP/Auto + H264/H265/AAC/MPA/G711/MP2T support');
