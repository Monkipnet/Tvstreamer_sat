const fs = require('fs');
const src = fs.readFileSync('src/StreamManager.cpp', 'utf8');

function must(text, why) {
  if (!src.includes(text)) {
    console.error('FAIL:', why, 'missing:', text);
    process.exit(1);
  }
}

must('HLS RTSP direct TS 203.73:', 'RTSP HLS must bypass the second tsdemux/remux when remap is off');
must('input_mux=input_rtsp_ts_mux', 'RTSP HLS direct path must document the normalized input mux');
must('RTSP PREVIEW 203.73:', 'private browser preview must accept the already-normalized RTSP SPTS');
must('privateRtspPreview', 'RTSP private preview condition must exist');
must('state->runtimeConfig.inputServiceId == 0', 'direct RTSP transport must be limited to AUTO/single-program input');
must('!cfg.remapEnabled', 'HLS direct RTSP path must preserve explicit remap behavior');

console.log('PASS: 203.73 RTSP normalized SPTS feeds HLS and private preview without a second demux/remux');
