const assert = require('assert').strict;
const fs = require('fs');
const path = require('path');
function read(p){return fs.readFileSync(path.join(__dirname, '..', p), 'utf8')}
const script=read('scripts/build_oscam_mini.sh');
const manager=read('src/OscamMiniManager.cpp');
const vendored=read('third_party/oscam-mini/csctapi/ifd_pcsc.c');
assert.match(script, /pkg-config --exists libpcsclite/);
assert.match(script, /-DHAVE_PCSC=1/);
assert.match(script, /--enable MODULE_NEWCAMD readers CARDREADER_PHOENIX/);
assert.match(vendored, /reader_nb = atoi/);
assert.match(manager, /reader.protocol == "pcsc"/);
assert.match(manager, /reader.protocol != "pcsc"/);
assert.match(manager, /pcscIndex/);
assert.match(manager, /r.protocol==='pcsc'/);
assert.match(manager, /pcsc \? "none"/);
console.log('PASS: PC/SC build/dependency, indexed reader validation and UI checks');
