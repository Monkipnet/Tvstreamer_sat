'use strict';
const assert = require('assert').strict || require('assert');
const {webcrypto} = require('crypto');
const preview = require('../web/preview/preview-player.js');
const requests = [], urls = [], closedPlayers = [];
const listeners = {};
function fakeElement(tag) {
  return {
    tagName: tag, className: '', parentNode: null,
    setAttribute() {}, removeAttribute() {}, addEventListener() {}, removeEventListener() {},
    append() {}, remove() {}, pause() {}, load() {}, focus() {},
    closest() { return null; }
  };
}
global.document = {
  body: fakeElement('body'), activeElement: fakeElement('button'),
  createElement: fakeElement, addEventListener() {}, removeEventListener() {}
};
global.window = {
  location: new URL('http://localhost:8880/'), crypto: webcrypto,
  addEventListener(name, cb) { listeners[name] = cb; },
  removeEventListener(name) { delete listeners[name]; },
  mpegts: {
    Events: {ERROR: 'ERROR'}, getFeatureList() {return {mseLivePlayback: true};},
    createPlayer(spec) {
      urls.push(spec.url);
      return {
        on() {}, attachMediaElement() {}, load() {}, play() {return Promise.resolve();},
        pause() {}, unload() {}, detachMediaElement() {},
        destroy() { closedPlayers.push(spec.url); }
      };
    }
  }
};
global.fetch = (url, opts) => {
  requests.push({url, opts});
  return Promise.resolve({ok: true, json: () => Promise.resolve({})});
};
const instance = preview.install({resolve: id => Promise.resolve({active:true,sources:[{
  kind:'http', preview_kind:'mpegts', label:'Временный HTTP MPEG-TS',
  preview_url:'/api/streams/'+id+'/preview.ts'
}]})});
async function drain() { for (let i=0;i<6;i++) await Promise.resolve(); }
(async () => {
  instance.open('stream-one', 'one'); await drain();
  assert.equal(urls.length, 1);
  const first = new URL(urls[0]);
  const token1 = first.searchParams.get('session');
  assert.match(token1, /^[0-9a-f]{32}$/);
  instance.open('stream-two', 'two'); await drain();
  assert.equal(closedPlayers.length,1);
  assert.equal(requests.length, 1);
  assert.equal(requests[0].url, '/api/streams/stream-one/preview/close');
  assert.deepEqual(JSON.parse(requests[0].opts.body), {session: token1});
  assert.equal(requests[0].opts.keepalive, true);
  const second = new URL(urls[1]);
  assert.notEqual(second.searchParams.get('session'), token1);
  listeners.pagehide();
  assert.equal(requests.length, 2);
  assert.equal(requests[1].url, '/api/streams/stream-two/preview/close');
  assert.equal(closedPlayers.length, 2);
  // Idempotent: double-close and destroy do not issue duplicate requests.
  instance.close(); instance.destroy();
  assert.equal(requests.length, 2);
  // A configured public HTTP stream must never be affected by private-close POST.
  const configured = preview.install({resolve: () => Promise.resolve({sources:[{
    kind:'http',preview_kind:'mpegts',preview_url:'/stream/stream-three.ts'
  }]})});
  configured.open('stream-three'); await drain();
  configured.close(); configured.destroy();
  assert.equal(requests.length, 2);
  console.log('PASS: session-isolated forced preview close, reconnect, pagehide, idempotence, public HTTP isolation');
})().catch(error => {console.error(error); process.exitCode=1;});
