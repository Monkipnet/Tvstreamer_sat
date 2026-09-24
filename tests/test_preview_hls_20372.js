'use strict';
const assert = require('assert').strict;
const { webcrypto } = require('crypto');
const preview = require('../web/preview/preview-player.js');
const requests = [], loaded = [], hlsDestroyed = [];
const listeners = {};
function fakeElement(tag) {
  return {tagName:tag, className:'', setAttribute(){}, removeAttribute(){},
    addEventListener(){}, removeEventListener(){}, append(){}, remove(){},
    pause(){}, load(){}, focus(){}, canPlayType(){return '';},
    play(){return Promise.resolve();}, closest(){return null;}};
}
global.document = {body:fakeElement('body'), activeElement:fakeElement('button'),
  createElement:fakeElement, addEventListener(){}, removeEventListener(){}};
class FakeHls {
  static isSupported(){return true}
  static Events={ERROR:'error'}
  on(){}
  attachMedia(video){ this.video=video }
  loadSource(url){loaded.push(url)}
  destroy(){hlsDestroyed.push(this.video)}
}
global.window={location:new URL('http://localhost:8880/'), crypto:webcrypto,
  addEventListener(name, cb){listeners[name]=cb}, removeEventListener(name){delete listeners[name]},
  Hls:FakeHls};
global.fetch=(url,opts)=>{requests.push({url,opts});return Promise.resolve({ok:true,json:()=>Promise.resolve({})})};
const ui=preview.install({resolve:()=>Promise.resolve({active:true, input_is_hls:true, sources:[
  {kind:'http',preview_kind:'mpegts',preview_url:'/api/streams/id/preview.ts'},
  {kind:'hls',preview_kind:'hls',preview_url:'/channel/video.m3u8',label:'HLS'}
]})});
async function drain(){for(let i=0;i<8;i++)await Promise.resolve()}
(async()=>{
  ui.open('id'); await drain();
  assert.deepEqual(loaded,['http://localhost:8880/channel/video.m3u8']);
  assert.equal(requests.length,0,'public HLS preview should not open private MPEG-TS session');
  ui.close();
  assert.equal(hlsDestroyed.length,1);
  assert.equal(requests.length,0,'no private close for configured HLS');
  ui.destroy();
  console.log('PASS: input HLS chooses configured playlist, destroys viewer session, no private relay');
})().catch(e=>{console.error(e);process.exitCode=1});
