/* TVStreammerSAT5 browser preview: choose an existing HTTP MPEG-TS output,
 * or the private on-demand HTTP session backed by the same channel pipeline.
 * Production SRT/HLS/UDP output configurations are not modified by the UI.
 */
(function (global, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  global.TVStreammerPreview = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  function chooseHttpSource(payload) {
    var sources = Array.isArray(payload) ? payload : payload && payload.sources;
    if (!Array.isArray(sources)) throw Error('Ответ preview API должен содержать массив sources');
    var source = sources.find(function (s) {
      return s && String(s.kind || s.type || '').toLowerCase() === 'http' &&
        String(s.preview_kind || s.previewKind || '').toLowerCase() === 'mpegts' &&
        typeof (s.preview_url || s.previewUrl) === 'string' &&
        Boolean((s.preview_url || s.previewUrl).trim());
    });
    if (!source) return null;
    return {
      label: String(source.label || 'HTTP MPEG-TS'),
      url: (source.preview_url || source.previewUrl).trim()
    };
  }

  function chooseHlsSource(payload) {
    var sources = Array.isArray(payload) ? payload : payload && payload.sources;
    if (!Array.isArray(sources)) return null;
    var source = sources.find(function (s) {
      return s && String(s.kind || s.type || '').toLowerCase() === 'hls' &&
        String(s.preview_kind || s.previewKind || '').toLowerCase() === 'hls' &&
        typeof (s.preview_url || s.previewUrl) === 'string' &&
        Boolean((s.preview_url || s.previewUrl).trim());
    });
    return source ? {label: String(source.label || 'HLS'), url: source.preview_url.trim()} : null;
  }

  function safeBrowserUrl(raw, base, allowCrossOrigin) {
    if (!raw) return null;
    var url;
    try { url = new URL(raw, base); }
    catch (_) { throw Error('Некорректный адрес HTTP-предпросмотра'); }
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw Error('Адрес предпросмотра должен быть HTTP(S)');
    }
    if (!allowCrossOrigin && url.origin !== new URL(base).origin) {
      throw Error('Предпросмотр должен выдаваться веб-сервером TVStreammer (same-origin)');
    }
    return url.href;
  }

  function newSessionToken() {
    if (!window.crypto || typeof window.crypto.getRandomValues !== 'function') {
      throw Error('Браузер не поддерживает безопасный токен HTTP-предпросмотра');
    }
    var bytes = new Uint8Array(16);
    window.crypto.getRandomValues(bytes);
    return Array.from(bytes, function (value) {
      return value.toString(16).padStart(2, '0');
    }).join('');
  }

  function endServerSession(session) {
    if (!session) return;
    // keepalive lets the close request survive tab closing; network errors are
    // expected on navigation and cannot be handled by blocking the UI.
    try {
      var request = fetch('/api/streams/' + encodeURIComponent(session.id) + '/preview/close', {
        method: 'POST', credentials: 'same-origin', keepalive: true,
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({session: session.token})
      });
      if (request && typeof request.catch === 'function') request.catch(function () {});
    } catch (_) {}
  }

  function element(tag, cls, content) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (content != null) node.textContent = String(content);
    return node;
  }

  function install(options) {
    if (typeof document === 'undefined' || typeof window === 'undefined') {
      throw Error('install() запускается только в браузере');
    }
    options = options || {};
    var selector = options.tileSelector || '[data-stream-id]';
    var resolve = options.resolve || function (streamId, signal) {
      return fetch('/api/streams/' + encodeURIComponent(streamId) + '/preview', {
        credentials: 'same-origin', signal: signal, headers: {Accept: 'application/json'}
      }).then(function (response) {
        if (!response.ok) throw Error('Preview API: HTTP ' + response.status);
        return response.json();
      });
    };
    var activeTs = null, activeHls = null, activeRequest = null, requestSerial = 0;
    var activeSession = null;
    var modal = null, video = null, status = null, previousFocus = null;

    function message(text, error) {
      if (!status) return;
      status.textContent = text || '';
      status.className = 'tvp-status' + (error ? ' tvp-error' : '');
    }
    function resetMedia() {
      if (activeHls) {
        var oldHls = activeHls;
        activeHls = null;
        try { oldHls.destroy(); } catch (_) {}
      }
      if (activeTs) {
        var player = activeTs;
        activeTs = null;
        try { player.pause(); player.unload(); player.detachMediaElement(); player.destroy(); } catch (_) {}
      }
      if (video) {
        video.pause();
        video.removeAttribute('src');
        video.load();
      }
    }
    function close() {
      requestSerial++;
      if (activeRequest) { activeRequest.abort(); activeRequest = null; }
      var closing = activeSession;
      activeSession = null;
      // Closing one tile must never terminate another viewer's HTTP session.
      endServerSession(closing);
      resetMedia();
      if (modal) { modal.remove(); modal = null; }
      video = status = null;
      if (previousFocus && typeof previousFocus.focus === 'function') previousFocus.focus();
    }
    function playHls(source) {
      var url;
      try { url = safeBrowserUrl(source.url, window.location.href, options.allowCrossOrigin === true); }
      catch (error) { message(error.message, true); return; }
      try {
        video.muted = true;
        if (video.canPlayType('application/vnd.apple.mpegurl')) {
          video.src = url;
        } else if (window.Hls && typeof window.Hls.isSupported === 'function' &&
                   window.Hls.isSupported()) {
          var player = new window.Hls({enableWorker: false, lowLatencyMode: true});
          activeHls = player;
          player.on(window.Hls.Events.ERROR, function (_event, data) {
            if (activeHls === player && data && data.fatal) {
              message('Ошибка HLS: ' + String(data.details || data.type || 'нет данных'), true);
            }
          });
          player.attachMedia(video);
          player.loadSource(url);
        } else {
          message('Для HLS необходима поддержка браузера или локальная hls.js.', true);
          return;
        }
        message('HLS · ' + source.label + ' · звук включается в плеере');
        Promise.resolve(video.play()).catch(function () { message('Нажмите ▶ для запуска видео.'); });
      } catch (error) { resetMedia(); message('Ошибка HLS: ' + error.message, true); }
    }

    function playHttp(payload, streamId) {
      if (payload && payload.active === false) {
        message('Поток остановлен: временный HTTP-предпросмотр недоступен.', true);
        return;
      }
      var hls = chooseHlsSource(payload);
      // Input HLS may contain TS discontinuities that are handled by the
      // existing playlist; prefer it if a compatible browser player exists.
      var inputMode = String(payload && payload.input_mode || '').toLowerCase();
      var isHlsInput = (payload && payload.input_is_hls === true) || inputMode === 'hls';
      var canHls = (typeof video.canPlayType === 'function' &&
        video.canPlayType('application/vnd.apple.mpegurl')) ||
        (window.Hls && typeof window.Hls.isSupported === 'function' && window.Hls.isSupported());
      if (hls && canHls && isHlsInput) { playHls(hls); return; }
      var source = chooseHttpSource(payload);
      if (!source && hls && canHls) { playHls(hls); return; }
      if (!source) {
        message('Временный HTTP-предпросмотр недоступен для этого канала.', true);
        return;
      }
      var url;
      try {
        url = safeBrowserUrl(source.url, window.location.href, options.allowCrossOrigin === true);
        var previewPath = '/api/streams/' + encodeURIComponent(streamId) + '/preview.ts';
        var parsed = new URL(url);
        if (parsed.origin === window.location.origin && parsed.pathname === previewPath) {
          var token = newSessionToken();
          parsed.searchParams.set('session', token);
          url = parsed.href;
          activeSession = {id: streamId, token: token};
        }
      } catch (error) { message(error.message, true); return; }
      if (!window.mpegts || !window.mpegts.getFeatureList ||
          !window.mpegts.getFeatureList().mseLivePlayback) {
        message('Для HTTP MPEG-TS необходимы локальная mpegts.js и поддержка MediaSource.', true);
        return;
      }
      try {
        video.muted = true;
        var player = window.mpegts.createPlayer({type: 'mpegts', isLive: true, url: url},
          {enableWorker: false, lazyLoad: false, liveBufferLatencyChasing: false});
        activeTs = player;
        player.on(window.mpegts.Events.ERROR, function (_type, detail) {
          if (activeTs !== player) return;
          // If an already-configured HLS output exists (including satellite
          // channels), try its browser player before declaring preview failed.
          // No new source, encoder or public endpoint is created here.
          if (hls && canHls) {
            resetMedia();
            playHls(hls);
          } else {
            message('Ошибка HTTP MPEG-TS: ' + String(detail || 'нет данных') +
              '. Проверьте кодеки канала и доступность новых видеокадров.', true);
          }
        });
        player.attachMediaElement(video);
        player.load();
        message('HTTP MPEG-TS · ' + source.label + ' · звук включается в плеере. ' +
          'Для спутникового MPEG-2/AC3 браузеру может потребоваться H.264/AAC-превью.');
        Promise.resolve(player.play()).catch(function () {
          if (activeTs === player) message('Нажмите ▶ для запуска видео.');
        });
      } catch (error) {
        resetMedia();
        message('Ошибка HTTP MPEG-TS: ' + error.message, true);
      }
    }
    function open(streamId, streamName) {
      close();
      previousFocus = document.activeElement;
      var serial = requestSerial;
      modal = element('div', 'tvp-overlay');
      modal.setAttribute('role', 'presentation');
      var dialog = element('section', 'tvp-dialog');
      dialog.setAttribute('role', 'dialog');
      dialog.setAttribute('aria-modal', 'true');
      dialog.setAttribute('aria-label', 'HTTP-предпросмотр потока');
      var header = element('div', 'tvp-header');
      var title = element('h2', 'tvp-title', streamName || ('Поток ' + streamId));
      var closeBtn = element('button', 'tvp-close', '×');
      closeBtn.type = 'button';
      closeBtn.setAttribute('aria-label', 'Закрыть предпросмотр');
      closeBtn.addEventListener('click', close);
      header.append(title, closeBtn);
      video = element('video', 'tvp-video');
      video.controls = true;
      video.playsInline = true;
      video.preload = 'none';
      video.setAttribute('aria-label', 'HTTP-предпросмотр');
      status = element('div', 'tvp-status', 'Подключение к HTTP-потоку…');
      dialog.append(header, video, status);
      modal.append(dialog);
      modal.addEventListener('mousedown', function (event) { if (event.target === modal) close(); });
      document.body.append(modal);
      closeBtn.focus();
      activeRequest = new AbortController();
      Promise.resolve().then(function () { return resolve(streamId, activeRequest.signal); })
        .then(function (payload) {
          if (serial !== requestSerial || !modal) return;
          activeRequest = null;
          playHttp(payload, streamId);
        }).catch(function (error) {
          if (serial !== requestSerial || !modal || error.name === 'AbortError') return;
          activeRequest = null;
          message('Не удалось получить HTTP-предпросмотр: ' + error.message, true);
        });
    }
    function onDblClick(event) {
      if (event.target.closest('button, a, input, textarea, select, [contenteditable="true"]')) return;
      var tile = event.target.closest(selector);
      if (!tile || !tile.getAttribute('data-stream-id')) return;
      event.preventDefault();
      event.stopPropagation();
      open(tile.getAttribute('data-stream-id'),
        tile.getAttribute('data-stream-name') || tile.getAttribute('aria-label') || '');
    }
    function onKeyDown(event) { if (event.key === 'Escape' && modal) { event.preventDefault(); close(); } }
    document.addEventListener('dblclick', onDblClick, true);
    document.addEventListener('keydown', onKeyDown, true);
    // Handles tab navigation, not only the modal close button / Escape.
    window.addEventListener('pagehide', close);
    return {open: open, close: close, destroy: function () {
      close();
      document.removeEventListener('dblclick', onDblClick, true);
      document.removeEventListener('keydown', onKeyDown, true);
      window.removeEventListener('pagehide', close);
    }};
  }
  return {install: install, chooseHttpSource: chooseHttpSource,
    chooseHlsSource: chooseHlsSource, safeBrowserUrl: safeBrowserUrl};
});
