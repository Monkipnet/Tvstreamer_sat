/* TVStreammerSAT5 browser preview UI. No external player processes. */
(function (global, factory) {
  var api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  global.TVStreammerPreview = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, function () {
  'use strict';

  function cleanKind(value) {
    var kind = String(value || '').toLowerCase();
    return ['hls', 'mp4', 'http', 'srt', 'udp', 'mpegts', 'unknown'].includes(kind) ? kind : 'unknown';
  }
  function normalizeSources(payload) {
    var array = Array.isArray(payload) ? payload : payload && payload.sources;
    if (!Array.isArray(array)) throw Error('Ответ preview API должен содержать массив sources');
    return array.map(function (s, index) {
      if (!s || typeof s !== 'object') throw Error('Неверное описание источника #' + index);
      var kind = cleanKind(s.kind || s.type);
      // Never use SRT/UDP input addresses as browser playback URLs. A backend
      // must explicitly supply a browser-ready preview_url for EVERY output.
      var url = s.preview_url || s.previewUrl || null;
      var playKind = cleanKind(s.preview_kind || s.previewKind || kind);
      if (kind === 'srt' || kind === 'udp' || kind === 'mpegts') {
        if (!s.preview_kind && !s.previewKind) playKind = 'unknown';
      }
      return {
        id: String(s.id == null ? index : s.id),
        label: String(s.label || s.name || ('Выход ' + (index + 1))),
        kind: kind,
        playKind: playKind,
        url: typeof url === 'string' && url.trim() ? url.trim() : null,
        details: String(s.details || ''),
        primary: s.primary === true
      };
    });
  }
  function safeBrowserUrl(raw, base, allowCrossOrigin) {
    if (!raw) return null;
    var u;
    try { u = new URL(raw, base); } catch (_) { throw Error('Некорректный адрес предпросмотра'); }
    if (!['http:', 'https:'].includes(u.protocol)) throw Error('Адрес предпросмотра должен быть HTTP(S)');
    if (!allowCrossOrigin && u.origin !== new URL(base).origin) {
      throw Error('Предпросмотр должен выдаваться веб-сервером TVStreammer (same-origin)');
    }
    return u.href;
  }
  function isHls(source) {
    return source.playKind === 'hls' || /\.m3u8(?:\?|$)/i.test(source.url || '');
  }
  function canPreview(source) {
    if (!source.url) return false;
    return isHls(source) || source.playKind === 'mp4' || source.playKind === 'http' || source.playKind === 'mpegts';
  }
  function element(tag, cls, content) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (content != null) n.textContent = String(content);
    return n;
  }
  function install(options) {
    if (typeof document === 'undefined' || typeof window === 'undefined') {
      throw Error('install() запускается только в браузере');
    }
    options = options || {};
    var selector = options.tileSelector || '[data-stream-id]';
    var resolve = options.resolve || function (streamId, signal) {
      return fetch('/api/streams/' + encodeURIComponent(streamId) + '/preview', {
        credentials: 'same-origin', signal: signal, headers: {'Accept': 'application/json'}
      }).then(function (response) {
        if (!response.ok) throw Error('Preview API: HTTP ' + response.status);
        return response.json();
      });
    };
    var activeHls = null, activeTs = null, activeRequest = null, requestSerial = 0;
    var modal = null, video = null, controls = null, status = null, title = null;
    var previousFocus = null, currentSources = [];

    function resetMedia() {
      if (activeHls) { activeHls.destroy(); activeHls = null; }
      if (activeTs) { try { activeTs.pause(); activeTs.unload(); activeTs.detachMediaElement(); activeTs.destroy(); } catch (_) {} activeTs = null; }
      if (video) {
        video.pause();
        video.removeAttribute('src');
        video.load();
      }
    }
    function message(txt, error) {
      if (status) { status.textContent = txt || ''; status.className = 'tvp-status' + (error ? ' tvp-error' : ''); }
    }
    function close() {
      requestSerial++;
      if (activeRequest) { activeRequest.abort(); activeRequest = null; }
      resetMedia();
      if (modal) { modal.remove(); modal = null; }
      video = controls = status = title = null;
      currentSources = [];
      if (previousFocus && typeof previousFocus.focus === 'function') previousFocus.focus();
    }
    function play(source, choiceButton) {
      resetMedia();
      Array.prototype.forEach.call(controls.querySelectorAll('button'), function (button) {
        button.setAttribute('aria-pressed', button === choiceButton ? 'true' : 'false');
      });
      if (!source.url) {
        message('Для выхода «' + source.label + '» сервер не предоставил адрес браузерного предпросмотра. Для SRT/UDP без HLS-выхода нужен отдельный HLS-мост.', true);
        return;
      }
      if (!canPreview(source)) {
        message('Формат этого выхода недоступен браузеру. Нужен HLS-предпросмотр (H.264/AAC), созданный на сервере.', true);
        return;
      }
      var url;
      try { url = safeBrowserUrl(source.url, window.location.href, options.allowCrossOrigin === true); }
      catch (e) { message(e.message, true); return; }
      video.muted = true;
      if (source.playKind === 'mpegts') {
        if (!window.mpegts || !window.mpegts.getFeatureList().mseLivePlayback) {
          message('Для HTTP MPEG-TS нужна локальная библиотека mpegts.js и поддержка MediaSource браузером.', true);
          return;
        }
        try {
          var ts = window.mpegts.createPlayer({ type: 'mpegts', isLive: true, url: url },
            { enableWorker: false, lazyLoad: false, liveBufferLatencyChasing: false });
          activeTs = ts;
          ts.on(window.mpegts.Events.ERROR, function (_type, detail) {
            if (activeTs === ts) message('Ошибка HTTP TS: ' + String(detail || 'нет данных'), true);
          });
          ts.attachMediaElement(video);
          ts.load();
          message('HTTP MPEG-TS · ' + source.label + ' · звук включается в плеере');
          Promise.resolve(ts.play()).catch(function () { message('Нажмите ▶ для запуска видео.'); });
        } catch (error) { resetMedia(); message('Ошибка MPEG-TS: ' + error.message, true); }
        return;
      }
      if (isHls(source)) {
        if (video.canPlayType('application/vnd.apple.mpegurl')) {
          video.src = url;
          message('HLS · ' + source.label + ' · звук включается в плеере' + (source.kind !== 'hls' ? ' · общий медиапредпросмотр, не проверка протокола ' + source.kind.toUpperCase() : ''));
          video.play().catch(function () { message('Нажмите ▶ для запуска видео.'); });
          return;
        }
        if (window.Hls && window.Hls.isSupported()) {
          var hls = new window.Hls({ enableWorker: true, backBufferLength: 15 });
          activeHls = hls;
          hls.on(window.Hls.Events.ERROR, function (_ev, data) {
            if (activeHls !== hls) return;
            if (data && data.fatal) message('Ошибка HLS: ' + (data.details || data.type), true);
          });
          hls.on(window.Hls.Events.MANIFEST_PARSED, function () {
            if (activeHls !== hls) return;
            message('HLS · ' + source.label + ' · звук включается в плеере' + (source.kind !== 'hls' ? ' · общий медиапредпросмотр, не проверка протокола ' + source.kind.toUpperCase() : ''));
            video.play().catch(function () { message('Нажмите ▶ для запуска видео.'); });
          });
          hls.attachMedia(video);
          hls.loadSource(url);
          return;
        }
        message('Для этого браузера нужен локальный hls.min.js (см. scripts/vendor_preview_libs.sh).', true);
        return;
      }
      // HTML5 video can play compatible MP4 and certain browser-supported HTTP
      // formats, but HTTP MPEG-TS is NOT assumed to be browser-compatible.
      if (source.playKind !== 'mp4' && /\.ts(?:\?|$)/i.test(url)) {
        message('Непрерывный HTTP MPEG-TS требуется преобразовать в HLS на стороне сервера.', true);
        return;
      }
      video.src = url;
      message('HTTP · ' + source.label + ' · звук включается в плеере');
      video.play().catch(function () { message('Нажмите ▶ для запуска видео.'); });
    }
    function showSources(payload) {
      currentSources = normalizeSources(payload);
      if (!currentSources.length) { message('У плитки нет доступных выходов для предпросмотра.', true); return; }
      controls.replaceChildren();
      currentSources.forEach(function (source) {
        var button = element('button', 'tvp-choice');
        button.type = 'button';
        var header = element('span', 'tvp-choice-name', source.label);
        var detail = element('small', 'tvp-choice-details', [source.kind.toUpperCase(), source.details].filter(Boolean).join(' · '));
        button.append(header, detail);
        button.setAttribute('aria-pressed', 'false');
        button.addEventListener('click', function () { play(source, button); });
        controls.appendChild(button);
      });
      var preferred = currentSources.findIndex(function (s) { return s.primary && canPreview(s); });
      if (preferred < 0) preferred = currentSources.findIndex(canPreview);
      if (preferred >= 0) controls.children[preferred].click();
      else message('Для этой плитки нет готового браузерного HLS/HTTP-предпросмотра. Выберите выход, чтобы увидеть причину.', true);
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
      dialog.setAttribute('aria-label', 'Предпросмотр потока');
      var header = element('div', 'tvp-header');
      title = element('h2', 'tvp-title', streamName || ('Поток ' + streamId));
      var closeBtn = element('button', 'tvp-close', '×');
      closeBtn.type = 'button'; closeBtn.setAttribute('aria-label', 'Закрыть предпросмотр');
      closeBtn.addEventListener('click', close);
      header.append(title, closeBtn);
      video = element('video', 'tvp-video');
      video.controls = true; video.playsInline = true; video.preload = 'none';
      video.setAttribute('aria-label', 'Видеоплеер предпросмотра');
      controls = element('div', 'tvp-choices');
      controls.setAttribute('role', 'group');
      controls.setAttribute('aria-label', 'Выбор выходного потока');
      status = element('div', 'tvp-status', 'Получение списка выходов…');
      dialog.append(header, video, controls, status);
      modal.append(dialog);
      modal.addEventListener('mousedown', function (e) { if (e.target === modal) close(); });
      document.body.append(modal); closeBtn.focus();
      activeRequest = new AbortController();
      Promise.resolve().then(function () { return resolve(streamId, activeRequest.signal); })
        .then(function (payload) {
          if (serial !== requestSerial || !modal) return;
          activeRequest = null;
          showSources(payload);
        }).catch(function (error) {
          if (serial !== requestSerial || !modal || error.name === 'AbortError') return;
          activeRequest = null;
          message('Не удалось получить выходы: ' + error.message, true);
        });
    }
    function onDblClick(event) {
      if (event.target.closest('button, a, input, textarea, select, [contenteditable="true"]')) return;
      var tile = event.target.closest(selector);
      if (!tile || !tile.getAttribute('data-stream-id')) return;
      event.preventDefault();
      event.stopPropagation();
      open(tile.getAttribute('data-stream-id'), tile.getAttribute('data-stream-name') || tile.getAttribute('aria-label') || '');
    }
    function onKeyDown(event) { if (event.key === 'Escape' && modal) { event.preventDefault(); close(); } }
    document.addEventListener('dblclick', onDblClick, true);
    document.addEventListener('keydown', onKeyDown, true);
    return { open: open, close: close, destroy: function () {
      close(); document.removeEventListener('dblclick', onDblClick, true);
      document.removeEventListener('keydown', onKeyDown, true);
    } };
  }
  return { install: install, normalizeSources: normalizeSources, safeBrowserUrl: safeBrowserUrl, canPreview: canPreview };
});
