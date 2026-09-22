# TVStreammerSAT5 v203.67 — встроенный предпросмотр браузера

## Реализовано в исходниках

- Двойной клик по фону существующей плитки открывает диалог с плеером. Одиночный клик и кнопки Старт/Стоп/Ред./График/URL не меняются; двойной клик по кнопке не запускает предпросмотр.
- Авторизованный endpoint `GET /api/streams/<id>/preview` отдаёт список ВСЕХ выходов из конфигурации плитки, сохраняет порядок и сообщает реальные доступные браузеру URL. Данные авторизации SRT/входных URL в ответ не копируются.
- HLS выход: существующий `/hls/<id>/video.m3u8` через native-HLS или локальный hls.js.
- HTTP MPEG-TS выход: существующий `/stream/<id>.ts` через локальный mpegts.js / MediaSource. Содержимое должно иметь совместимые с браузером кодеки (например, H.264/AAC).
- SRT/UDP/RTSP/RTMP выходы: если у ЭТОЙ ЖЕ плитки УЖЕ настроена HLS-ветка, плеер показывает её медиасодержимое; при отсутствии HLS, но наличии HTTP-ветки используется HTTP MPEG-TS. Это **общий предпросмотр медиа**, не анализ фактической доставки выбранного SRT/UDP выхода. Если в плитке только SRT/UDP и нет HTTP/HLS, плеер явно показывает недоступность (никакой фиктивный SRT→HLS мост не создаётся).
- При смене выхода прежний HLS/mpegts проигрыватель освобождается; при закрытии диалога загрузки сегментов/HTTP прекращаются. Плеер сам НЕ перестраивает и не перезапускает production stream.
- UI/CSS/клиентская логика встроены непосредственно в `src/HttpServer.cpp`; отдельные `web/preview/` файлы сохранены для редактирования/тестов. Вендорные медиабиблиотеки загружаются с того же веб-сервера, без CDN браузера.

## Обязательная загрузка двух JS-библиотек (один раз на сборочной машине)

В ZIP лежит ПОЛНЫЙ исходный проект, но собранные сторонние hls.js и mpegts.js **не включены**: при создании архива доступ к загрузке бинарных JS-дистрибутивов отсутствовал. Для Chromium/Firefox обязательно скачать их на машине с выходом в Интернет и добавить в Git:

```bash
bash scripts/vendor_preview_libs.sh
ls -lh web/vendor/*.js
```

Скрипт получает закреплённые версии hls.js 1.6.15 и mpegts.js 1.7.3 и их тексты лицензий Apache-2.0. Не скачивайте их в браузере при просмотре — сервер обслуживает локальные файлы из `/preview/hls.min.js` и `/preview/mpegts.min.js`. Если при сборке Internet недоступен, подготовьте `web/vendor/` на Windows и перенесите с исходниками.

### Запуск из дерева и установка

Исполняемый файл `build-preview-20367/TVStreammerSAT5` ищет `web/vendor/` в корне проекта. После установки бинарника в `/opt/TVStreammerSAT5/TVStreammerSAT5` библиотека должна лежать в `/opt/TVStreammerSAT5/web/vendor/`.

Ничего не выполняйте через `cmake --install` на production без проверки: проект содержит установку OSCam и systemd unit. Команды ниже собирают в отдельный build-каталог и не заменяют работающий бинарник.

### Windows PowerShell: безопасное применение патча к своему Git-репозиторию

ZIP содержит все исходники. Для существующего Git безопаснее применить отдельный `TVStreammerSAT5-203.67-browser-preview.patch`: `git apply --check` покажет конфликты и не удалит незакоммиченные изменения. НЕ перезаписывайте весь репозиторий через `Expand-Archive -Force`.

```powershell
Set-Location 'D:\PROJECTS\Tvstreamer_sat'
git status --short
git apply --check "$env:USERPROFILE\Downloads\TVStreammerSAT5-203.67-browser-preview.patch"
git apply "$env:USERPROFILE\Downloads\TVStreammerSAT5-203.67-browser-preview.patch"
# Скачайте два JS-дистрибутива и их LICENSE в web/vendor до коммита:
New-Item -ItemType Directory -Force 'web\vendor' | Out-Null
Invoke-WebRequest 'https://cdn.jsdelivr.net/npm/hls.js@1.6.15/dist/hls.min.js' -OutFile 'web\vendor\hls.min.js'
Invoke-WebRequest 'https://cdn.jsdelivr.net/npm/mpegts.js@1.7.3/dist/mpegts.js' -OutFile 'web\vendor\mpegts.min.js'
Invoke-WebRequest 'https://cdn.jsdelivr.net/npm/hls.js@1.6.15/LICENSE' -OutFile 'web\vendor\LICENSE.hls.js'
Invoke-WebRequest 'https://cdn.jsdelivr.net/npm/mpegts.js@1.7.3/LICENSE' -OutFile 'web\vendor\LICENSE.mpegts.js'
git diff --check
git add src/HttpServer.cpp web/preview web/vendor scripts/vendor_preview_libs.sh docs/BROWSER_PREVIEW_20367.md tests/test_browser_preview.js
git commit -m "Add in-browser multi-output preview to TVStreammerSAT5 203.67"
git push origin HEAD
```

Если `git apply --check` не прошёл, не используйте `git reset --hard` и не заменяйте C++ файл целиком; нужно сопоставить текущую ветку со снапшотом v203.67.

### Ubuntu: обновление и тестовая сборка отдельно от работающего сервиса

```bash
cd /opt/TVStreammerSAT5
git status --short
git pull --ff-only
ls -lh web/vendor/hls.min.js web/vendor/mpegts.min.js
cmake -S . -B build-preview-20367 -DCMAKE_BUILD_TYPE=Release -DTVSTREAMMERSAT5_BUILD_OSCAM_MINI=OFF
nice -n 10 cmake --build build-preview-20367 --parallel 2 --target TVStreammerSAT5 tvstreammersat5_ca_newcamd
sha256sum build-preview-20367/TVStreammerSAT5
```

`cmake` требует установленные *сборочные* зависимости проекта (GStreamer dev, Boost, libcurl, JSONCPP, OpenSSL, libdvbcsa), как и v203.67. `git pull` и CMake-команды не заменяют сервисный бинарник, если unit указывает на `/opt/TVStreammerSAT5/TVStreammerSAT5`; перед сборкой проверьте фактический `ExecStart` через `systemctl cat tvstreammersat5.service`.

Проверка после отдельной установки новой версии в окне обслуживания: `GET /api/streams/<id>/preview` (требует логин), двойной клик плитки, выбор каждого выхода, проверка HTTP статуса `/preview/hls.min.js` и `/preview/mpegts.min.js`, открытие/закрытие плеера, а также отсутствие событий `MEDIA STALL` в журнале. Здесь полноценный live-тест на сервере не проводился.
