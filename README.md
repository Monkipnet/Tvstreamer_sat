# TVStreammerSAT5

**Версия: 203.73**

TVStreammerSAT5 - сервер маршрутизации, мониторинга и преобразования телевизионных потоков на базе C++17 и GStreamer. Программа принимает сетевые и спутниковые источники, формирует один или несколько выходов для каждого канала и управляется через встроенную русско-английскую веб-панель.

![Основная панель TVStreammerSAT5](./docs/screenshots/dashboard.png)

![Управление OSCam-mini](./docs/screenshots/oscam-mini.png)

## Возможности

- создание, редактирование, запуск, остановка и удаление потоков из браузера;
- основной и резервный источник с автоматическим переключением и возвратом;
- несколько независимых выходов у одного потока;
- MPEG-TS passthrough или транскодирование видео и аудио;
- CBR/VBR-формирование транспортного потока, контроль PCR и continuity counter;
- переназначение SID, video/audio PID, Service Name и Provider;
- DVB-S/S2 сканирование с выбором адаптера, frontend, транспондера и сервисов;
- совместное использование одного DVB frontend каналами одного транспондера;
- работа с FTA и локальной Conditional Access через CA-плагин и OSCam-mini;
- мониторинг входного/выходного битрейта, ошибок, DVB signal/quality и загрузки интерфейсов;
- список абонентов, фильтрация по IP и отображение активных сессий;
- Telegram-уведомления о состоянии потоков;
- встроенный тестовый источник и библиотека файлов замены;
- Basic Authentication и шифрование пароля панели в конфигурации.

## Архитектура

Обычные потоки обрабатываются внутри процесса без обязательного транскодирования.
Для каждого канала сначала выбирается входной протокол, затем создаётся passthrough
или remap-путь и один основной либо несколько дополнительных выходов.

```text
Входной протокол
      |
      +-- passthrough/remap ----------------------> выходной протокол
      |
      +-- опциональный GStreamer transcoder
              |
              +-- decode
              +-- deinterlace / scale / frame rate
              +-- H.264 + AAC/MP3
              +-- выходной модуль
```

Код протоколов разделён по каталогам:

```text
src/protocols/inputs/          URI-модули входов транскодера
src/protocols/outputs/         URI-модули выходов транскодера
src/protocols/stream/inputs/   входы обычного поточного пути
src/protocols/stream/outputs/  выходы обычного поточного пути
```

## Поддерживаемые протоколы

### Входы

| Источник | Примеры и режимы |
| --- | --- |
| UDP MPEG-TS | unicast, multicast, выбор входного интерфейса |
| RTP MPEG-TS | unicast и multicast |
| SRT | Caller и Listener |
| HTTP MPEG-TS | одиночный поток по HTTP/HTTPS |
| HLS | master/media playlist, сегменты, Header или Query access key |
| RTSP | сетевые камеры и медиасерверы |
| RTMP | RTMP-источники |
| Файл | локальный файл, в том числе файл замены с циклическим воспроизведением |
| DVB-S/S2 | Linux DVB frontend через `dvbsrc` |
| Тестовый сигнал | встроенный `test://bars` |

### Выходы

| Выход | Назначение |
| --- | --- |
| UDP MPEG-TS VBR | передача исходного транспортного потока |
| UDP MPEG-TS CBR | транспортный поток с заданным целевым битрейтом |
| RTP MPEG-TS | доставка MPEG-TS поверх RTP |
| SRT | Caller или Listener |
| HTTP TS | непрерывный MPEG-TS по HTTP |
| HLS | live playlist и MPEG-TS сегменты |
| RTSP Push | публикация на внешний RTSP-сервер |
| RTMP Push | публикация на RTMP-сервер или YouTube |

Для одного канала можно настроить основной и дополнительные выходы разных типов.

H.264-транскодирование поддерживает CPU `x264enc` и NVIDIA NVENC через GStreamer
`nvh264enc`. В режиме `Auto` программа предпочитает NVENC, если элемент `nvh264enc`
доступен, и автоматически использует `x264enc` иначе. Для NVENC требуется рабочий
проприетарный драйвер NVIDIA с поддержкой NVENC и GStreamer `nvcodec`; проверить
сервер можно командами `nvidia-smi` и `gst-inspect-1.0 nvh264enc`.

## Веб-панель

После запуска панель доступна по адресу:

```text
http://SERVER_IP:9000/
```

Начальные учётные данные при первом запуске:

```text
login: admin
password: admin
```

Сразу измените пароль в настройках. Он хранится в `tvstreammersat5-config.json` в зашифрованном виде AES-256-GCM, а локальный ключ создаётся рядом с конфигурацией в файле `tvstreammersat5-ui.key` с правами `0600`.

Основная панель показывает карточки каналов, состояние источника, активный вход, битрейт, режим выхода, ошибки MPEG-TS, DVB-метрики и состояние декодирования. Настройки программы, абоненты, CA-клиенты и окно «О программе» доступны из верхней панели.

## Абоненты и мониторинг подключений

Окно **Абоненты** показывает активные подключения к потокам по HTTP, HLS и SRT. Для каждого IP отображаются номер и название потока, протокол и количество соединений. У зарегистрированного абонента текущие номера потоков видны в колонке сессии.

Незарегистрированный IP можно сразу:

- добавить в абоненты с доступом к выбранному потоку;
- заблокировать независимо от состояния общей IP-фильтрации;
- позднее разблокировать в списке заблокированных адресов.

Список блокировок хранится в `tvstreammersat5-subscribers.json` в поле `blocked_ips`. UDP не устанавливает клиентскую сессию, поэтому приложение не может определить получателей UDP unicast/multicast; для их контроля нужен мониторинг IGMP и сетевого оборудования.

## DVB-S/S2

Диалог добавления спутниковых каналов поддерживает:

- выбор `/dev/dvb/adapterN/frontendN`;
- DVB-S и DVB-S2;
- частоту, symbol rate, поляризацию, FEC и модуляцию;
- DiSEqC и параметры LNB LOF;
- DVB-S2 stream ID;
- просмотр LOCK, signal и quality;
- сканирование PAT/PMT/SDT и выбор найденных сервисов;
- автоматическое сохранение SID, PMT, PCR и elementary PID.

Каналы одного транспондера могут использовать общий физический frontend. Для одновременного приёма другого транспондера требуется другой frontend или остановка текущих каналов на этом устройстве.

Пользователь процесса должен иметь доступ на чтение и запись к `/dev/dvb/*`.

## Conditional Access и OSCam-mini

Проект включает:

- версионированный in-process `CaBackend` ABI;
- плагин `tvstreammersat5-ca-newcamd.so`;
- обнаружение Phoenix/SmartMouse USB reader;
- привязку зашифрованного канала к конкретному CA-клиенту;
- ограничения количества сервисов и состояние декодирования в карточке канала;
- vendored OSCam-mini с Newcamd, Irdeto, Viaccess и Phoenix.

OSCam-mini собирается общей целью CMake и управляется на странице:

```text
http://SERVER_IP:9000/oscam-mini
```

Подробная настройка описана в [OSCAM_MINI.md](OSCAM_MINI.md). Интерфейс плагина описан в [docs/CA_BACKEND_PLUGIN_API.md](docs/CA_BACKEND_PLUGIN_API.md), транспорт Phoenix - в [docs/PHOENIX_SERIAL_TRANSPORT.md](docs/PHOENIX_SERIAL_TRANSPORT.md).

Используйте Conditional Access только с оборудованием, картами и сервисами, для которых у вас есть законные права доступа.

## Системные требования

- Ubuntu 24.04 или совместимая Debian/Ubuntu система;
- CMake 3.10 или новее;
- компилятор с поддержкой C++17;
- GStreamer 1.0 и наборы Base/Good/Bad/Ugly/Libav;
- Boost Thread/System, JsonCpp, libcurl, OpenSSL и libdvbcsa;
- Linux DVB и Phoenix/SmartMouse устройства - только для соответствующих функций.

## Сборка

Установите зависимости:

```bash
chmod +x install_deps.sh
sudo ./install_deps.sh
```

Соберите приложение, Newcamd CA-плагин и OSCam-mini:

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
```

Основные артефакты:

```text
build/TVStreammerSAT5
build/tvstreammersat5-ca-newcamd.so
build/oscam-mini/oscam-mini
```

## Запуск

Программа читает конфигурацию из текущего рабочего каталога. При первом запуске она создаётся автоматически.

```bash
mkdir -p ~/tvstreammersat5-data
cd ~/tvstreammersat5-data
/path/to/project/build/TVStreammerSAT5
```

В журнале появится адрес HTTP-порта, по умолчанию `9000`. Для остановки используйте `Ctrl+C` или штатное управление сервисом.

Установка собранных компонентов:

```bash
sudo cmake --install build
```

## Запуск как systemd-сервис

Установите бинарник и создайте отдельный рабочий каталог для конфигурации:

```bash
sudo mkdir -p /opt/tvstreammersat5
sudo install -m 755 build/TVStreammerSAT5 /opt/tvstreammersat5/TVStreammerSAT5
```

Пример `/etc/systemd/system/tvstreammersat5.service`:

```ini
[Unit]
Description=TVStreammerSAT5
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/tvstreammersat5
ExecStart=/opt/tvstreammersat5/TVStreammerSAT5
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

Активируйте сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now tvstreammersat5
sudo systemctl status tvstreammersat5 --no-pager --full
```

## Конфигурационные файлы

Все изменяемые данные находятся в рабочем каталоге процесса:

```text
tvstreammersat5-config.json       основные настройки и потоки
tvstreammersat5-ui.key            ключ шифрования пароля панели
tvstreammersat5-subscribers.json  абоненты и IP-фильтрация
backup-files/                     загруженные файлы замены
```

Перед обновлением сохраняйте эти файлы. Не публикуйте конфигурацию: она может содержать сетевые адреса, Telegram token и параметры CA-клиентов.

## Параметры UDP и PCR

Для обычного UDP-выхода программа накапливает 1500 мс данных и начинает передачу
с ближайшего независимо декодируемого видеокадра. Если конкретному приёмнику нужен
прежний пятисекундный стартовый запас, задайте переменную окружения:

```bash
TVS_UDP_STARTUP_BUFFER_MS=5000
```

Допустимый диапазон: от `250` до `30000` мс. Переменную следует передать процессу
программы или контейнеру через `docker run -e TVS_UDP_STARTUP_BUFFER_MS=5000 ...`.

Для непрерывных DVB/IP MPEG-TS потоков CBR-выход сохраняет исходный PCR, чтобы
PCR и PTS/DTS оставались в одной временной шкале. Синтетический непрерывный PCR
используется автоматически для сегментированного HLS. Старый режим синтетического
PCR можно принудительно включить переменной:

```bash
TVS_UDP_FORCE_SYNTHETIC_PCR=1
```

## Docker

Сборка и запуск в Docker используют host networking, чтобы корректно работали
multicast, RTP, SRT listener и привязка к сетевым интерфейсам.

### Сборка образа

```bash
docker build --pull -t tvstreammersat5:202.28 .
```

Для полной пересборки без использования слоёв кеша:

```bash
docker build --pull --no-cache -t tvstreammersat5:202.28 .
```

### Фоновый запуск

Укажите постоянный каталог данных на хосте. Если стандартного файла
`tvstreammersat5-config.json` ещё нет, программа создаст его при первом запуске.

```bash
cd ~/Tvstreamer_sat
mkdir -p /opt/tvstreammersat5

CONTAINER_NAME=tvstreammersat5 \
DETACH=1 \
RECREATE=1 \
IMAGE_NAME=tvstreammersat5:202.28 \
CONFIG_FILE=/opt/tvstreammersat5/tvstreammersat5-config.json \
bash ./scripts/run_container.sh

docker ps --filter name=tvstreammersat5
docker logs --tail 100 tvstreammersat5
```

Скрипт проверяет наличие образа до удаления прежнего контейнера и выводит
идентификатор созданного контейнера. `RECREATE=1` позволяет одной командой
заменить существующий контейнер либо создать его, если контейнера ещё нет.
В фоновом режиме действует политика `unless-stopped`. Скрипт использует host
networking, подключает каталог данных и автоматически передаёт найденные
`/dev/dvb` устройства в контейнер.

Новая конфигурация создаётся без каналов с учётными данными `admin` / `admin`.
После первого входа сразу измените пароль.

Интерактивный временный запуск остаётся доступен без `DETACH=1`:

```bash
IMAGE_NAME=tvstreammersat5:202.28 \
CONFIG_FILE=/opt/tvstreammersat5/tvstreammersat5-config.json \
./scripts/run_container.sh
```

### Управление контейнером

`docker restart` перезапускает уже существующий контейнер с тем же образом.
После пересборки образа эту команду использовать недостаточно: контейнер нужно
удалить и создать заново по инструкции следующего раздела.

```bash
# Состояние контейнера
docker ps -a --filter name=tvstreammersat5

# Текущие и последние 200 строк журнала
docker logs --tail 200 tvstreammersat5
docker logs --tail 200 -f tvstreammersat5

# Перезапуск
docker restart tvstreammersat5

# Остановка и повторный запуск
docker stop tvstreammersat5
docker start tvstreammersat5

# Проверка параметров и состояния
docker inspect tvstreammersat5
```

### Обновление и пересборка проекта

Выполняйте весь блок из корня репозитория. Новый образ сначала полностью
собирается и проверяется Docker, и только затем скрипт заменяет контейнер.

```bash
cd ~/Tvstreamer_sat

git pull origin main
docker build --pull -t tvstreammersat5:202.28 .

CONTAINER_NAME=tvstreammersat5 \
DETACH=1 \
RECREATE=1 \
IMAGE_NAME=tvstreammersat5:202.28 \
CONFIG_FILE=/opt/tvstreammersat5/tvstreammersat5-config.json \
bash ./scripts/run_container.sh

# Контейнер должен иметь состояние Up
docker ps --filter name=tvstreammersat5 \
  --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

# Проверка запуска программы
docker logs --tail 100 tvstreammersat5
curl --fail http://127.0.0.1:9000/health
```

Если скрипт завершился ошибкой до строки `Removing existing container`, старый
контейнер остаётся на месте. Сообщение укажет причину, например недоступный образ
или некорректный путь пользовательского конфигурационного файла.

Конфигурация, ключ UI, список абонентов и файлы замены сохраняются на хосте в
каталоге рядом с `CONFIG_FILE`, поэтому удаление и повторное создание контейнера
их не удаляет.

Проверка после пересборки:

```bash
docker ps -a --filter name=tvstreammersat5
docker ps --filter name=tvstreammersat5
docker logs --tail 100 tvstreammersat5
curl --fail http://127.0.0.1:9000/health
```

## Проверка и диагностика

Проверка HTTP-сервера без авторизации:

```bash
curl http://127.0.0.1:9000/health
```

Проверка GStreamer:

```bash
./scripts/check_transcoder_plugins.sh
gst-inspect-1.0 dvbsrc
gst-inspect-1.0 mpegtsmux
```

Для подробного журнала GStreamer:

```bash
GST_DEBUG=2 ./build/TVStreammerSAT5
```

Если DVB frontend занят, проверьте другие процессы и убедитесь, что каналы на одном физическом frontend настроены на один транспондер. Если нет транскодирования, запустите `scripts/check_transcoder_plugins.sh` и проверьте наличие подходящих видео- и аудиоэнкодеров.

## Лицензии сторонних компонентов

Исходники OSCam-mini находятся в `third_party/oscam-mini` вместе с собственными файлами лицензии и сведениями об upstream revision. Лицензии остальных библиотек определяются установленными системными пакетами.

## Контакты

Поддержка: [monkipnet@gmail.com](mailto:monkipnet@gmail.com)
