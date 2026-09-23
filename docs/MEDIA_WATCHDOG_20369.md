# TVStreammerSAT5 203.69 — SRT/HTTP source media watchdog

This is a source-only release candidate based on the 203.68 preview-open-fixed
source archive. It has **not** been built or run with the production GStreamer
stack, and must not be installed on a live multi-channel server without testing.

## Why 203.68 can miss a stalled channel

The existing 203.64–203.66 media watchdog excluded HTTP MPEG-TS from
`mediaWatchEligible`. Its `media-degraded` state only waited for media to return;
a connected SRT sender continuously forwarding NULL packets would never cause
a reconnect. The ordinary input-byte watchdog treated transport bytes as
restored, even when the source kept sending only NULL/PSI.

## This patch

* Enables existing input/final-output media checks for HTTP MPEG-TS as well as
  SRT (for remapped, non-transcoded streams). The existing UDP/RTP logic remains.
* When HTTP/SRT transport stays active but **no discovered audio/video input
  PID payload** arrives for 18 seconds, reconnects only that channel's active
  source via the existing full-pipeline recovery function. Retains the selected
  backup, if any. If transport itself is dead, the ordinary no-byte watchdog
  remains responsible.
* If video PES PTS has advanced at least twice in the current generation and
  subsequently does not advance for 18 seconds while input media bytes keep
  arriving, reconnects the channel. Encrypted/unreadable/missing PES PTS never
  arms this check; audio-only channels use the input-media check only.
* If real media continues arriving but the UDP-CBR program never produces its
  first output media packet for 20 seconds, reconnects that channel.
* Startup grace: 20 seconds; reconnect attempts triggered by this new watchdog
  at most once per 60 seconds. Existing no-byte and output-watchdog conditions
  remain independent. The new source generation counter prevents old pipeline
  PTS/output counters being accepted as proof of a newly started source.
* Logs `MEDIA WATCH 203.69` with the selected reason and input/output counters.
  PCR, PSI, and NULL output bitrate alone never mark media recovered.

## Limitations / rollout

This is NOT a decoder/player validation. The input PTS test requires readable
video PES headers and media PID discovery; it cannot detect a provider
re-encoding the *same picture* with continually advancing PTS, or a stalled
non-remapped/transcoded stream. Non-remapped HTTP/SRT channels receive the new input-side checks, but the
existing final-output probe and the no-first-CBR-media check require remap.
It also cannot guarantee that an upstream
source contains useful media after reconnection. Do not apply to production
without compiling against the target's own GStreamer headers, synthetic TS
regression testing, and a 1-channel canary test. Only then schedule the service
restart (it interrupts all channels); no live service was modified here.

Sample isolated build on test host:

```bash
node tests/test_browser_preview.js
node tests/test_preview_close_20368.js
g++ -std=c++17 -O2 -Wall -Wextra -Werror tests/test_ts_media_progress_20369.cpp -o /tmp/test_ts_media_progress_20369
/tmp/test_ts_media_progress_20369
cmake -S . -B build-media-20369 -DCMAKE_BUILD_TYPE=Release -DTVSTREAMMERSAT5_BUILD_OSCAM_MINI=OFF
cmake --build build-media-20369 --parallel 2 --target TVStreammerSAT5
```

After a *test-server* canary, inspect:

```bash
journalctl -u tvstreammersat5.service --since '30 minutes ago' --no-pager | \
  grep -E 'MEDIA WATCH 203.69|MEDIA STALL RECOVERY|FINAL TS STALL RECOVERY'
```
