# TVStreammerSAT5 203.73 EN interface fix

This patch keeps the numeric application version **203.73** unchanged.
When the English UI is selected, the About dialog displays **203.73 EN**.

## What was fixed

- RU/EN switching now also localizes UI fragments generated dynamically after page load.
- Stream settings, output settings, DVB-S/S2 dialogs, MPTS dialogs, quality/history UI and browser-preview messages are translated in English mode.
- Dynamic labels, tooltips, placeholders, alerts and confirm dialogs are localized.
- The HTML `lang` attribute follows the selected language.
- English mode no longer depends only on `data-i18n` labels; dynamically rendered Russian text is translated as well.
- Russian mode remains available and restores the original Russian strings.

## Versioning

`src/AppVersion.h` remains:

```cpp
inline constexpr const char* kProgramVersion = "203.73";
```

Only the displayed About-dialog version gets the `EN` suffix while English mode is active.

## Validation performed

- Inline JavaScript syntax check with Node.js.
- `tests/test_english_ui_20373.js`.
- Existing preview / DVB / SD 16:9 / UDP watchdog / OSCam-PCSC regression tests.

A full native build was not performed in the patch-generation environment because GStreamer development packages were not installed there.
