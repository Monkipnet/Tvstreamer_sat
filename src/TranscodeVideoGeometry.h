#pragma once

#include <string>

// SD widescreen can be stored using square pixels (1024x576) or as
// anamorphic broadcast SD (720x576 with a 64:45 pixel aspect ratio).
// Keep the legacy 720x576 preset unchanged for existing saved streams.
namespace tvs::transcode {

struct VideoGeometry {
    int width = 0;
    int height = 0;
    int pixelAspectNum = 1;
    int pixelAspectDen = 1;
};

inline bool videoGeometry(const std::string& preset, VideoGeometry& geometry) {
    if (preset == "3840x2160") { geometry = {3840, 2160, 1, 1}; return true; }
    if (preset == "3200x1800") { geometry = {3200, 1800, 1, 1}; return true; }
    if (preset == "2560x1440") { geometry = {2560, 1440, 1, 1}; return true; }
    if (preset == "1920x1080") { geometry = {1920, 1080, 1, 1}; return true; }
    if (preset == "1280x720") { geometry = {1280, 720, 1, 1}; return true; }
    if (preset == "1024x576") { geometry = {1024, 576, 1, 1}; return true; }
    if (preset == "720x576_16_9") { geometry = {720, 576, 64, 45}; return true; }
    if (preset == "720x576") { geometry = {720, 576, 1, 1}; return true; }
    return false;
}

} // namespace tvs::transcode
