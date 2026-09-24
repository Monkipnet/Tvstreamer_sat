#include "../src/TranscodeVideoGeometry.h"
#include <cassert>
#include <iostream>

int main() {
    tvs::transcode::VideoGeometry g;
    assert(tvs::transcode::videoGeometry("720x576_16_9", g));
    assert(g.width == 720 && g.height == 576);
    assert(g.pixelAspectNum == 64 && g.pixelAspectDen == 45);
    // 720/576 * 64/45 = 16/9 (exactly).
    assert(9LL * g.width * g.pixelAspectNum == 16LL * g.height * g.pixelAspectDen);
    assert(tvs::transcode::videoGeometry("1024x576", g));
    assert(g.width == 1024 && g.height == 576 && g.pixelAspectNum == 1 && g.pixelAspectDen == 1);
    assert(9LL * g.width * g.pixelAspectNum == 16LL * g.height * g.pixelAspectDen);
    assert(tvs::transcode::videoGeometry("720x576", g));
    assert(g.width == 720 && g.height == 576 && g.pixelAspectNum == 1 && g.pixelAspectDen == 1);
    assert(tvs::transcode::videoGeometry("1920x1080", g));
    assert(g.width == 1920 && g.height == 1080 && g.pixelAspectNum == 1 && g.pixelAspectDen == 1);
    assert(!tvs::transcode::videoGeometry("invalid", g));
    std::cout << "PASS: SD 16:9 square and anamorphic geometry, legacy presets\n";
}
