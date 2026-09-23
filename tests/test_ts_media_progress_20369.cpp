#include "../src/TsMediaProgress.h"
#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>

using tvs::ts_media_progress::videoPesPts90k;

std::array<uint8_t, 188> packet(uint64_t pts) {
    std::array<uint8_t, 188> p {};
    p.fill(0xFF);
    p[0] = 0x47; p[1] = 0x41; p[2] = 0x02; p[3] = 0x10;
    p[4] = 0; p[5] = 0; p[6] = 1;
    p[7] = 0xE0; p[8] = 0; p[9] = 0;
    p[10] = 0x80; p[11] = 0x80; p[12] = 5;
    p[13] = static_cast<uint8_t>(0x20U | (((pts >> 30) & 7U) << 1) | 1U);
    p[14] = static_cast<uint8_t>(pts >> 22);
    p[15] = static_cast<uint8_t>(((pts >> 15) & 0x7FU) << 1 | 1U);
    p[16] = static_cast<uint8_t>(pts >> 7);
    p[17] = static_cast<uint8_t>((pts & 0x7FU) << 1 | 1U);
    return p;
}

int main() {
    uint64_t decoded = ~uint64_t{0};
    for (auto timestamp : {uint64_t{0}, uint64_t{90000}, (uint64_t{1}<<33)-1}) {
        const auto p = packet(timestamp);
        assert(videoPesPts90k(p.data(), p.size(), decoded));
        assert(decoded == timestamp);
    }
    auto p = packet(90000);
    p[1] = 0x01; // No PES start.
    assert(!videoPesPts90k(p.data(), p.size(), decoded));
    p = packet(90000); p[3] = 0x50; // Scrambled input is not readable.
    assert(!videoPesPts90k(p.data(), p.size(), decoded));
    p = packet(90000); p[15] &= 0xFE; // Invalid marker bit.
    assert(!videoPesPts90k(p.data(), p.size(), decoded));
    p = packet(90000); p[3] = 0x30; p[4] = 180; // Truncated PES header.
    assert(!videoPesPts90k(p.data(), p.size(), decoded));
    p = packet(90000); p[3] = 0x30; p[4] = 1; p[5] = 0;
    for (int i=4; i<=17; ++i) p[i+2] = packet(90000)[i];
    assert(videoPesPts90k(p.data(), p.size(), decoded) && decoded == 90000);
    std::cout << "PASS: TS video PES PTS parsing, 33-bit wrap, malformed/scrambled and adaptation packets\n";
}
