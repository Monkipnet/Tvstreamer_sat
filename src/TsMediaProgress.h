#pragma once

#include <cstddef>
#include <cstdint>

namespace tvs::ts_media_progress {

// Read a PES PTS carried wholly in the first TS packet. Partial PES headers,
// encrypted payloads, malformed marker bits and adaptation-only packets are
// intentionally ignored; absence of a readable PTS must not trigger a stall.
inline bool videoPesPts90k(const uint8_t* packet, std::size_t size, uint64_t& pts) {
    if (!packet || size < 188 || packet[0] != 0x47 ||
        (packet[1] & 0xC0U) != 0x40U || (packet[3] & 0xC0U) != 0) {
        return false;
    }
    const uint8_t control = static_cast<uint8_t>((packet[3] >> 4) & 3U);
    if (control != 1 && control != 3) return false;
    std::size_t offset = 4;
    if (control == 3) {
        offset += 1 + packet[4];
        if (offset > 188) return false;
    }
    if (offset + 14 > 188 || packet[offset] != 0 || packet[offset + 1] != 0 ||
        packet[offset + 2] != 1 || (packet[offset + 7] & 0xC0U) < 0x80U ||
        packet[offset + 8] < 5) return false;
    const uint8_t* p = packet + offset + 9;
    if ((p[0] & 0xF0U) != 0x20U && (p[0] & 0xF0U) != 0x30U) return false;
    if (!(p[0] & 1U) || !(p[2] & 1U) || !(p[4] & 1U)) return false;
    pts = (static_cast<uint64_t>((p[0] >> 1) & 7U) << 30) |
          (static_cast<uint64_t>(p[1]) << 22) |
          (static_cast<uint64_t>(p[2] >> 1) << 15) |
          (static_cast<uint64_t>(p[3]) << 7) |
          static_cast<uint64_t>(p[4] >> 1);
    return true;
}

} // namespace tvs::ts_media_progress
