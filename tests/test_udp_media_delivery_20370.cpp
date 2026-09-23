#include "../src/UdpMediaDeliveryHealth.h"
#include <array>
#include <cassert>
#include <cstdint>
#include <iostream>

std::array<uint8_t, 188> packet(uint16_t pid, uint64_t pts, bool pes = true) {
    std::array<uint8_t, 188> p {};
    p.fill(0xff);
    p[0] = 0x47;
    p[1] = static_cast<uint8_t>(((pid >> 8) & 0x1f) | (pes ? 0x40 : 0));
    p[2] = static_cast<uint8_t>(pid);
    p[3] = 0x10;
    if (pes) {
        p[4] = 0; p[5] = 0; p[6] = 1;
        p[7] = 0xe0; p[8] = 0; p[9] = 0;
        p[10] = 0x80; p[11] = 0x80; p[12] = 5;
        p[13] = static_cast<uint8_t>(0x20U | (((pts >> 30) & 7U) << 1) | 1U);
        p[14] = static_cast<uint8_t>(pts >> 22);
        p[15] = static_cast<uint8_t>(((pts >> 15) & 0x7fU) << 1 | 1U);
        p[16] = static_cast<uint8_t>(pts >> 7);
        p[17] = static_cast<uint8_t>((pts & 0x7fU) << 1 | 1U);
    }
    return p;
}

int main() {
    using tvs::udp_media_delivery::inspectSuccessfulDatagram;
    bool known = false;
    uint64_t previous = 0;
    const auto video1 = packet(258, 90000);
    const auto video2 = packet(258, 93600);
    const auto audio = packet(257, 93600, false);
    const auto nullPacket = packet(0x1fff, 93600, false);
    auto stats = inspectSuccessfulDatagram(nullPacket.data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 0 && stats.videoPtsAdvances == 0 && !known);
    stats = inspectSuccessfulDatagram(video1.data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 1 && stats.videoPesStarts == 1 &&
           stats.videoPtsAdvances == 0 && known);
    stats = inspectSuccessfulDatagram(video1.data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 1 && stats.videoPtsAdvances == 0);
    stats = inspectSuccessfulDatagram(video2.data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 1 && stats.videoPtsAdvances == 1);
    stats = inspectSuccessfulDatagram(audio.data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 1 && stats.videoPesStarts == 0);
    // With video PID unspecified, PAT PID 0 must not be mistaken for video.
    const auto pat = packet(0, 90000, false);
    stats = inspectSuccessfulDatagram(pat.data(), 188, 0, 257, known, previous);
    assert(stats.mediaPackets == 0 && stats.videoPtsAdvances == 0);
    // A different PID and scrambled video are not proof of recoverable video.
    stats = inspectSuccessfulDatagram(packet(300, 1).data(), 188, 258, 257, known, previous);
    assert(stats.mediaPackets == 0);
    auto scrambled = video2;
    scrambled[3] |= 0x80;
    stats = inspectSuccessfulDatagram(scrambled.data(), 188, 258, 257, known, previous);
    assert(stats.videoPesStarts == 0 && stats.videoPtsAdvances == 0);
    using tvs::udp_media_delivery::Fault;
    using tvs::udp_media_delivery::FaultEvidence;
    using tvs::udp_media_delivery::classifyFault;
    FaultEvidence watch {};
    watch.eligible = watch.inputMediaRecent = true;
    watch.startupAgeMs = 25000;
    assert(classifyFault(watch) == Fault::NoMediaStarted);
    watch.inputMediaRecent = false;
    assert(classifyFault(watch) == Fault::None); // upstream problem, not UDP fault
    watch.inputMediaRecent = true;
    watch.startupAgeMs = 19999;
    assert(classifyFault(watch) == Fault::None); // startup reservoir grace
    watch.startupAgeMs = 25000;
    watch.outputMediaEstablished = true;
    watch.outputMediaGapMs = 18000;
    assert(classifyFault(watch) == Fault::MediaStalled);
    watch.outputMediaGapMs = 0;
    watch.inputVideoPtsAdvancing = watch.outputVideoPtsEstablished = true;
    watch.outputVideoPtsGapMs = 18000;
    assert(classifyFault(watch) == Fault::VideoPtsStalled);
    watch.outputVideoPtsGapMs = 0;
    assert(classifyFault(watch) == Fault::None); // fresh video, no rebuild
    watch.outputVideoPtsEstablished = false;
    watch.outputVideoPesStarts = 3;
    assert(classifyFault(watch) == Fault::VideoPtsNeverAdvanced);
    watch.eligible = false;
    assert(classifyFault(watch) == Fault::None); // cooldown / not network source
    std::cout << "PASS: 203.70 UDP TS delivery, NULL/PTS gates and startup/stall/cooldown policy\n";
}
