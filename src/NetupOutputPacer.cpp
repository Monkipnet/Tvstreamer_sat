#include "NetupOutputPacer.h"

#include <gst/app/gstappsink.h>
#include <gst/app/gstappsrc.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <exception>
#include <iostream>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <time.h>
#include <vector>

namespace {

constexpr std::size_t kTsPacketSize = 188;
constexpr std::size_t kTsPacketsPerChunk = 7;
constexpr std::size_t kChunkBytes = kTsPacketSize * kTsPacketsPerChunk;
// One HLS segment is normally ~2 s. Start 1.5 s behind the producer so normal
// playlist/download jitter is absorbed before the first network packet leaves.
constexpr uint64_t kStartupReservoirMilliseconds = 1500ULL;
// Bound each NETUP branch independently. At 7 Mbit/s this is >18 s of TS,
// comfortably above the normal 1.5-3 s operating reservoir while preventing an
// unbounded queue if a downstream sink stops accepting data.
constexpr std::size_t kMaximumBufferedBytes = 16U * 1024U * 1024U;
constexpr uint64_t kNanosecondsPerSecond = 1000000000ULL;
constexpr uint64_t kLateResetIntervals = 4ULL;

uint64_t monotonicNanoseconds() {
    timespec now {};
    if (::clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return static_cast<uint64_t>(now.tv_sec) * kNanosecondsPerSecond +
           static_cast<uint64_t>(now.tv_nsec);
}

timespec toTimespec(uint64_t nanoseconds) {
    timespec value {};
    value.tv_sec = static_cast<time_t>(nanoseconds / kNanosecondsPerSecond);
    value.tv_nsec = static_cast<long>(nanoseconds % kNanosecondsPerSecond);
    return value;
}

void sleepUntilMonotonic(uint64_t deadlineNanoseconds) {
    const timespec deadline = toTimespec(deadlineNanoseconds);
    int result = 0;
    do {
        result = ::clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &deadline, nullptr);
    } while (result == EINTR);
}

uint64_t multiplyDivide(uint64_t value, uint64_t multiplier, uint64_t divisor) {
    if (divisor == 0) return 0;
#if defined(__SIZEOF_INT128__)
    const unsigned __int128 product =
        static_cast<unsigned __int128>(value) * static_cast<unsigned __int128>(multiplier);
    return static_cast<uint64_t>(product / divisor);
#else
    const long double result =
        (static_cast<long double>(value) * static_cast<long double>(multiplier)) /
        static_cast<long double>(divisor);
    if (result >= static_cast<long double>(std::numeric_limits<uint64_t>::max())) {
        return std::numeric_limits<uint64_t>::max();
    }
    return static_cast<uint64_t>(result);
#endif
}

std::size_t roundUpToChunk(std::size_t value) {
    if (value <= kChunkBytes) return kChunkBytes;
    const std::size_t remainder = value % kChunkBytes;
    if (remainder == 0) return value;
    if (value > std::numeric_limits<std::size_t>::max() - (kChunkBytes - remainder)) {
        return value;
    }
    return value + (kChunkBytes - remainder);
}

void fillNullChunk(std::array<guint8, kChunkBytes>& chunk, guint8& continuity) {
    for (std::size_t packetIndex = 0; packetIndex < kTsPacketsPerChunk; ++packetIndex) {
        guint8* packet = chunk.data() + packetIndex * kTsPacketSize;
        std::fill(packet, packet + kTsPacketSize, static_cast<guint8>(0xFF));
        packet[0] = 0x47;
        packet[1] = 0x1F;
        packet[2] = 0xFF;
        packet[3] = static_cast<guint8>(0x10 | (continuity & 0x0F));
        continuity = static_cast<guint8>((continuity + 1U) & 0x0F);
    }
}

class ReservoirBridge {
public:
    ReservoirBridge(
        GstElement* appSrcElement,
        uint64_t bitrate,
        std::string stream,
        std::string type)
        : appSrc(GST_APP_SRC(gst_object_ref(appSrcElement))),
          targetBitrate(bitrate),
          streamId(std::move(stream)),
          outputType(std::move(type)) {
        const uint64_t bytesPerSecond = targetBitrate / 8ULL;
        uint64_t startupBytes64 = multiplyDivide(
            bytesPerSecond, kStartupReservoirMilliseconds, 1000ULL);
        startupBytes64 = std::max<uint64_t>(startupBytes64, kChunkBytes);
        startupBytes = roundUpToChunk(static_cast<std::size_t>(std::min<uint64_t>(
            startupBytes64,
            static_cast<uint64_t>(kMaximumBufferedBytes - kChunkBytes))));
        chunkIntervalNanoseconds = std::max<uint64_t>(1ULL, multiplyDivide(
            kChunkBytes * 8ULL, kNanosecondsPerSecond, targetBitrate));

        senderThread = std::thread(&ReservoirBridge::sendLoop, this);
    }

    ~ReservoirBridge() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
        }
        condition.notify_all();
        if (senderThread.joinable()) {
            senderThread.join();
        }
        if (appSrc) {
            gst_app_src_end_of_stream(appSrc);
            gst_object_unref(appSrc);
            appSrc = nullptr;
        }
    }

    GstFlowReturn pushBuffer(GstBuffer* buffer) {
        if (!buffer) return GST_FLOW_ERROR;

        GstMapInfo map {};
        if (!gst_buffer_map(buffer, &map, GST_MAP_READ)) {
            return GST_FLOW_ERROR;
        }

        if (map.size == 0) {
            gst_buffer_unmap(buffer, &map);
            return GST_FLOW_OK;
        }
        if (map.size > kMaximumBufferedBytes) {
            gst_buffer_unmap(buffer, &map);
            return GST_FLOW_ERROR;
        }
        std::vector<guint8> bytes(map.data, map.data + map.size);
        gst_buffer_unmap(buffer, &map);

        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [this, size = bytes.size()] {
            return stopping || queuedBytes + size <= kMaximumBufferedBytes;
        });
        if (stopping) return GST_FLOW_FLUSHING;

        queuedBytes += bytes.size();
        buffers.emplace_back(std::move(bytes));
        lock.unlock();
        condition.notify_all();
        return GST_FLOW_OK;
    }

    void upstreamEos() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            upstreamEnded = true;
        }
        condition.notify_all();
    }

private:
    bool popChunk(std::array<guint8, kChunkBytes>& chunk) {
        std::lock_guard<std::mutex> lock(mutex);
        if (queuedBytes < kChunkBytes) {
            return false;
        }

        std::size_t written = 0;
        while (written < kChunkBytes && !buffers.empty()) {
            std::vector<guint8>& front = buffers.front();
            const std::size_t available = front.size() - frontOffset;
            const std::size_t copyBytes = std::min(kChunkBytes - written, available);
            std::memcpy(chunk.data() + written, front.data() + frontOffset, copyBytes);
            frontOffset += copyBytes;
            written += copyBytes;
            queuedBytes -= copyBytes;

            if (frontOffset == front.size()) {
                buffers.pop_front();
                frontOffset = 0;
            }
        }
        condition.notify_all();
        return written == kChunkBytes;
    }

    bool waitForStartup() {
        std::unique_lock<std::mutex> lock(mutex);
        condition.wait(lock, [this] {
            return stopping || queuedBytes >= startupBytes ||
                   (upstreamEnded && queuedBytes >= kChunkBytes);
        });
        return !stopping && queuedBytes >= kChunkBytes;
    }

    bool shouldStopAfterEos() {
        std::lock_guard<std::mutex> lock(mutex);
        return upstreamEnded && queuedBytes < kChunkBytes;
    }

    void sendLoop() {
        if (!waitForStartup()) {
            return;
        }

        const uint64_t startQueued = [this] {
            std::lock_guard<std::mutex> lock(mutex);
            return static_cast<uint64_t>(queuedBytes);
        }();
        std::cerr << "NETUP output reservoir 203.53: stream=" << streamId
                  << " type=" << outputType
                  << " event=start"
                  << " target_bitrate=" << targetBitrate
                  << " startup_ms=" << kStartupReservoirMilliseconds
                  << " startup_bytes=" << startupBytes
                  << " queued_bytes=" << startQueued
                  << " chunk_bytes=" << kChunkBytes
                  << " chunk_interval_us=" << (chunkIntervalNanoseconds / 1000ULL)
                  << " clock=monotonic-absolute"
                  << std::endl;

        uint64_t nextDeadline = monotonicNanoseconds();
        uint64_t emittedChunks = 0;
        guint8 nullContinuity = 0;

        while (true) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (stopping) break;
            }

            const uint64_t now = monotonicNanoseconds();
            if (now < nextDeadline) {
                sleepUntilMonotonic(nextDeadline);
            } else if (now > nextDeadline + chunkIntervalNanoseconds * kLateResetIntervals) {
                // Never compensate a scheduler stall by dumping several chunks at
                // once. Reset the wall-clock phase and resume one 1316-byte chunk
                // per interval.
                nextDeadline = now;
            }

            std::array<guint8, kChunkBytes> chunk {};
            const bool haveRealData = popChunk(chunk);
            if (!haveRealData) {
                if (shouldStopAfterEos()) {
                    gst_app_src_end_of_stream(appSrc);
                    break;
                }
                fillNullChunk(chunk, nullContinuity);
                ++underflowChunks;
            }

            GstBuffer* output = gst_buffer_new_allocate(nullptr, kChunkBytes, nullptr);
            if (!output) break;
            gst_buffer_fill(output, 0, chunk.data(), kChunkBytes);
            GST_BUFFER_PTS(output) = GST_CLOCK_TIME_NONE;
            GST_BUFFER_DTS(output) = GST_CLOCK_TIME_NONE;
            GST_BUFFER_DURATION(output) = GST_CLOCK_TIME_NONE;

            const GstFlowReturn flow = gst_app_src_push_buffer(appSrc, output);
            if (flow != GST_FLOW_OK) {
                std::cerr << "NETUP output reservoir 203.53: stream=" << streamId
                          << " type=" << outputType
                          << " event=downstream-stop"
                          << " flow=" << static_cast<int>(flow)
                          << " emitted_chunks=" << emittedChunks
                          << " underflow_chunks=" << underflowChunks
                          << std::endl;
                break;
            }

            ++emittedChunks;
            nextDeadline += chunkIntervalNanoseconds;
        }
    }

    GstAppSrc* appSrc = nullptr;
    const uint64_t targetBitrate = 0;
    const std::string streamId;
    const std::string outputType;
    std::size_t startupBytes = kChunkBytes;
    uint64_t chunkIntervalNanoseconds = 1;

    std::mutex mutex;
    std::condition_variable condition;
    std::deque<std::vector<guint8>> buffers;
    std::size_t frontOffset = 0;
    std::size_t queuedBytes = 0;
    bool stopping = false;
    bool upstreamEnded = false;
    uint64_t underflowChunks = 0;
    std::thread senderThread;
};

GstFlowReturn onNewSample(GstAppSink* sink, gpointer userData) {
    auto* bridge = static_cast<ReservoirBridge*>(userData);
    GstSample* sample = gst_app_sink_pull_sample(sink);
    if (!sample) return GST_FLOW_EOS;
    GstBuffer* buffer = gst_sample_get_buffer(sample);
    const GstFlowReturn result = bridge ? bridge->pushBuffer(buffer) : GST_FLOW_ERROR;
    gst_sample_unref(sample);
    return result;
}

void onEos(GstAppSink*, gpointer userData) {
    auto* bridge = static_cast<ReservoirBridge*>(userData);
    if (bridge) bridge->upstreamEos();
}

void destroyBridge(gpointer userData) {
    delete static_cast<ReservoirBridge*>(userData);
}

} // namespace

namespace NetupOutputPacer {

GstElement* create(
    uint64_t targetBitrate,
    const std::string& streamId,
    const std::string& outputType,
    const std::string& elementName,
    std::string& error) {
    error.clear();
    if (targetBitrate == 0) {
        error = "NETUP reservoir target bitrate must be greater than zero";
        return nullptr;
    }

    GstElement* bin = gst_bin_new(elementName.c_str());
    GstElement* input = gst_element_factory_make("appsink", (elementName + "_input").c_str());
    GstElement* output = gst_element_factory_make("appsrc", (elementName + "_output").c_str());
    if (!bin || !input || !output) {
        error = "failed to create NETUP reservoir appsink/appsrc bridge";
        if (input) gst_object_unref(input);
        if (output) gst_object_unref(output);
        if (bin) gst_object_unref(bin);
        return nullptr;
    }

    GstCaps* caps = gst_caps_new_simple(
        "video/mpegts",
        "systemstream", G_TYPE_BOOLEAN, TRUE,
        "packetsize", G_TYPE_INT, static_cast<gint>(kTsPacketSize),
        nullptr);

    g_object_set(input,
        "sync", FALSE,
        "emit-signals", FALSE,
        "max-buffers", static_cast<guint>(0),
        "drop", FALSE,
        "wait-on-eos", FALSE,
        "caps", caps,
        nullptr);

    g_object_set(output,
        "is-live", TRUE,
        "format", GST_FORMAT_TIME,
        "do-timestamp", FALSE,
        "block", TRUE,
        "max-bytes", static_cast<guint64>(4U * 1024U * 1024U),
        "caps", caps,
        nullptr);
    gst_caps_unref(caps);

    if (!gst_bin_add(GST_BIN(bin), input) || !gst_bin_add(GST_BIN(bin), output)) {
        error = "failed to add NETUP reservoir bridge children";
        gst_object_unref(bin);
        return nullptr;
    }

    GstPad* inputPad = gst_element_get_static_pad(input, "sink");
    GstPad* outputPad = gst_element_get_static_pad(output, "src");
    GstPad* ghostSink = inputPad ? gst_ghost_pad_new("sink", inputPad) : nullptr;
    GstPad* ghostSrc = outputPad ? gst_ghost_pad_new("src", outputPad) : nullptr;
    if (inputPad) gst_object_unref(inputPad);
    if (outputPad) gst_object_unref(outputPad);

    if (!ghostSink || !ghostSrc ||
        !gst_element_add_pad(bin, ghostSink) ||
        !gst_element_add_pad(bin, ghostSrc)) {
        error = "failed to expose NETUP reservoir bridge pads";
        if (ghostSink && !GST_OBJECT_PARENT(ghostSink)) gst_object_unref(ghostSink);
        if (ghostSrc && !GST_OBJECT_PARENT(ghostSrc)) gst_object_unref(ghostSrc);
        gst_object_unref(bin);
        return nullptr;
    }

    ReservoirBridge* bridge = nullptr;
    try {
        bridge = new ReservoirBridge(output, targetBitrate, streamId, outputType);
    } catch (const std::exception& ex) {
        error = std::string("failed to start NETUP reservoir sender: ") + ex.what();
        gst_object_unref(bin);
        return nullptr;
    }

    GstAppSinkCallbacks callbacks {};
    callbacks.eos = onEos;
    callbacks.new_sample = onNewSample;
    gst_app_sink_set_callbacks(GST_APP_SINK(input), &callbacks, bridge, destroyBridge);

    return bin;
}

} // namespace NetupOutputPacer
