#include "TranscoderModule.h"
#include "TranscodeVideoGeometry.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <iostream>
#include <filesystem>
#include <cstdlib>
#include <sstream>
#include <thread>
#include <unistd.h>
#include <memory>
#include <mutex>

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/wait.h>

namespace {



bool executableInPath(const std::string& name, std::string* path = nullptr) {
    const char* envPath = std::getenv("PATH");
    std::string paths = envPath ? envPath : "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
    std::stringstream ss(paths);
    std::string dir;
    while (std::getline(ss, dir, ':')) {
        if (dir.empty()) continue;
        std::filesystem::path candidate = std::filesystem::path(dir) / name;
        if (::access(candidate.c_str(), X_OK) == 0) {
            if (path) *path = candidate.string();
            return true;
        }
    }
    return false;
}

bool factoryAvailable(const char* name) {
    GstElementFactory* factory = gst_element_factory_find(name);
    if (!factory) return false;
    gst_object_unref(factory);
    return true;
}

struct EncoderProbeResult {
    bool ok = false;
    bool timedOut = false;
    int exitCode = -1;
    int signal = 0;
};

EncoderProbeResult probeVideoEncoderFactory(const std::string& factory) {
    EncoderProbeResult result;
    if (!factoryAvailable(factory.c_str())) return result;

    std::string gstLaunch;
    if (!executableInPath("gst-launch-1.0", &gstLaunch)) return result;

    const pid_t pid = ::fork();
    if (pid < 0) {
        std::cerr << "Intel encoder probe 203.08: factory=" << factory
                  << " result=fork-failed errno=" << errno << std::endl;
        return result;
    }

    if (pid == 0) {
        // A broken qsvh264enc on older Intel generations can abort in libmfx.
        // Probe it in an isolated child and suppress core dumps so the main
        // TVStreammerSAT5 process is never affected by the driver assertion.
        struct rlimit coreLimit {};
        coreLimit.rlim_cur = 0;
        coreLimit.rlim_max = 0;
        ::setrlimit(RLIMIT_CORE, &coreLimit);

        const int nullFd = ::open("/dev/null", O_WRONLY | O_CLOEXEC);
        if (nullFd >= 0) {
            ::dup2(nullFd, STDOUT_FILENO);
            ::dup2(nullFd, STDERR_FILENO);
            if (nullFd > STDERR_FILENO) ::close(nullFd);
        }

        ::execl(gstLaunch.c_str(), gstLaunch.c_str(),
                "-q",
                "videotestsrc", "num-buffers=24", "!",
                "video/x-raw,format=NV12,width=320,height=240,framerate=25/1", "!",
                factory.c_str(), "bitrate=1000", "!",
                "fakesink", "sync=false",
                static_cast<char*>(nullptr));
        ::_exit(127);
    }

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        int status = 0;
        const pid_t done = ::waitpid(pid, &status, WNOHANG);
        if (done == pid) {
            if (WIFEXITED(status)) {
                result.exitCode = WEXITSTATUS(status);
                result.ok = result.exitCode == 0;
            } else if (WIFSIGNALED(status)) {
                result.signal = WTERMSIG(status);
            }
            return result;
        }
        if (done < 0 && errno != EINTR) return result;
        if (std::chrono::steady_clock::now() >= deadline) {
            result.timedOut = true;
            ::kill(pid, SIGKILL);
            while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
            return result;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

std::string intelVideoEncoderFactory() {
    return TranscoderModule::workingIntelVideoEncoderFactory();
}

bool isIntelVideoEncoder(const std::string& factory) {
    return factory == "qsvh264enc" || factory == "vah264enc" || factory == "vaapih264enc";
}

std::string selectedVideoEncoderFactory(const StreamConfig& config) {
    const std::string requested = config.transcodeVideoEncoder;
    if (requested == "nvenc") return factoryAvailable("nvh264enc") ? "nvh264enc" : std::string();
    if (requested == "intel") return intelVideoEncoderFactory();
    if (requested == "x264") return factoryAvailable("x264enc") ? "x264enc" : std::string();
    // Auto: discrete NVIDIA first, then Intel hardware, then CPU x264.
    if (factoryAvailable("nvh264enc")) return "nvh264enc";
    if (const std::string intel = intelVideoEncoderFactory(); !intel.empty()) return intel;
    if (factoryAvailable("x264enc")) return "x264enc";
    return {};
}

void setUIntIfPresent(GstElement* element, const char* property, guint value) {
    if (element && g_object_class_find_property(G_OBJECT_GET_CLASS(element), property)) {
        g_object_set(element, property, value, nullptr);
    }
}

void setBoolIfPresent(GstElement* element, const char* property, gboolean value) {
    if (element && g_object_class_find_property(G_OBJECT_GET_CLASS(element), property)) {
        g_object_set(element, property, value, nullptr);
    }
}

void setEnumIfPresent(GstElement* element, const char* property, const char* value) {
    if (element && g_object_class_find_property(G_OBJECT_GET_CLASS(element), property)) {
        gst_util_set_object_arg(G_OBJECT(element), property, value);
    }
}

void configureVideoEncoder(GstElement* encoder, const std::string& factory, guint bitrateKbps) {
    if (!encoder) return;
    if (factory == "nvh264enc") {
        g_object_set(encoder,
            "bitrate", bitrateKbps,
            "gop-size", 50,
            "bframes", 0u,
            "aud", TRUE,
            "zerolatency", TRUE,
            "repeat-sequence-header", TRUE,
            "strict-gop", TRUE,
            "vbv-buffer-size", std::max<guint>(bitrateKbps, 500u),
            nullptr);
        setEnumIfPresent(encoder, "rc-mode", "cbr");
        return;
    }
    if (isIntelVideoEncoder(factory)) {
        setUIntIfPresent(encoder, "bitrate", bitrateKbps);
        setUIntIfPresent(encoder, "b-frames", 0);
        setBoolIfPresent(encoder, "aud", TRUE);
        if (factory == "qsvh264enc") {
            setUIntIfPresent(encoder, "gop-size", 50);
            setUIntIfPresent(encoder, "idr-interval", 0);
            setEnumIfPresent(encoder, "rate-control", "cbr");
        } else if (factory == "vah264enc") {
            setUIntIfPresent(encoder, "key-int-max", 50);
            setEnumIfPresent(encoder, "rate-control", "cbr");
            setUIntIfPresent(encoder, "target-usage", 4);
        } else {
            // Legacy vaapih264enc names differ across GStreamer releases; only
            // set properties when the installed element actually exposes them.
            setUIntIfPresent(encoder, "keyframe-period", 50);
            setUIntIfPresent(encoder, "key-int-max", 50);
            setEnumIfPresent(encoder, "rate-control", "cbr");
        }
        return;
    }

    g_object_set(encoder,
        "bitrate", bitrateKbps,
        "key-int-max", 50,
        "bframes", 2,
        "byte-stream", TRUE,
        "aud", TRUE,
        "vbv-buf-capacity", 1000u,
        nullptr);
    gst_util_set_object_arg(G_OBJECT(encoder), "speed-preset", "veryfast");
    gst_util_set_object_arg(G_OBJECT(encoder), "tune", "zerolatency");
    g_object_set(encoder, "option-string",
        "nal-hrd=cbr:force-cfr=1:repeat-headers=1:scenecut=0", nullptr);
}

struct TimestampNormalizer {
    std::mutex mutex;
    GstClockTime lastPts = GST_CLOCK_TIME_NONE;
    GstClockTime lastDts = GST_CLOCK_TIME_NONE;
    GstClockTime fallbackDuration = 20 * GST_MSECOND;
};

GstPadProbeReturn normalizeEncodedTimestamps(GstPad*, GstPadProbeInfo* info, gpointer userData) {
    if (!(GST_PAD_PROBE_INFO_TYPE(info) & GST_PAD_PROBE_TYPE_BUFFER)) return GST_PAD_PROBE_OK;
    auto* state = static_cast<TimestampNormalizer*>(userData);
    GstBuffer* input = GST_PAD_PROBE_INFO_BUFFER(info);
    if (!state || !input) return GST_PAD_PROBE_OK;

    GstBuffer* buffer = gst_buffer_make_writable(input);
    if (!buffer) return GST_PAD_PROBE_OK;
    GST_PAD_PROBE_INFO_DATA(info) = buffer;

    std::lock_guard<std::mutex> lock(state->mutex);
    GstClockTime duration = GST_BUFFER_DURATION(buffer);
    if (!GST_CLOCK_TIME_IS_VALID(duration) || duration == 0) duration = state->fallbackDuration;

    GstClockTime pts = GST_BUFFER_PTS(buffer);
    GstClockTime dts = GST_BUFFER_DTS(buffer);

    if (!GST_CLOCK_TIME_IS_VALID(pts)) {
        pts = GST_CLOCK_TIME_IS_VALID(state->lastPts) ? state->lastPts + duration : 0;
    } else if (GST_CLOCK_TIME_IS_VALID(state->lastPts) && pts <= state->lastPts) {
        pts = state->lastPts + duration;
    }

    if (!GST_CLOCK_TIME_IS_VALID(dts)) {
        dts = GST_CLOCK_TIME_IS_VALID(state->lastDts) ? state->lastDts + duration : pts;
    } else if (GST_CLOCK_TIME_IS_VALID(state->lastDts) && dts <= state->lastDts) {
        dts = state->lastDts + duration;
    }

    if (dts > pts) pts = dts;
    GST_BUFFER_PTS(buffer) = pts;
    GST_BUFFER_DTS(buffer) = dts;
    GST_BUFFER_DURATION(buffer) = duration;
    state->lastPts = pts;
    state->lastDts = dts;
    return GST_PAD_PROBE_OK;
}

void attachTimestampNormalizer(GstElement* element, GstClockTime fallbackDuration) {
    if (!element) return;
    GstPad* srcPad = gst_element_get_static_pad(element, "src");
    if (!srcPad) return;
    auto* state = new TimestampNormalizer();
    state->fallbackDuration = fallbackDuration;
    gst_pad_add_probe(srcPad, GST_PAD_PROBE_TYPE_BUFFER, normalizeEncodedTimestamps, state,
        [](gpointer data) { delete static_cast<TimestampNormalizer*>(data); });
    gst_object_unref(srcPad);
}

struct TranscodeContext {
    GstElement* bin = nullptr;
    GstElement* mux = nullptr;
    StreamConfig config;
    bool videoLinked = false;
    bool audioLinked = false;
};

bool add(GstElement* bin, GstElement* element) {
    return bin && element && gst_bin_add(GST_BIN(bin), element);
}

void sync(GstElement* element) {
    if (element) gst_element_sync_state_with_parent(element);
}


bool gstValueCanContainInt(const GValue* value, gint expected) {
    if (!value) return false;
    if (G_VALUE_HOLDS_INT(value)) {
        return g_value_get_int(value) == expected;
    }
    if (GST_VALUE_HOLDS_INT_RANGE(value)) {
        return expected >= gst_value_get_int_range_min(value) &&
               expected <= gst_value_get_int_range_max(value);
    }
    if (GST_VALUE_HOLDS_LIST(value) || GST_VALUE_HOLDS_ARRAY(value)) {
        const guint count = GST_VALUE_HOLDS_LIST(value)
            ? gst_value_list_get_size(value)
            : gst_value_array_get_size(value);
        for (guint i = 0; i < count; ++i) {
            const GValue* item = GST_VALUE_HOLDS_LIST(value)
                ? gst_value_list_get_value(value, i)
                : gst_value_array_get_value(value, i);
            if (gstValueCanContainInt(item, expected)) return true;
        }
    }
    return false;
}

bool structureFieldCanContainInt(const GstStructure* structure, const char* field, gint expected) {
    return structure && gstValueCanContainInt(gst_structure_get_value(structure, field), expected);
}

bool structureFieldStringCanContain(const GstStructure* structure, const char* field, const char* expected) {
    if (!structure || !field || !expected) return false;
    const GValue* value = gst_structure_get_value(structure, field);
    if (!value) return false;
    if (G_VALUE_HOLDS_STRING(value)) {
        return g_strcmp0(g_value_get_string(value), expected) == 0;
    }
    if (GST_VALUE_HOLDS_LIST(value) || GST_VALUE_HOLDS_ARRAY(value)) {
        const guint count = GST_VALUE_HOLDS_LIST(value)
            ? gst_value_list_get_size(value)
            : gst_value_array_get_size(value);
        for (guint i = 0; i < count; ++i) {
            const GValue* item = GST_VALUE_HOLDS_LIST(value)
                ? gst_value_list_get_value(value, i)
                : gst_value_array_get_value(value, i);
            if (G_VALUE_HOLDS_STRING(item) && g_strcmp0(g_value_get_string(item), expected) == 0) {
                return true;
            }
        }
    }
    return false;
}

struct AudioEncoderSelection {
    GstElement* element = nullptr;
    std::string factory;
};

AudioEncoderSelection makeAudioEncoder(const std::string& codec) {
    const char* const* factories = nullptr;
    static const char* aacFactories[] = {"fdkaacenc", "voaacenc", "avenc_aac", nullptr};
    static const char* mp3Factories[] = {"lamemp3enc", "avenc_mp3", nullptr};
    factories = codec == "mp3" ? mp3Factories : aacFactories;
    for (const char* const* name = factories; *name; ++name) {
        GstElementFactory* factory = gst_element_factory_find(*name);
        if (factory) {
            gst_object_unref(factory);
            return {gst_element_factory_make(*name, nullptr), *name};
        }
    }
    return {};
}

void configureAudioBitrate(GstElement* encoder, const std::string& factory, uint64_t bitrate) {
    if (!encoder) return;
    bitrate = std::clamp<uint64_t>(bitrate, 64000, 320000);

    if (factory == "lamemp3enc") {
        // lamemp3enc expects kbit/s and requires bitrate mode for the cbr flag
        // and bitrate property to take effect.
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(encoder), "target")) {
            gst_util_set_object_arg(G_OBJECT(encoder), "target", "bitrate");
        }
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(encoder), "cbr")) {
            g_object_set(encoder, "cbr", TRUE, nullptr);
        }
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(encoder), "bitrate")) {
            g_object_set(encoder, "bitrate", static_cast<gint>(bitrate / 1000), nullptr);
        }
        return;
    }

    // libav AAC/MP3 and the native AAC encoders use bits/s.
    if (g_object_class_find_property(G_OBJECT_GET_CLASS(encoder), "bitrate")) {
        g_object_set(encoder, "bitrate", static_cast<gint>(bitrate), nullptr);
    }
}

bool linkElementToMux(GstElement* source, GstElement* mux) {
    if (!source || !mux) return false;
    GstPad* srcPad = gst_element_get_static_pad(source, "src");
    GstPad* sinkPad = gst_element_request_pad_simple(mux, "sink_%d");
    if (!srcPad || !sinkPad) {
        if (srcPad) gst_object_unref(srcPad);
        if (sinkPad) {
            gst_element_release_request_pad(mux, sinkPad);
            gst_object_unref(sinkPad);
        }
        return false;
    }
    const bool ok = gst_pad_link(srcPad, sinkPad) == GST_PAD_LINK_OK;
    if (!ok) gst_element_release_request_pad(mux, sinkPad);
    gst_object_unref(srcPad);
    gst_object_unref(sinkPad);
    return ok;
}

void drainPad(GstElement* bin, GstPad* pad) {
    if (!bin || !pad || gst_pad_is_linked(pad)) return;
    GstElement* queue = gst_element_factory_make("queue", nullptr);
    GstElement* sink = gst_element_factory_make("fakesink", nullptr);
    if (!queue || !sink || !add(bin, queue) || !add(bin, sink) || !gst_element_link(queue, sink)) {
        if (queue && !GST_OBJECT_PARENT(queue)) gst_object_unref(queue);
        if (sink && !GST_OBJECT_PARENT(sink)) gst_object_unref(sink);
        return;
    }
    g_object_set(sink, "sync", FALSE, "async", FALSE, nullptr);
    GstPad* queueSink = gst_element_get_static_pad(queue, "sink");
    if (queueSink) {
        gst_pad_link(pad, queueSink);
        gst_object_unref(queueSink);
    }
    sync(queue);
    sync(sink);
}

void onDecodedPadAdded(GstElement*, GstPad* pad, gpointer userData) {
    auto* context = static_cast<TranscodeContext*>(userData);
    if (!context || !context->bin || !context->mux) return;

    GstCaps* caps = gst_pad_get_current_caps(pad);
    if (!caps) caps = gst_pad_query_caps(pad, nullptr);
    if (!caps || gst_caps_is_empty(caps)) {
        if (caps) gst_caps_unref(caps);
        drainPad(context->bin, pad);
        return;
    }
    const GstStructure* structure = gst_caps_get_structure(caps, 0);
    const std::string media = gst_structure_get_name(structure);

    if (media.rfind("video/x-raw", 0) == 0 && !context->videoLinked) {
        tvs::transcode::VideoGeometry geometry;
        if (!tvs::transcode::videoGeometry(context->config.transcodeResolution, geometry)) {
            std::cerr << "Transcoder: unsupported output video geometry" << std::endl;
            gst_caps_unref(caps);
            drainPad(context->bin, pad);
            return;
        }
        const int width = geometry.width;
        const int height = geometry.height;
        const guint bitrateKbps = static_cast<guint>(
            std::max<uint64_t>(500000, context->config.transcodeVideoBitrate) / 1000);

        const std::string videoEncoderFactory = selectedVideoEncoderFactory(context->config);
        GstElement* queue = gst_element_factory_make("queue", nullptr);
        GstElement* convert = gst_element_factory_make("videoconvert", nullptr);
        GstElement* deinterlace = gst_element_factory_make("deinterlace", nullptr);
        GstElement* scale = gst_element_factory_make("videoscale", nullptr);
        GstElement* postScaleConvert = (videoEncoderFactory == "nvh264enc" || isIntelVideoEncoder(videoEncoderFactory))
            ? gst_element_factory_make("videoconvert", nullptr)
            : nullptr;
        GstElement* filter = gst_element_factory_make("capsfilter", nullptr);
        GstElement* encoder = videoEncoderFactory.empty()
            ? nullptr
            : gst_element_factory_make(videoEncoderFactory.c_str(), nullptr);
        GstElement* parser = gst_element_factory_make("h264parse", nullptr);
        GstElement* outQueue = gst_element_factory_make("queue", nullptr);
        if (!queue || !convert || !deinterlace || !scale ||
            ((videoEncoderFactory == "nvh264enc" || isIntelVideoEncoder(videoEncoderFactory)) && !postScaleConvert) ||
            !filter || !encoder || !parser || !outQueue) {
            std::cerr << "Transcoder: missing video elements" << std::endl;
            gst_caps_unref(caps);
            drainPad(context->bin, pad);
            return;
        }

        const char* rawFormat = (videoEncoderFactory == "nvh264enc" || isIntelVideoEncoder(videoEncoderFactory)) ? "NV12" : "I420";
        GstCaps* rawCaps = gst_caps_new_simple("video/x-raw",
            "format", G_TYPE_STRING, rawFormat,
            "width", G_TYPE_INT, width,
            "height", G_TYPE_INT, height,
            "pixel-aspect-ratio", GST_TYPE_FRACTION,
            geometry.pixelAspectNum, geometry.pixelAspectDen,
            "interlace-mode", G_TYPE_STRING, "progressive",
            nullptr);
        g_object_set(filter, "caps", rawCaps, nullptr);
        gst_caps_unref(rawCaps);
        // 202.73: preserve temporal resolution. 576i25/1080i25 carries 50 fields/s;
        // YADIF all-fields converts that to 50 progressive frames/s. Progressive
        // sources pass through auto-strict without an artificial videorate stage.
        gst_util_set_object_arg(G_OBJECT(deinterlace), "method", "yadif");
        gst_util_set_object_arg(G_OBJECT(deinterlace), "mode", "auto-strict");
        gst_util_set_object_arg(G_OBJECT(deinterlace), "fields", "all");
        gst_util_set_object_arg(G_OBJECT(deinterlace), "locking", "passive");
        configureVideoEncoder(encoder, videoEncoderFactory, bitrateKbps);
        // 202.74: repeat parameter sets with every IDR so late SRT/UDP subscribers
        // acquire decoder configuration immediately at the next keyframe.
        g_object_set(parser, "config-interval", -1, nullptr);

        const bool elementsAdded =
            add(context->bin, queue) && add(context->bin, convert) && add(context->bin, deinterlace) &&
            add(context->bin, scale) && (!postScaleConvert || add(context->bin, postScaleConvert)) &&
            add(context->bin, filter) && add(context->bin, encoder) && add(context->bin, parser) &&
            add(context->bin, outQueue);
        const bool videoLinked = elementsAdded &&
            (postScaleConvert
                ? gst_element_link_many(queue, convert, deinterlace, scale, postScaleConvert,
                                        filter, encoder, parser, outQueue, nullptr)
                : gst_element_link_many(queue, convert, deinterlace, scale,
                                        filter, encoder, parser, outQueue, nullptr));
        if (!elementsAdded || !videoLinked || !linkElementToMux(outQueue, context->mux)) {
            std::cerr << "Transcoder: failed to build video branch" << std::endl;
            gst_caps_unref(caps);
            drainPad(context->bin, pad);
            return;
        }

        GstPad* sinkPad = gst_element_get_static_pad(queue, "sink");
        if (sinkPad && gst_pad_link(pad, sinkPad) == GST_PAD_LINK_OK) {
            context->videoLinked = true;
            std::cerr << "Transcoder: video linked using " << videoEncoderFactory << " "
                      << width << "x" << height << " @ "
                      << context->config.transcodeVideoBitrate
                      << " bit/s cadence=preserve-progressive/double-interlaced-fields"
                      << " headers=every-idr" << std::endl;
        }
        if (sinkPad) gst_object_unref(sinkPad);
        for (GstElement* e : {queue, convert, deinterlace, scale, postScaleConvert, filter, encoder, parser, outQueue}) sync(e);
    } else if (media.rfind("audio/x-raw", 0) == 0 && !context->audioLinked) {
        const std::string codec = context->config.transcodeAudioCodec == "mp3" ? "mp3" : "aac";
        GstElement* queue = gst_element_factory_make("queue", nullptr);
        GstElement* convert = gst_element_factory_make("audioconvert", nullptr);
        GstElement* resample = gst_element_factory_make("audioresample", nullptr);
        GstElement* rate = gst_element_factory_make("audiorate", nullptr);
        GstElement* filter = gst_element_factory_make("capsfilter", nullptr);
        const auto encoderSelection = makeAudioEncoder(codec);
        GstElement* encoder = encoderSelection.element;
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(resample), "quality")) {
            g_object_set(resample, "quality", 6, nullptr);
        }
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(rate), "skip-to-first")) {
            g_object_set(rate, "skip-to-first", TRUE, nullptr);
        }
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(rate), "tolerance")) {
            g_object_set(rate, "tolerance", static_cast<guint64>(20 * GST_MSECOND), nullptr);
        }
        GstElement* parser = gst_element_factory_make(codec == "mp3" ? "mpegaudioparse" : "aacparse", nullptr);
        // MP3 keeps an explicit caps filter. AAC must negotiate directly from aacparse
        // to mpegtsmux so codec_data (AudioSpecificConfig) is preserved unchanged.
        GstElement* encodedFilter = codec == "mp3" ? gst_element_factory_make("capsfilter", nullptr) : nullptr;
        GstElement* outQueue = gst_element_factory_make("queue", nullptr);
        if (!queue || !convert || !resample || !rate || !filter || !encoder || !parser ||
            (codec == "mp3" && !encodedFilter) || !outQueue) {
            std::cerr << "Transcoder: missing " << codec << " audio elements" << std::endl;
            gst_caps_unref(caps);
            drainPad(context->bin, pad);
            return;
        }

        // Use the exact PCM format accepted by the selected encoder.
        const char* rawAudioFormat = "S16LE";
        if (encoderSelection.factory == "avenc_aac") {
            rawAudioFormat = "F32LE";
        } else if (encoderSelection.factory == "avenc_mp3") {
            rawAudioFormat = "S16P";
        }
        const char* rawAudioLayout = encoderSelection.factory == "avenc_mp3"
            ? "non-interleaved"
            : "interleaved";
        GstCaps* audioCaps = gst_caps_new_simple("audio/x-raw",
            "format", G_TYPE_STRING, rawAudioFormat,
            "rate", G_TYPE_INT, 48000,
            "channels", G_TYPE_INT, 2,
            "layout", G_TYPE_STRING, rawAudioLayout,
            nullptr);
        g_object_set(filter, "caps", audioCaps, nullptr);
        gst_caps_unref(audioCaps);
        configureAudioBitrate(encoder, encoderSelection.factory, context->config.transcodeAudioBitrate);
        if (g_object_class_find_property(G_OBJECT_GET_CLASS(parser), "disable-passthrough")) {
            g_object_set(parser, "disable-passthrough", TRUE, nullptr);
        }

        if (codec == "mp3") {
            GstCaps* encodedCaps = gst_caps_from_string(
                "audio/mpeg,mpegversion=(int)1,layer=(int)3,parsed=(boolean)true,rate=(int)48000,channels=(int)2");
            g_object_set(encodedFilter, "caps", encodedCaps, nullptr);
            gst_caps_unref(encodedCaps);
        }

        // Normalize timestamps on the last encoded element before the mux. For AAC this
        // is aacparse itself, deliberately with no downstream capsfilter that could strip
        // codec_data. For MP3 it remains the explicit encoded caps filter.
        const GstClockTime audioFrameDuration = codec == "mp3"
            ? gst_util_uint64_scale_int(GST_SECOND, 1152, 48000)
            : gst_util_uint64_scale_int(GST_SECOND, 1024, 48000);
        attachTimestampNormalizer(codec == "mp3" ? encodedFilter : parser, audioFrameDuration);

        bool branchBuilt = add(context->bin, queue) && add(context->bin, convert) &&
            add(context->bin, resample) && add(context->bin, rate) && add(context->bin, filter) &&
            add(context->bin, encoder) && add(context->bin, parser);
        if (branchBuilt && encodedFilter) branchBuilt = add(context->bin, encodedFilter);
        branchBuilt = branchBuilt && add(context->bin, outQueue);

        bool branchLinked = false;
        if (branchBuilt) {
            if (encodedFilter) {
                branchLinked = gst_element_link_many(queue, convert, resample, rate, filter,
                    encoder, parser, encodedFilter, outQueue, nullptr);
            } else {
                branchLinked = gst_element_link_many(queue, convert, resample, rate, filter,
                    encoder, parser, outQueue, nullptr);
            }
        }

        if (!branchBuilt || !branchLinked || !linkElementToMux(outQueue, context->mux)) {
            std::cerr << "Transcoder: failed to build " << codec << " audio branch with "
                      << encoderSelection.factory << std::endl;
            gst_caps_unref(caps);
            drainPad(context->bin, pad);
            return;
        }

        GstPad* sinkPad = gst_element_get_static_pad(queue, "sink");
        if (sinkPad && gst_pad_link(pad, sinkPad) == GST_PAD_LINK_OK) {
            context->audioLinked = true;
            std::cerr << "Transcoder: audio linked using " << encoderSelection.factory
                      << " input=" << rawAudioFormat << "/" << rawAudioLayout << "/48000/stereo"
                      << " output=" << (codec == "aac" ? "AAC negotiated by aacparse" : "MP3")
                      << " at " << context->config.transcodeAudioBitrate << " bit/s" << std::endl;
        }
        if (sinkPad) gst_object_unref(sinkPad);
        for (GstElement* e : {queue, convert, resample, rate, filter, encoder, parser, encodedFilter, outQueue}) sync(e);
    } else {
        // Multiple programs, subtitles, data PIDs, and duplicate audio/video tracks must
        // be consumed. Leaving a tsdemux pad unlinked can propagate GST_FLOW_NOT_LINKED
        // and stop the complete stream.
        drainPad(context->bin, pad);
    }
    gst_caps_unref(caps);
}


bool buildVideoPassthroughBranch(TranscodeContext* context, GstPad* pad, GstCaps* caps) {
    if (!context || !context->bin || !context->mux || !pad || !caps || context->videoLinked) return false;

    const GstStructure* structure = gst_caps_get_structure(caps, 0);
    if (!structure) return false;
    const char* mediaType = gst_structure_get_name(structure);
    std::string parserFactory;

    if (g_strcmp0(mediaType, "video/x-h264") == 0) {
        parserFactory = "h264parse";
    } else if (g_strcmp0(mediaType, "video/x-h265") == 0) {
        parserFactory = "h265parse";
    } else if (g_strcmp0(mediaType, "video/mpeg") == 0) {
        parserFactory = "mpegvideoparse";
    }

    if (parserFactory.empty()) {
        gchar* capsText = gst_caps_to_string(caps);
        std::cerr << "Transcoder: video passthrough does not support caps="
                  << (capsText ? capsText : "unknown") << std::endl;
        g_free(capsText);
        return false;
    }

    GstElement* queue = gst_element_factory_make("queue", nullptr);
    GstElement* parser = gst_element_factory_make(parserFactory.c_str(), nullptr);
    GstElement* outQueue = gst_element_factory_make("queue", nullptr);
    if (!queue || !parser || !outQueue || !add(context->bin, queue) ||
        !add(context->bin, parser) || !add(context->bin, outQueue) ||
        !gst_element_link_many(queue, parser, outQueue, nullptr) ||
        !linkElementToMux(outQueue, context->mux)) {
        std::cerr << "Transcoder: failed to build video passthrough branch using "
                  << parserFactory << std::endl;
        return false;
    }

    if (g_object_class_find_property(G_OBJECT_GET_CLASS(parser), "disable-passthrough")) {
        g_object_set(parser, "disable-passthrough", FALSE, nullptr);
    }
    // Make late SRT/UDP subscribers recover quickly from a mid-GOP join.
    if ((parserFactory == "h264parse" || parserFactory == "h265parse") &&
        g_object_class_find_property(G_OBJECT_GET_CLASS(parser), "config-interval")) {
        g_object_set(parser, "config-interval", -1, nullptr);
    }

    GstPad* sinkPad = gst_element_get_static_pad(queue, "sink");
    const bool linked = sinkPad && gst_pad_link(pad, sinkPad) == GST_PAD_LINK_OK;
    if (sinkPad) gst_object_unref(sinkPad);
    if (!linked) return false;

    context->videoLinked = true;
    gchar* capsText = gst_caps_to_string(caps);
    std::cerr << "Transcoder: original video passthrough linked using " << parserFactory
              << " caps=" << (capsText ? capsText : "unknown") << std::endl;
    g_free(capsText);
    sync(queue);
    sync(parser);
    sync(outQueue);
    return true;
}

bool buildAudioPassthroughBranch(TranscodeContext* context, GstPad* pad, GstCaps* caps) {
    if (!context || !context->bin || !context->mux || !pad || !caps || context->audioLinked) return false;

    const GstStructure* structure = gst_caps_get_structure(caps, 0);
    if (!structure) return false;
    const char* mediaType = gst_structure_get_name(structure);
    std::string parserFactory;

    if (g_strcmp0(mediaType, "audio/mpeg") == 0) {
        // parsebin may expose non-fixed caps such as:
        // audio/mpeg, mpegversion=(int){ 2, 4 }, stream-format=(string){ raw, adts, adif, loas }
        // gst_structure_get_int() fails on lists/ranges, so inspect the GValue directly.
        const bool canBeMpegAudio = structureFieldCanContainInt(structure, "mpegversion", 1) ||
            structureFieldCanContainInt(structure, "layer", 1) ||
            structureFieldCanContainInt(structure, "layer", 2) ||
            structureFieldCanContainInt(structure, "layer", 3);
        const bool canBeAac = structureFieldCanContainInt(structure, "mpegversion", 4) ||
            structureFieldCanContainInt(structure, "mpegversion", 2) ||
            structureFieldStringCanContain(structure, "stream-format", "adts") ||
            structureFieldStringCanContain(structure, "stream-format", "raw") ||
            structureFieldStringCanContain(structure, "stream-format", "loas") ||
            structureFieldStringCanContain(structure, "stream-format", "adif");

        if (canBeMpegAudio && !canBeAac) parserFactory = "mpegaudioparse";
        else if (canBeAac) parserFactory = "aacparse";
        else parserFactory = "aacparse";
    } else if (g_strcmp0(mediaType, "audio/x-ac3") == 0 ||
               g_strcmp0(mediaType, "audio/x-eac3") == 0) {
        parserFactory = "ac3parse";
    }

    if (parserFactory.empty()) {
        gchar* capsText = gst_caps_to_string(caps);
        std::cerr << "Transcoder: audio passthrough does not support caps="
                  << (capsText ? capsText : "unknown") << std::endl;
        g_free(capsText);
        return false;
    }

    GstElement* queue = gst_element_factory_make("queue", nullptr);
    GstElement* parser = gst_element_factory_make(parserFactory.c_str(), nullptr);
    GstElement* outQueue = gst_element_factory_make("queue", nullptr);
    if (!queue || !parser || !outQueue || !add(context->bin, queue) ||
        !add(context->bin, parser) || !add(context->bin, outQueue) ||
        !gst_element_link_many(queue, parser, outQueue, nullptr) ||
        !linkElementToMux(outQueue, context->mux)) {
        std::cerr << "Transcoder: failed to build audio passthrough branch using "
                  << parserFactory << std::endl;
        return false;
    }

    if (g_object_class_find_property(G_OBJECT_GET_CLASS(parser), "disable-passthrough")) {
        g_object_set(parser, "disable-passthrough", FALSE, nullptr);
    }

    GstPad* sinkPad = gst_element_get_static_pad(queue, "sink");
    const bool linked = sinkPad && gst_pad_link(pad, sinkPad) == GST_PAD_LINK_OK;
    if (sinkPad) gst_object_unref(sinkPad);
    if (!linked) return false;

    context->audioLinked = true;
    gchar* capsText = gst_caps_to_string(caps);
    std::cerr << "Transcoder: original audio passthrough linked using " << parserFactory
              << " caps=" << (capsText ? capsText : "unknown") << std::endl;
    g_free(capsText);
    sync(queue);
    sync(parser);
    sync(outQueue);
    return true;
}

void onDemuxPadAdded(GstElement*, GstPad* pad, gpointer userData) {
    auto* context = static_cast<TranscodeContext*>(userData);
    if (!context || !context->bin) return;

    GstCaps* caps = gst_pad_get_current_caps(pad);
    if (!caps) caps = gst_pad_query_caps(pad, nullptr);
    std::string capsText;
    if (caps) {
        gchar* serializedCaps = gst_caps_to_string(caps);
        if (serializedCaps) {
            capsText = serializedCaps;
            g_free(serializedCaps);
        }
        gst_caps_unref(caps);
    }
    const bool mediaPad = capsText.find("video/") != std::string::npos ||
                          capsText.find("audio/") != std::string::npos;
    if (!mediaPad) {
        drainPad(context->bin, pad);
        return;
    }

    if (context->config.transcodeVideoCodec == "copy" &&
        capsText.find("video/") != std::string::npos) {
        GstCaps* videoCaps = gst_pad_get_current_caps(pad);
        if (!videoCaps) videoCaps = gst_pad_query_caps(pad, nullptr);
        const bool linked = videoCaps && buildVideoPassthroughBranch(context, pad, videoCaps);
        if (videoCaps) gst_caps_unref(videoCaps);
        if (!linked) drainPad(context->bin, pad);
        return;
    }

    if (context->config.transcodeAudioCodec == "copy" &&
        capsText.find("audio/") != std::string::npos) {
        GstCaps* audioCaps = gst_pad_get_current_caps(pad);
        if (!audioCaps) audioCaps = gst_pad_query_caps(pad, nullptr);
        const bool linked = audioCaps && buildAudioPassthroughBranch(context, pad, audioCaps);
        if (audioCaps) gst_caps_unref(audioCaps);
        if (!linked) drainPad(context->bin, pad);
        return;
    }

    GstElement* queue = gst_element_factory_make("queue", nullptr);
    GstElement* decode = gst_element_factory_make("decodebin", nullptr);
    if (!queue || !decode || !add(context->bin, queue) || !add(context->bin, decode) ||
        !gst_element_link(queue, decode)) {
        std::cerr << "Transcoder: failed to create decoder branch" << std::endl;
        drainPad(context->bin, pad);
        return;
    }
    GstPad* sinkPad = gst_element_get_static_pad(queue, "sink");
    const bool linked = sinkPad && gst_pad_link(pad, sinkPad) == GST_PAD_LINK_OK;
    if (sinkPad) gst_object_unref(sinkPad);
    if (!linked) {
        drainPad(context->bin, pad);
        return;
    }
    g_signal_connect(decode, "pad-added", G_CALLBACK(onDecodedPadAdded), context);
    sync(queue);
    sync(decode);
}

} // namespace

std::string TranscoderModule::workingIntelVideoEncoderFactory() {
    static std::once_flag probeOnce;
    static std::string selected;

    std::call_once(probeOnce, []() {
        // Preserve the preferred order on modern Intel systems, but only keep
        // an encoder after it has encoded real NV12 frames in a child process.
        for (const char* name : {"qsvh264enc", "vah264enc", "vaapih264enc"}) {
            if (!factoryAvailable(name)) continue;
            const EncoderProbeResult probe = probeVideoEncoderFactory(name);
            std::cerr << "Intel encoder probe 203.08: factory=" << name
                      << " result=" << (probe.ok ? "ok" : "failed");
            if (probe.timedOut) std::cerr << " reason=timeout";
            else if (probe.signal != 0) std::cerr << " signal=" << probe.signal;
            else if (probe.exitCode >= 0) std::cerr << " exit=" << probe.exitCode;
            std::cerr << std::endl;
            if (probe.ok) {
                selected = name;
                break;
            }
        }

        std::cerr << "Intel encoder selection 203.08: selected="
                  << (selected.empty() ? "none" : selected)
                  << " policy=runtime-probe qsv->va->legacy-vaapi" << std::endl;
    });

    return selected;
}

TranscoderCapabilities TranscoderModule::inspectCapabilities() {
    TranscoderCapabilities result;
    std::string gstLaunchPath;
    if (!executableInPath("gst-launch-1.0", &gstLaunchPath)) {
        result.missingElements.emplace_back("gst-launch-1.0");
    }
    const char* required[] = {
        "uridecodebin", "decodebin", "queue",
        "videoconvert", "deinterlace", "videoscale", "videorate", "capsfilter",
        "h264parse",
        "audioconvert", "audioresample",
        "aacparse", "mpegtsmux", "udpsink", nullptr
    };

    for (const char** name = required; *name; ++name) {
        GstElementFactory* factory = gst_element_factory_find(*name);
        if (!factory) result.missingElements.emplace_back(*name);
        else gst_object_unref(factory);
    }

    result.x264Available = factoryAvailable("x264enc");
    result.nvencAvailable = factoryAvailable("nvh264enc");
    result.intelEncoder = intelVideoEncoderFactory();
    result.intelAvailable = !result.intelEncoder.empty();
    if (result.nvencAvailable) result.videoEncoder = "nvh264enc";
    else if (result.intelAvailable) result.videoEncoder = result.intelEncoder;
    else if (result.x264Available) result.videoEncoder = "x264enc";
    else result.missingElements.emplace_back("H.264 encoder: nvh264enc, qsvh264enc/vah264enc/vaapih264enc or x264enc");
    GstElementFactory* aacParser = gst_element_factory_find("aacparse");
    if (aacParser) {
        gst_object_unref(aacParser);
        for (const char* name : {"voaacenc", "fdkaacenc", "avenc_aac"}) {
            GstElementFactory* factory = gst_element_factory_find(name);
            if (factory) {
                result.aacEncoder = name;
                gst_object_unref(factory);
                break;
            }
        }
    }
    GstElementFactory* mp3Parser = gst_element_factory_find("mpegaudioparse");
    if (mp3Parser) {
        gst_object_unref(mp3Parser);
        for (const char* name : {"lamemp3enc", "avenc_mp3"}) {
            GstElementFactory* factory = gst_element_factory_find(name);
            if (factory) {
                result.mp3Encoder = name;
                gst_object_unref(factory);
                break;
            }
        }
    }
    result.audioEncoder = !result.aacEncoder.empty() ? result.aacEncoder : result.mp3Encoder;
    if (GstElementFactory* factory = gst_element_factory_find("deinterlace")) {
        result.deinterlaceAvailable = true;
        gst_object_unref(factory);
    }
    result.available = result.missingElements.empty();
    result.message = result.available
        ? "GStreamer transcoding is available: preferred video encoder " + result.videoEncoder +
              ", gst-launch=" + gstLaunchPath
        : "Transcoding is unavailable because required GStreamer elements are missing";
    return result;
}

bool TranscoderModule::resolutionSize(const std::string& value, int& width, int& height) {
    tvs::transcode::VideoGeometry geometry;
    if (!tvs::transcode::videoGeometry(value, geometry)) return false;
    width = geometry.width;
    height = geometry.height;
    return true;
}

uint64_t TranscoderModule::recommendedVideoBitrate(const std::string& value) {
    if (value == "3840x2160") return 25000000;
    if (value == "3200x1800") return 18000000;
    if (value == "2560x1440") return 12000000;
    if (value == "1920x1080") return 6000000;
    if (value == "1280x720") return 3500000;
    if (value == "1024x576") return 2500000;
    if (value == "720x576_16_9") return 2000000;
    if (value == "720x576") return 2000000;
    return 6000000;
}

GstElement* TranscoderModule::createBin(const StreamConfig& config, std::string& error) {
    const std::string videoCodec = config.transcodeVideoCodec == "copy" ? "copy" : "h264";
    int width = 0, height = 0;
    if (videoCodec != "copy" && !resolutionSize(config.transcodeResolution, width, height)) {
        error = "unsupported transcode resolution";
        return nullptr;
    }
    const auto capabilities = inspectCapabilities();
    if (!capabilities.available) {
        error = capabilities.message;
        if (!capabilities.missingElements.empty()) {
            error += ": ";
            for (size_t i = 0; i < capabilities.missingElements.size(); ++i) {
                if (i) error += ", ";
                error += capabilities.missingElements[i];
            }
        }
        return nullptr;
    }
    if (videoCodec != "copy") {
        const std::string requestedEncoder = config.transcodeVideoEncoder;
        if (requestedEncoder == "nvenc" && !capabilities.nvencAvailable) {
            error = "NVIDIA NVENC was requested but GStreamer nvh264enc is not available";
            return nullptr;
        }
        if (requestedEncoder == "intel" && !capabilities.intelAvailable) {
            error = "Intel hardware H.264 was requested but qsvh264enc/vah264enc/vaapih264enc is not available";
            return nullptr;
        }
        if (requestedEncoder == "x264" && !capabilities.x264Available) {
            error = "CPU x264 was requested but GStreamer x264enc is not available";
            return nullptr;
        }
    }

    const std::string audioCodec = config.transcodeAudioCodec == "copy" ? "copy" :
        (config.transcodeAudioCodec == "mp3" ? "mp3" : "aac");
    if ((audioCodec == "aac" && capabilities.aacEncoder.empty()) ||
        (audioCodec == "mp3" && capabilities.mp3Encoder.empty())) {
        error = audioCodec + " encoder is not available";
        return nullptr;
    }

    GstElement* bin = gst_bin_new("transcoder_bin");
    GstElement* inputQueue = gst_element_factory_make("queue", "transcode_input_queue");
    GstElement* parsebin = gst_element_factory_make("parsebin", "transcode_parsebin");
    GstElement* mux = gst_element_factory_make("mpegtsmux", "transcode_mux");
    GstElement* outputParse = gst_element_factory_make("tsparse", "transcode_output_tsparse");
    if (!bin || !inputQueue || !parsebin || !mux || !outputParse ||
        !add(bin, inputQueue) || !add(bin, parsebin) || !add(bin, mux) || !add(bin, outputParse)) {
        error = "failed to create transcoder bin elements";
        if (bin) gst_object_unref(bin);
        return nullptr;
    }

    const guint64 muxBitrate = static_cast<guint64>(
        (videoCodec == "copy" ? std::max<uint64_t>(config.transcodeVideoBitrate, 500000)
                              : config.transcodeVideoBitrate) +
        (audioCodec == "copy" ? 384000 : config.transcodeAudioBitrate) + 350000);
    g_object_set(mux,
        "alignment", 7,
        "bitrate", muxBitrate,
        nullptr);
    std::cerr << "Transcoder 202.78: video=" << videoCodec
              << " video_encoder=" << (videoCodec == "copy" ? "copy" : selectedVideoEncoderFactory(config))
              << " audio=" << audioCodec
              << " cadence=preserve-progressive/double-interlaced-fields"
              << " mux_bitrate=" << muxBitrate << std::endl;
    g_object_set(outputParse, "set-timestamps", TRUE, nullptr);
    if (!gst_element_link(inputQueue, parsebin) || !gst_element_link(mux, outputParse)) {
        error = "failed to link transcoder bin core";
        gst_object_unref(bin);
        return nullptr;
    }

    GstPad* parseSink = gst_element_get_static_pad(inputQueue, "sink");
    GstPad* outputSrc = gst_element_get_static_pad(outputParse, "src");
    GstPad* ghostSink = parseSink ? gst_ghost_pad_new("sink", parseSink) : nullptr;
    GstPad* ghostSrc = outputSrc ? gst_ghost_pad_new("src", outputSrc) : nullptr;
    if (parseSink) gst_object_unref(parseSink);
    if (outputSrc) gst_object_unref(outputSrc);
    if (!ghostSink || !ghostSrc || !gst_element_add_pad(bin, ghostSink) ||
        !gst_element_add_pad(bin, ghostSrc)) {
        if (ghostSink && !GST_OBJECT_PARENT(ghostSink)) gst_object_unref(ghostSink);
        if (ghostSrc && !GST_OBJECT_PARENT(ghostSrc)) gst_object_unref(ghostSrc);
        error = "failed to create transcoder ghost pads";
        gst_object_unref(bin);
        return nullptr;
    }

    auto* context = new TranscodeContext();
    context->bin = bin;
    context->mux = mux;
    context->config = config;
    g_object_set_data_full(G_OBJECT(bin), "tvstreammersat5-transcode-context", context,
        [](gpointer p) { delete static_cast<TranscodeContext*>(p); });
    g_signal_connect(parsebin, "pad-added", G_CALLBACK(onDemuxPadAdded), context);
    return bin;
}
