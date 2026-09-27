#include "GstTranscoderProcess.h"

#include "TranscoderModule.h"
#include "TranscodeVideoGeometry.h"
#include "protocols/GstInputProtocols.h"
#include "protocols/GstOutputProtocols.h"
#include "protocols/GstProtocolTypes.h"
#include "utils.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cctype>
#include <csignal>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <thread>
#include <utility>

#include <dirent.h>
#include <fcntl.h>
#include <gst/gst.h>
#include <sys/wait.h>
#include <unistd.h>

using tvs::protocols::ContainerKind;
using tvs::protocols::GstOutputSpec;

namespace {

// 203.47: udpsrc passes this value to SO_RCVBUF. Linux accounts the
// receive socket buffer at roughly 2x the requested value, so a 16 MiB
// request yields the ~32 MiB rb value already proven stable by the native
// TVStreammerSAT5 UDP ingest path. Keep this explicit so transcoder
// stability does not depend on the host net.core.rmem_default setting.
constexpr int kTranscoderUdpSocketBufferRequestBytes = 16 * 1024 * 1024;
constexpr int kTranscoderUdpLinuxEffectiveBufferBytes =
    2 * kTranscoderUdpSocketBufferRequestBytes;

void markOpenDescriptorsCloseOnExec() {
    DIR* directory = ::opendir("/proc/self/fd");
    if (directory) {
        const int directoryFd = ::dirfd(directory);
        while (dirent* entry = ::readdir(directory)) {
            char* end = nullptr;
            errno = 0;
            const long value = std::strtol(entry->d_name, &end, 10);
            if (errno != 0 || !end || *end != '\0' || value <= STDERR_FILENO || value == directoryFd) {
                continue;
            }
            const int fd = static_cast<int>(value);
            const int flags = ::fcntl(fd, F_GETFD);
            if (flags >= 0) {
                ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
            }
        }
        ::closedir(directory);
        return;
    }

    long maxFd = ::sysconf(_SC_OPEN_MAX);
    if (maxFd <= 0) maxFd = 4096;
    for (int fd = STDERR_FILENO + 1; fd < maxFd; ++fd) {
        const int flags = ::fcntl(fd, F_GETFD);
        if (flags >= 0) {
            ::fcntl(fd, F_SETFD, flags | FD_CLOEXEC);
        }
    }
}

void appendAvailableStderr(int fd, std::string& output) {
    if (fd < 0) return;
    char buffer[1024];
    for (;;) {
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count > 0) {
            output.append(buffer, static_cast<size_t>(count));
            if (output.size() > 8192) output.erase(0, output.size() - 8192);
            continue;
        }
        if (count < 0 && errno == EINTR) continue;
        break;
    }
}

void relayChildStderr(int fd) {
    if (fd < 0) return;
    const int flags = ::fcntl(fd, F_GETFL);
    if (flags >= 0) ::fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    char buffer[1024];
    for (;;) {
        const ssize_t count = ::read(fd, buffer, sizeof(buffer));
        if (count > 0) {
            std::cerr.write(buffer, count);
            std::cerr.flush();
            continue;
        }
        if (count < 0 && errno == EINTR) continue;
        break;
    }
    ::close(fd);
}

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

bool hasFactory(const char* name) {
    GstElementFactory* factory = gst_element_factory_find(name);
    if (!factory) return false;
    gst_object_unref(factory);
    return true;
}

bool validateFactories(const std::vector<std::string>& names, std::vector<std::string>& missing) {
    bool ok = true;
    for (const auto& name : names) {
        if (!hasFactory(name.c_str())) {
            missing.push_back(name);
            ok = false;
        }
    }
    return ok;
}

std::string findAacEncoder() {
    for (const char* name : {"voaacenc", "fdkaacenc", "avenc_aac"}) {
        if (hasFactory(name)) return name;
    }
    return {};
}

std::string findMp3Encoder() {
    for (const char* name : {"lamemp3enc", "avenc_mp3"}) {
        if (hasFactory(name)) return name;
    }
    return {};
}

void addQueue(std::vector<std::string>& args, const std::string& name,
              uint64_t maxTimeNs = 5000000000ULL, bool leaky = false) {
    args.insert(args.end(), {
        "queue",
        "name=" + name,
        "max-size-buffers=0",
        "max-size-bytes=0",
        "max-size-time=" + std::to_string(maxTimeNs)
    });
    if (leaky) args.push_back("leaky=downstream");
}

std::string property(const std::string& name, const std::string& value) {
    return name + "=" + value;
}

std::string shellQuote(const std::string& value) {
    if (value.empty()) return "''";
    bool safe = true;
    for (unsigned char ch : value) {
        if (!(std::isalnum(ch) || ch == '_' || ch == '-' || ch == '.' || ch == '/' ||
              ch == ':' || ch == '=' || ch == ',' || ch == '+' || ch == '?' ||
              ch == '&' || ch == '@' || ch == '%' || ch == ';')) {
            safe = false;
            break;
        }
    }
    if (safe) return value;
    std::string quoted = "'";
    for (char ch : value) {
        if (ch == '\'') quoted += "'\\''";
        else quoted += ch;
    }
    quoted += "'";
    return quoted;
}

std::string commandLineForLog(const std::vector<std::string>& args) {
    std::ostringstream ss;
    for (size_t i = 0; i < args.size(); ++i) {
        if (i > 0) ss << ' ';
        ss << shellQuote(args[i]);
    }
    return ss.str();
}

std::string intelVideoEncoderFactory() {
    return TranscoderModule::workingIntelVideoEncoderFactory();
}

std::string intelHevcVideoEncoderFactory() {
    return TranscoderModule::workingIntelHevcEncoderFactory();
}

bool isNvidiaVideoEncoder(const std::string& factory) {
    return factory == "nvh264enc" || factory == "nvh265enc";
}

bool isIntelVideoEncoder(const std::string& factory) {
    return factory == "qsvh264enc" || factory == "vah264enc" || factory == "vaapih264enc" ||
           factory == "qsvh265enc" || factory == "vah265enc" || factory == "vaapih265enc";
}

bool factoryLongNameContains(const char* factoryName, const std::string& needle) {
    GstElementFactory* factory = gst_element_factory_find(factoryName);
    if (!factory) return false;
    const gchar* longName = gst_element_factory_get_metadata(factory, GST_ELEMENT_METADATA_LONGNAME);
    const bool matched = longName && toLower(longName).find(toLower(needle)) != std::string::npos;
    gst_object_unref(factory);
    return matched;
}

bool forceSoftwareH264DecodeForPlatform() {
    // 203.10: Ivy Bridge VA H.264 decode can stop producing raw frames after
    // damaged/discontinuous live MPEG-TS. Field A/B testing showed avdec_h264
    // keeps recovering while Intel VA H.264 encoding remains stable.
    return factoryLongNameContains("vah264enc", "ivybridge") ||
           factoryLongNameContains("vah264dec", "ivybridge");
}

std::string softwareH264FeatureRankOverride() {
    const char* existing = std::getenv("GST_PLUGIN_FEATURE_RANK");
    std::string value = existing ? existing : "";
    if (!value.empty() && value.back() != ',') value += ',';
    value += "vah264dec:NONE,vaapih264dec:NONE,qsvh264dec:NONE";
    return value;
}

std::string selectedVideoEncoderFactory(const StreamConfig& cfg) {
    const bool hevc = toLower(cfg.transcodeVideoCodec) == "hevc";
    const std::string requested = toLower(cfg.transcodeVideoEncoder);
    if (requested == "nvenc") {
        const char* name = hevc ? "nvh265enc" : "nvh264enc";
        return hasFactory(name) ? name : std::string();
    }
    if (requested == "intel") {
        return hevc ? intelHevcVideoEncoderFactory() : intelVideoEncoderFactory();
    }
    if (requested == "x264") {
        const char* name = hevc ? "x265enc" : "x264enc";
        return hasFactory(name) ? name : std::string();
    }
    if (hevc) {
        if (hasFactory("nvh265enc")) return "nvh265enc";
        if (const std::string intel = intelHevcVideoEncoderFactory(); !intel.empty()) return intel;
        if (hasFactory("x265enc")) return "x265enc";
    } else {
        if (hasFactory("nvh264enc")) return "nvh264enc";
        if (const std::string intel = intelVideoEncoderFactory(); !intel.empty()) return intel;
        if (hasFactory("x264enc")) return "x264enc";
    }
    return {};
}
std::string scaledVideoCaps(const tvs::transcode::VideoGeometry& geometry,
                            const std::string& encoderFactory) {
    const char* format = (isNvidiaVideoEncoder(encoderFactory) || isIntelVideoEncoder(encoderFactory)) ? "NV12" : "I420";
    return "video/x-raw,format=" + std::string(format) + ",width=" + std::to_string(geometry.width) +
           ",height=" + std::to_string(geometry.height) +
           ",pixel-aspect-ratio=(fraction)" + std::to_string(geometry.pixelAspectNum) +
           "/" + std::to_string(geometry.pixelAspectDen) +
           ",interlace-mode=progressive";
}

bool appendVideoEncoder(std::vector<std::string>& args, const StreamConfig& cfg,
                        bool flv, int keyInt, std::string& error) {
    const bool hevc = toLower(cfg.transcodeVideoCodec) == "hevc";
    if (hevc && flv) {
        error = "HEVC is not supported by the RTMP/YouTube FLV output; use H.264";
        return false;
    }

    const std::string encoderFactory = selectedVideoEncoderFactory(cfg);
    if (encoderFactory.empty()) {
        error = hevc
            ? "no HEVC video encoder is available (need nvh265enc, Intel qsv/va H.265 or x265enc)"
            : "no H.264 video encoder is available (need nvh264enc, Intel qsv/va H.264 or x264enc)";
        return false;
    }

    const uint64_t bitrateKbps = tvs::protocols::safeVideoBitrate(cfg) / 1000;
    args.insert(args.end(), {"!", encoderFactory});

    if (encoderFactory == "nvh264enc" || encoderFactory == "nvh265enc") {
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            property("gop-size", std::to_string(keyInt)),
            "bframes=0",
            "rc-mode=cbr",
            "zerolatency=true",
            "aud=true",
            "repeat-sequence-header=true",
            "strict-gop=true",
            property("vbv-buffer-size", std::to_string(std::max<uint64_t>(bitrateKbps, 500)))
        });
    } else if (encoderFactory == "qsvh264enc" || encoderFactory == "qsvh265enc") {
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            property("gop-size", std::to_string(keyInt)),
            "b-frames=0",
            "rate-control=cbr",
            "idr-interval=0"
        });
    } else if (encoderFactory == "vah264enc" || encoderFactory == "vah265enc") {
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            property("key-int-max", std::to_string(keyInt)),
            "b-frames=0",
            "rate-control=cbr"
        });
    } else if (encoderFactory == "vaapih264enc" || encoderFactory == "vaapih265enc") {
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            "rate-control=cbr"
        });
    } else if (encoderFactory == "x265enc") {
        args.insert(args.end(), {
            "tune=zerolatency",
            "speed-preset=superfast",
            property("bitrate", std::to_string(bitrateKbps)),
            property("key-int-max", std::to_string(keyInt))
        });
    } else {
        args.insert(args.end(), {
            "tune=zerolatency",
            "speed-preset=superfast",
            property("bitrate", std::to_string(bitrateKbps)),
            property("key-int-max", std::to_string(keyInt)),
            "bframes=0",
            property("byte-stream", flv ? "false" : "true"),
            "aud=true",
            "insert-vui=true",
            "sliced-threads=true",
            "vbv-buf-capacity=1000",
            "option-string=nal-hrd=cbr:force-cfr=1:repeat-headers=1:scenecut=0"
        });
    }
    return true;
}
bool validateOutputAvailability(const StreamConfig& outputConfig, std::string& error) {
    std::vector<std::string> missing;
    validateFactories(tvs::protocols::requiredElementsForOutput(tvs::protocols::outputKind(outputConfig)), missing);
    if (!missing.empty()) {
        std::ostringstream ss;
        ss << "missing output protocol elements for " << tvs::protocols::normalizedOutputType(outputConfig);
        for (size_t i = 0; i < missing.size(); ++i) {
            ss << (i == 0 ? ": " : ", ") << missing[i];
        }
        error = ss.str();
        return false;
    }
    return true;
}


uint32_t effectiveInputServiceId(const StreamConfig& cfg) {
    // input_service_id=0 means AUTO. In AUTO mode the decoder sees the live
    // source directly and performs its normal program selection. Never fall
    // back to output service_id: that value is reserved for output remapping.
    return cfg.inputServiceId;
}

bool isSidAwareMpegTsInput(const StreamConfig& cfg) {
    const uint32_t sid = effectiveInputServiceId(cfg);
    if (sid == 0 || cfg.testPattern) return false;
    const std::string uri = toLower(tvs::protocols::inputUriForGstreamer(cfg));
    // Live IPTV/SRT transport streams are the paths where automatic URI
    // decoding can silently pick program 1 instead of the configured SID.
    // Keep non-TS containers/adaptive inputs on uridecodebin.
    return uri.rfind("srt://", 0) == 0 ||
           uri.rfind("udp://", 0) == 0;
}

bool appendTranscoderDecodeInput(
    std::vector<std::string>& args,
    const StreamConfig& cfg,
    std::string& error) {
    const std::string uri = tvs::protocols::inputUriForGstreamer(cfg);
    const bool udpInput = toLower(uri).rfind("udp://", 0) == 0;

    // 203.47: uridecodebin creates udpsrc internally, which leaves the source
    // socket on the system default SO_RCVBUF. On the production multicast
    // ingest that produced an 8 MiB rb socket and observable per-socket drops.
    // Use an explicit udpsrc only for UDP so we can request the same receive
    // capacity as the native TVStreammerSAT5 UDP path. All non-UDP protocols
    // retain the proven uridecodebin path unchanged.
    if (udpInput && !isSidAwareMpegTsInput(cfg)) {
        std::vector<std::string> missing;
        validateFactories({"udpsrc", "decodebin"}, missing);
        if (!missing.empty()) {
            std::ostringstream ss;
            ss << "missing UDP transcoder input elements";
            for (size_t i = 0; i < missing.size(); ++i) {
                ss << (i == 0 ? ": " : ", ") << missing[i];
            }
            error = ss.str();
            return false;
        }

        args.insert(args.end(), {
            "udpsrc",
            "name=transcode_udp_src",
            "uri=" + uri,
            "buffer-size=" + std::to_string(kTranscoderUdpSocketBufferRequestBytes),
            "!", "decodebin", "name=dec"
        });

        std::cerr << "GStreamer transcoder UDP input 203.47: uri=" << uri
                  << " socket_buffer_request=" << kTranscoderUdpSocketBufferRequestBytes
                  << " expected_linux_rb=" << kTranscoderUdpLinuxEffectiveBufferBytes
                  << " decode=decodebin"
                  << std::endl;
        return true;
    }

    if (!isSidAwareMpegTsInput(cfg)) {
        tvs::protocols::appendDecodeInput(args, cfg);
        return true;
    }

    std::vector<std::string> missing;
    if (udpInput) {
        validateFactories({"udpsrc", "tsparse", "tsdemux", "decodebin3"}, missing);
    } else {
        validateFactories({"urisourcebin", "tsparse", "tsdemux", "decodebin3"}, missing);
    }
    if (!missing.empty()) {
        std::ostringstream ss;
        ss << "missing SID-aware transcoder input elements";
        for (size_t i = 0; i < missing.size(); ++i) {
            ss << (i == 0 ? ": " : ", ") << missing[i];
        }
        error = ss.str();
        return false;
    }

    const uint32_t inputSid = effectiveInputServiceId(cfg);

    // Select the requested MPEG-TS service *before* decodebin.  The old
    // uridecodebin-only path auto-selected the first/default program, which is
    // why transcoding worked for SID 1 but produced no usable UDP output when
    // Input SID was another program.  ':' asks gst-launch to link all compatible
    // elementary pads from the selected tsdemux program into decodebin3.
    if (udpInput) {
        args.insert(args.end(), {
            "udpsrc",
            "name=transcode_udp_src",
            "uri=" + uri,
            "buffer-size=" + std::to_string(kTranscoderUdpSocketBufferRequestBytes),
            "!",
            "queue",
            "name=transcode_sid_input_queue",
            "max-size-buffers=0",
            "max-size-bytes=0",
            "max-size-time=8000000000",
            "!", "tsparse",
            "!", "tsdemux",
            "name=transcode_sid_demux",
            "program-number=" + std::to_string(inputSid),
            "latency=700",
            "transcode_sid_demux.", ":", "decodebin3", "name=dec"
        });

        std::cerr << "GStreamer transcoder UDP input 203.47: uri=" << uri
                  << " socket_buffer_request=" << kTranscoderUdpSocketBufferRequestBytes
                  << " expected_linux_rb=" << kTranscoderUdpLinuxEffectiveBufferBytes
                  << " input_sid=" << inputSid
                  << " decode=tsdemux+decodebin3"
                  << std::endl;
    } else {
        args.insert(args.end(), {
            "urisourcebin",
            "name=input_uri_src",
            "uri=" + uri,
            "use-buffering=false",
            "input_uri_src.", "!",
            "queue",
            "name=transcode_sid_input_queue",
            "max-size-buffers=0",
            "max-size-bytes=0",
            "max-size-time=8000000000",
            "!", "tsparse",
            "!", "tsdemux",
            "name=transcode_sid_demux",
            "program-number=" + std::to_string(inputSid),
            "latency=700",
            "transcode_sid_demux.", ":", "decodebin3", "name=dec"
        });
    }

    std::cerr << "GStreamer transcoder input selector: input_sid=" << inputSid
              << " method=tsdemux-program-number decode=decodebin3"
              << " uri=" << uri << std::endl;
    return true;
}

struct SharedOutputBranch {
    GstOutputSpec spec;
    std::size_t index = 0;
    bool temporaryPreview = false;
};

void uniquifyOutputFragment(
    std::vector<std::string>& fragment,
    GstOutputSpec& spec,
    std::size_t outputIndex) {
    const std::string suffix = "_out" + std::to_string(outputIndex);
    std::vector<std::pair<std::string, std::string>> renamed;

    for (auto& token : fragment) {
        if (token.rfind("name=", 0) != 0 || token.size() <= 5) continue;
        const std::string oldName = token.substr(5);
        const std::string newName = oldName + suffix;
        renamed.emplace_back(oldName, newName);
        token = "name=" + newName;
    }

    auto rewriteReference = [&renamed](std::string& value) {
        for (const auto& [oldName, newName] : renamed) {
            const std::string prefix = oldName + ".";
            if (value.rfind(prefix, 0) == 0) {
                value = newName + value.substr(oldName.size());
                return;
            }
        }
    };

    for (auto& token : fragment) {
        if (token.rfind("name=", 0) == 0) continue;
        rewriteReference(token);
    }
    rewriteReference(spec.videoPad);
    rewriteReference(spec.audioPad);
}

bool appendSharedVideoEncoderCore(
    std::vector<std::string>& args,
    const StreamConfig& cfg,
    std::string& error) {
    tvs::transcode::VideoGeometry geometry;
    if (!tvs::transcode::videoGeometry(cfg.transcodeResolution, geometry)) {
        error = "unsupported transcode resolution";
        return false;
    }
    const int width = geometry.width;
    const int height = geometry.height;

    const std::string encoderFactory = selectedVideoEncoderFactory(cfg);
    if (encoderFactory.empty()) {
        if (cfg.transcodeVideoEncoder == "nvenc") error = "NVIDIA NVENC nvh264enc is not available";
        else if (cfg.transcodeVideoEncoder == "intel") error = "Intel qsv/VA H.264 encoder did not pass the runtime probe";
        else if (cfg.transcodeVideoEncoder == "x264") error = "CPU x264enc is not available";
        else error = "no H.264 video encoder is available";
        return false;
    }

    if (cfg.testPattern) {
        args.insert(args.end(), {
            "videotestsrc", "is-live=true", "pattern=smpte",
            "!", "video/x-raw,framerate=25/1", "!"
        });
        addQueue(args, "transcode_video_queue", 3000000000ULL);
        args.insert(args.end(), {
            "!", "videoconvert",
            "!", "videoscale", "add-borders=false", "method=lanczos",
            "!", "videorate"
        });
    } else {
        args.insert(args.end(), {"dec.", "!"});
        addQueue(args, "transcode_video_queue", 8000000000ULL);
        args.insert(args.end(), {
            "!", "watchdog",
            "name=transcode_decoded_video_watchdog",
            "timeout=15000",
            "!", "video/x-raw",
            "!", "videoconvert",
            "!", "deinterlace", "method=yadif", "mode=auto-strict", "fields=top", "locking=passive",
            "!", "videorate",
            "!", "video/x-raw,framerate=25/1",
            "!", "videoscale", "add-borders=false", "method=lanczos"
        });
    }

    if (isNvidiaVideoEncoder(encoderFactory) || isIntelVideoEncoder(encoderFactory)) {
        args.insert(args.end(), {"!", "videoconvert"});
    }
    args.insert(args.end(), {"!", scaledVideoCaps(geometry, encoderFactory)});

    // 203.75: encode the selected H.264/HEVC profile once for all configured
    // outputs in this child. Per-output parsers normalize elementary-stream caps.
    if (!appendVideoEncoder(args, cfg, false, 25, error)) return false;
    args.insert(args.end(), {"!", "tee", "name=transcode_video_encoded_tee"});

    std::cerr << "GStreamer shared transcoder video 203.45: requested="
              << cfg.transcodeVideoEncoder
              << " selected=" << encoderFactory
              << " output=" << width << "x" << height
              << " pixel_aspect_ratio=" << geometry.pixelAspectNum << "/" << geometry.pixelAspectDen
              << " bitrate=" << tvs::protocols::safeVideoBitrate(cfg)
              << " encode_instances=1"
              << std::endl;
    return true;
}

bool appendSharedAudioEncoderCore(
    std::vector<std::string>& args,
    const StreamConfig& cfg,
    std::string& error) {
    const std::string audioCodec = toLower(cfg.transcodeAudioCodec);
    const uint64_t bitrate = tvs::protocols::safeAudioBitrate(cfg);
    std::string selectedAacEncoder;
    std::string selectedMp3Encoder;
    if (audioCodec == "mp3") selectedMp3Encoder = findMp3Encoder();
    else selectedAacEncoder = findAacEncoder();

    if (audioCodec == "mp3" && selectedMp3Encoder.empty()) {
        error = "MP3 encoder is not available";
        return false;
    }
    if (audioCodec != "mp3" && selectedAacEncoder.empty()) {
        error = "AAC encoder is not available";
        return false;
    }

    const std::string rawAudioCaps = selectedAacEncoder == "avenc_aac"
        ? "audio/x-raw,format=F32LE,layout=interleaved,rate=48000,channels=2"
        : "audio/x-raw,format=S16LE,layout=interleaved,rate=48000,channels=2";

    if (cfg.testPattern) {
        args.insert(args.end(), {
            "audiotestsrc", "is-live=true", "wave=sine", "freq=1000",
            "!", "audio/x-raw,rate=48000,channels=2", "!"
        });
        addQueue(args, "transcode_audio_queue", 3000000000ULL);
    } else {
        args.insert(args.end(), {"dec.", "!"});
        addQueue(args, "transcode_audio_queue", 8000000000ULL);
        args.insert(args.end(), {"!", "audio/x-raw"});
    }

    args.insert(args.end(), {
        "!", "audioconvert",
        "!", "audioresample", "quality=6",
        "!", "audiorate", "skip-to-first=true", "tolerance=20000000",
        "!", rawAudioCaps,
        "!"
    });

    if (audioCodec == "mp3") {
        if (selectedMp3Encoder == "lamemp3enc") {
            args.insert(args.end(), {
                "lamemp3enc",
                "target=bitrate",
                "cbr=true",
                property("bitrate", std::to_string(std::max<uint64_t>(bitrate / 1000, 64)))
            });
        } else {
            args.insert(args.end(), {
                "avenc_mp3",
                property("bitrate", std::to_string(bitrate))
            });
        }
    } else {
        args.insert(args.end(), {
            selectedAacEncoder,
            property("bitrate", std::to_string(bitrate))
        });
    }

    args.insert(args.end(), {"!", "tee", "name=transcode_audio_encoded_tee"});
    std::cerr << "GStreamer shared transcoder audio 203.45: codec=" << audioCodec
              << " bitrate=" << bitrate
              << " encode_instances=1"
              << std::endl;
    return true;
}

void appendSharedEncodedOutputBranches(
    std::vector<std::string>& args,
    const StreamConfig& cfg,
    const std::vector<SharedOutputBranch>& outputs) {
    const std::string audioCodec = toLower(cfg.transcodeAudioCodec);
    const bool hevc = toLower(cfg.transcodeVideoCodec) == "hevc";

    for (const auto& output : outputs) {
        const std::string suffix = "_out" + std::to_string(output.index);
        const bool flv = output.spec.container == ContainerKind::Flv;
        const bool rtsp = output.spec.container == ContainerKind::Rtsp;

        args.insert(args.end(), {"transcode_video_encoded_tee.", "!"});
        addQueue(args, "transcode_video_mux_queue" + suffix,
                 output.temporaryPreview ? 1000000000ULL : 3000000000ULL,
                 output.temporaryPreview);
        if (hevc) {
            args.insert(args.end(), {
                "!", "h265parse", property("config-interval", "-1"),
                "!", "video/x-h265,stream-format=byte-stream,alignment=au",
                "!", output.spec.videoPad
            });
        } else {
            args.insert(args.end(), {
                "!", "h264parse", property("config-interval", "-1"),
                "!", flv
                    ? "video/x-h264,stream-format=avc,alignment=au"
                    : "video/x-h264,stream-format=byte-stream,alignment=au",
                "!", output.spec.videoPad
            });
        }

        args.insert(args.end(), {"transcode_audio_encoded_tee.", "!"});
        addQueue(args, "transcode_audio_mux_queue" + suffix,
                 output.temporaryPreview ? 1000000000ULL : 3000000000ULL,
                 output.temporaryPreview);
        if (audioCodec == "mp3") {
            args.insert(args.end(), {
                "!", "mpegaudioparse",
                "!", "audio/mpeg,mpegversion=1,layer=3",
                "!", output.spec.audioPad
            });
        } else {
            args.insert(args.end(), {
                "!", "aacparse",
                "!", (flv || rtsp)
                    ? "audio/mpeg,mpegversion=4,stream-format=raw"
                    : "audio/mpeg,mpegversion=4,stream-format=adts",
                "!", output.spec.audioPad
            });
        }
    }
}

struct AbrRendition {
    std::string name;
    std::string resolution;
    uint64_t bitrate = 0;
};

std::vector<AbrRendition> abrRenditions(const StreamConfig& cfg) {
    int baseWidth = 0, baseHeight = 0;
    if (!TranscoderModule::resolutionSize(cfg.transcodeResolution, baseWidth, baseHeight)) return {};
    const uint64_t basePixels = static_cast<uint64_t>(baseWidth) * static_cast<uint64_t>(baseHeight);
    const uint64_t baseBitrate = tvs::protocols::safeVideoBitrate(cfg);

    const std::vector<AbrRendition> ladder = {
        {"1080p", "1920x1080", 6000000},
        {"720p",  "1280x720",  3500000},
        {"576p",  "1024x576",  2200000},
        {"576i",  "720x576_16_9", 1600000}
    };

    std::vector<AbrRendition> result;
    for (auto rendition : ladder) {
        int width = 0, height = 0;
        if (!TranscoderModule::resolutionSize(rendition.resolution, width, height)) continue;
        const uint64_t pixels = static_cast<uint64_t>(width) * static_cast<uint64_t>(height);
        if (pixels >= basePixels || rendition.resolution == cfg.transcodeResolution) continue;
        rendition.bitrate = std::min<uint64_t>(rendition.bitrate, baseBitrate * 3 / 4);
        if (rendition.bitrate < 500000 || rendition.bitrate >= baseBitrate) continue;
        result.push_back(std::move(rendition));
        if (result.size() >= 3) break;
    }
    return result;
}

std::filesystem::path abrBaseDirectory(const StreamConfig& cfg) {
    return cfg.hlsArchiveEnabled
        ? std::filesystem::path(cfg.hlsArchivePath) / cfg.id
        : std::filesystem::path("/tmp/tvstreammersat5-hls") / cfg.id;
}

bool writeAbrMasterPlaylist(const StreamConfig& cfg,
                            const std::vector<AbrRendition>& renditions,
                            std::string& error) {
    std::error_code ec;
    const auto dir = abrBaseDirectory(cfg);
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        error = "failed to create HLS ABR directory: " + ec.message();
        return false;
    }
    std::ofstream out(dir / "master.m3u8", std::ios::trunc);
    if (!out.is_open()) {
        error = "failed to create HLS ABR master playlist";
        return false;
    }

    out << "#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-INDEPENDENT-SEGMENTS\n";
    int width = 0, height = 0;
    if (!TranscoderModule::resolutionSize(cfg.transcodeResolution, width, height)) {
        error = "invalid primary ABR resolution";
        return false;
    }
    const uint64_t audio = tvs::protocols::safeAudioBitrate(cfg);
    out << "#EXT-X-STREAM-INF:BANDWIDTH="
        << (tvs::protocols::safeVideoBitrate(cfg) + audio + 350000)
        << ",RESOLUTION=" << width << "x" << height << "\n";
    out << "video.m3u8\n";

    for (const auto& rendition : renditions) {
        if (!TranscoderModule::resolutionSize(rendition.resolution, width, height)) continue;
        out << "#EXT-X-STREAM-INF:BANDWIDTH="
            << (rendition.bitrate + audio + 350000)
            << ",RESOLUTION=" << width << "x" << height << "\n";
        out << "abr/" << rendition.name << "/video.m3u8\n";
    }
    out.flush();
    if (!out.good()) {
        error = "failed to write HLS ABR master playlist";
        return false;
    }
    return true;
}

} // namespace

GstTranscoderProcess::~GstTranscoderProcess() {
    stop();
}

bool GstTranscoderProcess::isAvailable(std::string* error) {
    std::string gstLaunchPath;
    if (!executableInPath("gst-launch-1.0", &gstLaunchPath)) {
        if (error) *error = "gst-launch-1.0 executable was not found in PATH";
        return false;
    }

    std::vector<std::string> required = tvs::protocols::requiredInputElements();
    const std::vector<std::string> common = {
        "queue", "tee", "watchdog", "videoconvert", "deinterlace", "videoscale", "videorate",
        "h264parse", "audioconvert", "audioresample", "audiorate", "aacparse"
    };
    required.insert(required.end(), common.begin(), common.end());

    std::vector<std::string> missing;
    validateFactories(required, missing);
    if (!hasFactory("nvh264enc") && intelVideoEncoderFactory().empty() && !hasFactory("x264enc")) {
        missing.emplace_back("H.264 encoder: nvh264enc, Intel qsv/va or x264enc");
    }
    if (findAacEncoder().empty()) {
        missing.emplace_back("AAC encoder: fdkaacenc, voaacenc or avenc_aac");
    }
    if (!missing.empty()) {
        std::ostringstream ss;
        ss << "missing GStreamer transcoder elements";
        for (size_t i = 0; i < missing.size(); ++i) {
            ss << (i == 0 ? ": " : ", ") << missing[i];
        }
        if (error) *error = ss.str();
        return false;
    }
    if (error) *error = "GStreamer transcoder is available: " + gstLaunchPath;
    return true;
}

bool GstTranscoderProcess::spawnProcess(
    const std::vector<std::string>& args,
    const std::string& description,
    ChildProcess& child,
    std::string& error) {
    if (args.empty()) {
        error = "empty gst-launch command";
        return false;
    }

    const bool forceSoftwareH264Decode = forceSoftwareH264DecodeForPlatform();
    if (forceSoftwareH264Decode && !hasFactory("avdec_h264")) {
        error = "Intel Ivy Bridge transcoder recovery requires GStreamer avdec_h264";
        return false;
    }
    if (forceSoftwareH264Decode) {
        std::cerr << "Transcoder H264 decode 203.10: platform=Intel-IvyBridge"
                  << " policy=software-recovery decoder=avdec_h264"
                  << " hardware_encode=preserved input_transport=original-uri"
                  << std::endl;
    }

    // gst-launch does not need any TVStreammerSAT5 sockets. Mark every currently open
    // non-standard descriptor close-on-exec before forking so HTTP/metrics/listener
    // sockets cannot remain alive in the external transcoder process.
    markOpenDescriptorsCloseOnExec();

    int stderrPipe[2] = {-1, -1};
    const bool captureStderr = ::pipe(stderrPipe) == 0;
    if (captureStderr) {
        for (int fd : stderrPipe) {
            const int fdFlags = ::fcntl(fd, F_GETFD);
            if (fdFlags >= 0) ::fcntl(fd, F_SETFD, fdFlags | FD_CLOEXEC);
        }
        const int flags = ::fcntl(stderrPipe[0], F_GETFL);
        if (flags >= 0) ::fcntl(stderrPipe[0], F_SETFL, flags | O_NONBLOCK);
    }

    pid_t pid = ::fork();
    if (pid < 0) {
        if (captureStderr) {
            ::close(stderrPipe[0]);
            ::close(stderrPipe[1]);
        }
        error = std::string("fork failed: ") + std::strerror(errno);
        return false;
    }

    if (pid == 0) {
        if (forceSoftwareH264Decode) {
            const std::string rankOverride = softwareH264FeatureRankOverride();
            if (::setenv("GST_PLUGIN_FEATURE_RANK", rankOverride.c_str(), 1) != 0) {
                std::_Exit(126);
            }
        }
        if (captureStderr) {
            ::close(stderrPipe[0]);
            if (::dup2(stderrPipe[1], STDERR_FILENO) < 0) std::_Exit(126);
            if (stderrPipe[1] != STDERR_FILENO) ::close(stderrPipe[1]);
        }

        int devNull = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
        if (devNull >= 0) {
            ::dup2(devNull, STDIN_FILENO);
            if (devNull > STDERR_FILENO) ::close(devNull);
        }

        std::vector<std::string> storage = args;
        std::vector<char*> argv;
        argv.reserve(storage.size() + 1);
        for (auto& arg : storage) argv.push_back(arg.data());
        argv.push_back(nullptr);
        ::execvp(argv[0], argv.data());
        std::cerr << "GStreamer transcoder exec failed: " << std::strerror(errno) << std::endl;
        std::_Exit(127);
    }

    if (captureStderr) ::close(stderrPipe[1]);
    child.pid = pid;
    child.description = description;

    // Capture early gst-launch diagnostics.  SRT/relay setup can fail slightly
    // after process creation, so give SRT outputs a longer observation window.
    const int attempts = description.find("srt-") != std::string::npos ? 24 : 8;
    std::string startupStderr;
    for (int attempt = 0; attempt < attempts; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (captureStderr) appendAvailableStderr(stderrPipe[0], startupStderr);

        int status = 0;
        const pid_t done = ::waitpid(pid, &status, WNOHANG);
        if (done == 0) continue;
        if (done == pid) {
            if (captureStderr) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                appendAvailableStderr(stderrPipe[0], startupStderr);
                ::close(stderrPipe[0]);
            }
            std::ostringstream ss;
            ss << "GStreamer transcoder exited during startup for " << description;
            if (WIFEXITED(status)) {
                ss << " (exit=" << WEXITSTATUS(status) << ")";
            } else if (WIFSIGNALED(status)) {
                ss << " (signal=" << WTERMSIG(status) << ")";
            } else {
                ss << " (status=" << status << ")";
            }
            if (!startupStderr.empty()) ss << "\n" << startupStderr;
            error = ss.str();
            child.pid = -1;
            return false;
        }
        if (done < 0 && errno != EINTR) {
            if (captureStderr) ::close(stderrPipe[0]);
            error = std::string("waitpid failed after gst-launch start: ") + std::strerror(errno);
            child.pid = -1;
            return false;
        }
    }

    if (captureStderr) {
        appendAvailableStderr(stderrPipe[0], startupStderr);
        if (!startupStderr.empty()) {
            std::cerr << startupStderr;
            if (startupStderr.back() != '\n') std::cerr << std::endl;
        }
        try {
            std::thread(relayChildStderr, stderrPipe[0]).detach();
        } catch (const std::exception& ex) {
            std::cerr << "Resource guard: transcoder stderr relay thread creation failed: "
                      << ex.what() << std::endl;
            ::close(stderrPipe[0]);
        }
    }
    return true;
}

std::vector<std::string> GstTranscoderProcess::buildSharedCommand(
    const StreamConfig& baseConfig,
    const std::vector<StreamConfig>& outputConfigs,
    std::string& description,
    std::string& error) {
    std::vector<std::string> args = {"gst-launch-1.0", "-e"};
    std::vector<SharedOutputBranch> outputs;
    outputs.reserve(outputConfigs.size());

    std::ostringstream descriptionStream;
    descriptionStream << "shared-transcoder[";

    for (std::size_t index = 0; index < outputConfigs.size(); ++index) {
        const auto& outputConfig = outputConfigs[index];
        if (!validateOutputAvailability(outputConfig, error)) return {};
        if (toLower(baseConfig.transcodeVideoCodec) == "hevc" &&
            tvs::protocols::isFlvOutput(tvs::protocols::outputKind(outputConfig))) {
            error = "HEVC is not supported for RTMP/YouTube FLV output; select H.264";
            return {};
        }

        std::vector<std::string> outputFragment;
        GstOutputSpec outputSpec;
        if (!tvs::protocols::appendOutputMuxAndSink(
                outputFragment, outputConfig, outputSpec, error)) {
            return {};
        }

        // Each protocol helper was originally designed for its own gst-launch
        // process and therefore uses friendly names such as "mux". 203.45 keeps
        // those proven helper chains intact, but namespaces every named element
        // before combining all outputs into one shared process.
        uniquifyOutputFragment(outputFragment, outputSpec, index);
        args.insert(args.end(), outputFragment.begin(), outputFragment.end());

        if (index > 0) descriptionStream << ",";
        descriptionStream << outputSpec.description;

        if (outputSpec.kind == tvs::protocols::OutputKind::Http ||
            outputSpec.kind == tvs::protocols::OutputKind::Srt) {
            std::cerr << "Transcoded HTTP/SRT post-mux A/V reservoir: output="
                      << tvs::protocols::normalizedOutputType(outputConfig)
                      << " reservoir_ms=1500 queue_max_ms=6000"
                      << " placement=after-per-output-mpegtsmux-and-cbr-pacer"
                      << " remap_preserved="
                      << (outputConfig.remapEnabled ? "yes" : "not-requested")
                      << std::endl;
        }

        SharedOutputBranch branch;
        branch.spec = std::move(outputSpec);
        branch.index = index;
        branch.temporaryPreview = tvs::protocols::normalizedOutputType(outputConfig) == "http" &&
            outputConfig.outputPort == 0 && outputConfig.outputHost == "127.0.0.1";
        outputs.push_back(std::move(branch));
    }
    descriptionStream << "]";

    if (!baseConfig.testPattern) {
        std::cerr << "Transcoder decoded-video watchdog 203.10: timeout_ms=15000"
                  << " source=" << tvs::protocols::inputUriForGstreamer(baseConfig)
                  << " scope=post-decode action=exit-for-parent-failover"
                  << std::endl;
        if (!appendTranscoderDecodeInput(args, baseConfig, error)) return {};
        std::cerr << "GStreamer transcoder 203.75: video=" << toLower(baseConfig.transcodeVideoCodec)
                  << " encoder_request=" << baseConfig.transcodeVideoEncoder
                  << " deinterlace=yadif-top-fields"
                  << " cadence=fixed-25p"
                  << " output=" << baseConfig.transcodeResolution
                  << " architecture=shared-decode-encode+per-output-mux"
                  << " outputs=" << outputs.size()
                  << std::endl;
    }

    if (toLower(baseConfig.transcodeVideoCodec) == "hevc" && !hasFactory("h265parse")) {
        error = "HEVC was requested but GStreamer h265parse is not available";
        return {};
    }
    if (!appendSharedVideoEncoderCore(args, baseConfig, error)) return {};
    if (!appendSharedAudioEncoderCore(args, baseConfig, error)) return {};
    appendSharedEncodedOutputBranches(args, baseConfig, outputs);

    description = descriptionStream.str();
    return args;
}

bool GstTranscoderProcess::start(const StreamConfig& config, std::string& error) {
    stop();
    stopping = false;

    std::string availableMessage;
    if (!isAvailable(&availableMessage)) {
        error = availableMessage;
        return false;
    }

    auto outputs = tvs::protocols::outputConfigs(config);
    const bool hasHls = std::any_of(outputs.begin(), outputs.end(),
        [](const StreamConfig& output) {
            return tvs::protocols::normalizedOutputType(output) == "hls";
        });

    // Private preview continues to share the primary process.
    const bool hasHttp = std::any_of(outputs.begin(), outputs.end(),
        [](const StreamConfig& output) {
            return tvs::protocols::normalizedOutputType(output) == "http";
        });
    if (!hasHttp && hasFactory("tcpserversink") && hasFactory("mpegtsmux") &&
        hasFactory("tsparse")) {
        StreamConfig preview = config;
        preview.outputType = "http";
        preview.outputMode = "listener";
        preview.outputHost = "127.0.0.1";
        preview.outputPort = 0;
        preview.additionalOutputs.clear();
        preview.cbr = false;
        outputs.push_back(std::move(preview));
    }
    if (outputs.empty()) {
        error = "no outputs configured";
        return false;
    }

    std::string description;
    std::vector<std::string> args = buildSharedCommand(config, outputs, description, error);
    if (!error.empty() || args.empty()) {
        if (error.empty()) error = "failed to build shared transcoder command";
        return false;
    }

    std::cerr << "GStreamer transcoder 203.75: codec=" << toLower(config.transcodeVideoCodec)
              << " outputs=" << outputs.size()
              << " multibitrate=" << (config.transcodeMultibitrateEnabled ? "on" : "off")
              << std::endl;
    std::cerr << "GStreamer transcoder command: " << commandLineForLog(args) << std::endl;

    ChildProcess child;
    if (!spawnProcess(args, description, child, error)) return false;

    {
        std::lock_guard<std::mutex> lock(childrenMutex);
        children.clear();
        children.push_back(std::move(child));
    }

    if (config.transcodeMultibitrateEnabled &&
        toLower(config.transcodeVideoCodec) != "copy") {
        if (!hasHls) {
            std::cerr << "HLS ABR 203.75: checkbox enabled but no HLS output exists; "
                         "primary stream remains unchanged" << std::endl;
        } else {
            const auto renditions = abrRenditions(config);
            for (const auto& rendition : renditions) {
                StreamConfig variant = config;
                variant.transcodeMultibitrateEnabled = false;
                variant.hlsVariantName = rendition.name;
                variant.transcodeResolution = rendition.resolution;
                variant.transcodeVideoBitrate = rendition.bitrate;
                variant.outputType = "hls";
                variant.outputMode = "listener";
                variant.additionalOutputs.clear();
                variant.targetBitrate = std::max<uint64_t>(
                    rendition.bitrate + tvs::protocols::safeAudioBitrate(variant) + 1200000,
                    rendition.bitrate + 1500000);

                std::vector<StreamConfig> variantOutputs{variant};
                std::string variantDescription;
                std::string variantError;
                auto variantArgs = buildSharedCommand(
                    variant, variantOutputs, variantDescription, variantError);
                if (!variantError.empty() || variantArgs.empty()) {
                    error = "HLS ABR rendition " + rendition.name + ": " +
                        (variantError.empty() ? "failed to build pipeline" : variantError);
                    stop();
                    return false;
                }

                ChildProcess variantChild;
                if (!spawnProcess(variantArgs, "abr-" + rendition.name, variantChild, variantError)) {
                    error = "HLS ABR rendition " + rendition.name + ": " + variantError;
                    stop();
                    return false;
                }
                {
                    std::lock_guard<std::mutex> lock(childrenMutex);
                    children.push_back(std::move(variantChild));
                }
                std::cerr << "HLS ABR 203.75: rendition=" << rendition.name
                          << " resolution=" << rendition.resolution
                          << " video_bitrate=" << rendition.bitrate
                          << std::endl;
            }

            std::string masterError;
            if (!writeAbrMasterPlaylist(config, renditions, masterError)) {
                error = masterError;
                stop();
                return false;
            }
            std::cerr << "HLS ABR 203.75: master="
                      << (abrBaseDirectory(config) / "master.m3u8")
                      << " renditions=" << (renditions.size() + 1)
                      << std::endl;
        }
    }

    return true;
}
void GstTranscoderProcess::stop() {
    stopping = true;
    std::lock_guard<std::mutex> lock(childrenMutex);
    for (auto& child : children) {
        if (child.pid <= 0) continue;
        int status = 0;
        pid_t done = ::waitpid(child.pid, &status, WNOHANG);
        if (done == 0) {
            ::kill(child.pid, SIGTERM);
            for (int i = 0; i < 40; ++i) {
                done = ::waitpid(child.pid, &status, WNOHANG);
                if (done == child.pid) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            if (done == 0) {
                ::kill(child.pid, SIGKILL);
                ::waitpid(child.pid, &status, 0);
            }
        }
        child.pid = -1;
    }
    children.clear();
}

bool GstTranscoderProcess::isRunning() {
    std::lock_guard<std::mutex> lock(childrenMutex);

    // 203.45 normally owns one shared gst-launch child for every transcoded
    // output. Keep an all-children health rule so any future fallback or split
    // mode still cannot mask an exited child.
    bool allRunning = !children.empty();
    for (auto& child : children) {
        if (child.pid <= 0) {
            allRunning = false;
            continue;
        }

        int status = 0;
        const pid_t pid = child.pid;
        const pid_t done = ::waitpid(pid, &status, WNOHANG);
        if (done == 0) {
            continue;
        }
        if (done == pid) {
            std::cerr << "GStreamer shared transcoder guard 203.45: child-exited pid=" << pid
                      << " outputs=" << child.description
                      << " status=" << status
                      << " action=restart-whole-transcoder" << std::endl;
            child.pid = -1;
            allRunning = false;
        }
    }
    return allRunning;
}

std::vector<pid_t> GstTranscoderProcess::childPids() const {
    std::lock_guard<std::mutex> lock(childrenMutex);
    std::vector<pid_t> result;
    for (const auto& child : children) {
        if (child.pid > 0) {
            result.push_back(child.pid);
        }
    }
    return result;
}

std::string GstTranscoderProcess::description() const {
    std::lock_guard<std::mutex> lock(childrenMutex);
    std::ostringstream ss;
    for (size_t i = 0; i < children.size(); ++i) {
        if (i > 0) ss << ", ";
        ss << children[i].description;
    }
    return ss.str();
}
