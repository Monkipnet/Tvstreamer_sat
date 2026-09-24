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
    // 203.08: factory registration is not proof that the Intel backend works.
    // TranscoderModule probes qsv/VA in an isolated child process and caches
    // the first encoder that actually produces H.264 without aborting.
    return TranscoderModule::workingIntelVideoEncoderFactory();
}

bool isIntelVideoEncoder(const std::string& factory) {
    return factory == "qsvh264enc" || factory == "vah264enc" || factory == "vaapih264enc";
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
    if (cfg.transcodeVideoEncoder == "nvenc") return hasFactory("nvh264enc") ? "nvh264enc" : std::string();
    if (cfg.transcodeVideoEncoder == "intel") return intelVideoEncoderFactory();
    if (cfg.transcodeVideoEncoder == "x264") return hasFactory("x264enc") ? "x264enc" : std::string();
    if (hasFactory("nvh264enc")) return "nvh264enc";
    if (const std::string intel = intelVideoEncoderFactory(); !intel.empty()) return intel;
    if (hasFactory("x264enc")) return "x264enc";
    return {};
}

std::string scaledVideoCaps(const tvs::transcode::VideoGeometry& geometry,
                            const std::string& encoderFactory) {
    const char* format = (encoderFactory == "nvh264enc" || isIntelVideoEncoder(encoderFactory)) ? "NV12" : "I420";
    return "video/x-raw,format=" + std::string(format) + ",width=" + std::to_string(geometry.width) +
           ",height=" + std::to_string(geometry.height) +
           ",pixel-aspect-ratio=(fraction)" + std::to_string(geometry.pixelAspectNum) +
           "/" + std::to_string(geometry.pixelAspectDen) +
           ",interlace-mode=progressive";
}

bool appendVideoEncoder(std::vector<std::string>& args, const StreamConfig& cfg,
                        bool flv, int keyInt, std::string& error) {
    const std::string encoderFactory = selectedVideoEncoderFactory(cfg);
    if (encoderFactory.empty()) {
        if (cfg.transcodeVideoEncoder == "nvenc") {
            error = "NVIDIA NVENC was requested but GStreamer nvh264enc is not available";
        } else if (cfg.transcodeVideoEncoder == "intel") {
            error = "Intel hardware H.264 was requested but no qsv/VA H.264 encoder passed the runtime probe";
        } else if (cfg.transcodeVideoEncoder == "x264") {
            error = "CPU x264 was requested but GStreamer x264enc is not available";
        } else {
            error = "no H.264 video encoder is available (need nvh264enc, Intel qsv/va or x264enc)";
        }
        return false;
    }

    const uint64_t bitrateKbps = tvs::protocols::safeVideoBitrate(cfg) / 1000;
    args.insert(args.end(), {"!", encoderFactory});
    if (encoderFactory == "nvh264enc") {
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
    } else if (encoderFactory == "qsvh264enc") {
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            property("gop-size", std::to_string(keyInt)),
            "b-frames=0",
            "rate-control=cbr",
            "idr-interval=0"
        });
    } else if (encoderFactory == "vah264enc") {
        // Keep the GstVA command line on the conservative property set that is
        // known to work on older Intel generations (including Ivy Bridge).
        // h264parse downstream repeats codec headers, so encoder-specific AUD /
        // target-usage knobs are intentionally not required here.
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            property("key-int-max", std::to_string(keyInt)),
            "b-frames=0",
            "rate-control=cbr"
        });
    } else if (encoderFactory == "vaapih264enc") {
        // Legacy VAAPI fallback. Keep the argument set conservative because
        // property names differ between Ubuntu/GStreamer generations.
        args.insert(args.end(), {
            property("bitrate", std::to_string(bitrateKbps)),
            "rate-control=cbr"
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

    if (encoderFactory == "nvh264enc" || isIntelVideoEncoder(encoderFactory)) {
        args.insert(args.end(), {"!", "videoconvert"});
    }
    args.insert(args.end(), {"!", scaledVideoCaps(geometry, encoderFactory)});

    // 203.45: encode H.264 once. Keep a byte-stream-friendly shared encoder
    // output; per-output h264parse branches below convert to AVC when FLV needs it.
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

    for (const auto& output : outputs) {
        const std::string suffix = "_out" + std::to_string(output.index);
        const bool flv = output.spec.container == ContainerKind::Flv;
        const bool rtsp = output.spec.container == ContainerKind::Rtsp;

        args.insert(args.end(), {"transcode_video_encoded_tee.", "!"});
        addQueue(args, "transcode_video_mux_queue" + suffix,
                 output.temporaryPreview ? 1000000000ULL : 3000000000ULL,
                 output.temporaryPreview);
        args.insert(args.end(), {
            "!", "h264parse", property("config-interval", "-1"),
            "!", flv
                ? "video/x-h264,stream-format=avc,alignment=au"
                : "video/x-h264,stream-format=byte-stream,alignment=au",
            "!", output.spec.videoPad
        });

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
        std::cerr << "GStreamer transcoder 203.57: video=h264"
                  << " encoder_request=" << baseConfig.transcodeVideoEncoder
                  << " deinterlace=yadif-top-fields"
                  << " cadence=fixed-25p"
                  << " output=" << baseConfig.transcodeResolution
                  << " architecture=shared-decode-encode+per-output-mux"
                  << " outputs=" << outputs.size()
                  << std::endl;
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
    // 203.67 preview: share decoded/encoded A/V with the existing child process.
    // The private HTTP sink has no clients until an authenticated admin opens
    // the preview. No second source, decoder or video/audio encoder is launched.
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

    std::cerr << "GStreamer shared transcoder 203.45: outputs=" << outputs.size()
              << " decode_instances=1 video_encode_instances=1 audio_encode_instances=1"
              << " mux_policy=per-output"
              << std::endl;
    for (std::size_t index = 0; index < outputs.size(); ++index) {
        const auto& output = outputs[index];
        const auto outputKind = tvs::protocols::outputKind(output);
        std::cerr << "GStreamer shared transcoder output 203.45: index=" << index
                  << " type=" << tvs::protocols::normalizedOutputType(output)
                  << " host=" << output.outputHost
                  << " port=" << output.outputPort;
        if (outputKind == tvs::protocols::OutputKind::FifoRelay) {
            std::cerr << " ts-relay=unpaced";
        } else if (tvs::protocols::isTsOutput(outputKind)) {
            if (tvs::protocols::transportCbrEnabled(output)) {
                std::cerr << " ts-cbr-bitrate=" << tvs::protocols::muxBitrate(output);
            } else {
                std::cerr << " ts-cbr=off";
            }
        } else {
            std::cerr << " encoder-cbr-bitrate=" << tvs::protocols::safeVideoBitrate(output);
        }
        std::cerr << std::endl;
    }

    std::cerr << "GStreamer transcoder command: " << commandLineForLog(args) << std::endl;

    ChildProcess child;
    if (!spawnProcess(args, description, child, error)) return false;

    std::cerr << "GStreamer shared transcoder started pid=" << child.pid
              << " outputs=" << outputs.size()
              << " remap=" << (config.remapEnabled ? "on" : "off")
              << " input_sid=" << effectiveInputServiceId(config)
              << " service=" << config.serviceId
              << " vpid=" << config.videoPid
              << " apid=" << config.audioPid
              << std::endl;

    {
        std::lock_guard<std::mutex> lock(childrenMutex);
        children.clear();
        children.push_back(std::move(child));
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
