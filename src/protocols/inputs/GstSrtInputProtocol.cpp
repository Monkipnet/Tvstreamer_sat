#include "protocols/inputs/GstSrtInputProtocol.h"
#include "protocols/SrtVpsProfile.h"
#include "utils.h"
namespace tvs::protocols::inputs {
bool isSrtInput(const StreamConfig& cfg) {
    return toLower(cfg.inputUri).rfind("srt://", 0) == 0;
}
std::string srtInputUri(const StreamConfig& cfg) {
    std::string uri = cfg.inputUri;
    if (uri.find("mode=") == std::string::npos) {
        const std::string mode = toLower(cfg.inputMode) == "listener" ? "listener" : "caller";
        uri += (uri.find('?') == std::string::npos ? "?" : "&");
        uri += "mode=" + mode;
    }
    return tvs::protocols::srt_vps::applyToUri(uri, cfg);
}
}
