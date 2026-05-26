#ifndef DECODE_H
#define DECODE_H

#include "TCanvas.h"
#include "TF1.h"
#include "TGraph.h"
#include "TGraphErrors.h"
#include "TH1.h"
#include "TH2.h"
#include "TLegend.h"
#include "TString.h"

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <map>
#include <regex>
#include <set>
#include <string>
#include <tuple>
#include <vector>
using namespace std;
namespace fs = std::filesystem;

struct ReadoutRange
{
    long long readout_number    = -1;
    long long byte_offset_start = -1;
    long long byte_offset_end   = -1;
};

struct V3Hit
{
    size_t record_offset  = 0;
    int    readout_number = -1; // stays -1 when sidecar merge is disabled or unavailable

    uint8_t astep_header = 0;
    uint8_t layer_id     = 0;
    uint8_t chip_id      = 0;
    uint8_t payload      = 0;
    uint8_t location     = 0;
    uint8_t isCol        = 0;
    uint8_t timestamp    = 0;

    uint16_t tot_msb   = 0;
    uint16_t tot_lsb   = 0;
    uint16_t tot_total = 0;
    double   tot_us    = 0.;

    uint32_t fpga_ts = 0;
};

struct DecodeStats
{
    string file;
    size_t total_bytes        = 0;
    size_t decoded_hits       = 0;
    size_t skipped_bytes      = 0;
    size_t trailing_bytes     = 0;
    size_t malformed_bytes    = 0;
    double malformed_fraction = 0.0;

    size_t readout_ranges_loaded = 0;
    size_t unique_readout_count  = 0;
    size_t hits_without_readout  = 0;

    bool used_readout_index   = false;
    bool readout_index_loaded = false;
};

struct DecodeResult
{
    vector<V3Hit> hits;
    DecodeStats stats;
};

void decode(void) {} // Dummy function to suppress warning message
void draw_hit(vector<DecodeResult> results, bool use_readout_index = true, const char* path = "./");

bool extract_ll_field     (const std::string& obj, const std::string& key, long long& value);
bool is_valid_astep_header(const vector<uint8_t>& data, size_t i, int astep_packet_size);
bool load_readout_index   (const char* json_path, vector<ReadoutRange>& v_readout_ranges, bool verbose = false);
bool read_file_bytes      (const char* path, vector<uint8_t>& bytes);

int listup_scan_index          (const std::string& file, const std::string& pattern, int fallback);
int lookup_readout_number      (size_t record_offset, const vector<ReadoutRange>& v_readout_ranges);
vector<string> listup_bin_files(const char* path, bool verbose = false);

DecodeResult decode_astep(const char* data_bin, bool use_readout_index = true, bool verbose = false);

#endif //DECODE_H
