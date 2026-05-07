#include "decode.h"

bool extract_ll_field(const std::string& obj, const std::string& key, long long& value)
{
    std::regex re("\\\"" + key + "\\\"\\s*:\\s*(-?[0-9]+)");
    std::smatch m;
    if (!std::regex_search(obj, m, re)) return false;
    value = std::stoll(m[1].str());
    return true;
}//extract_ll_field

bool is_valid_astep_header(const vector<uint8_t>& data, size_t i, int astep_packet_size)
{
    if (i + astep_packet_size > data.size()) return false;
    return (data[i] == 0x0A) && (data[i + 1] == 0x01 || data[i + 1] == 0x02 || data[i + 1] == 0x03);
}//is_valid_steap_header

bool load_readout_index(const char* json_path, vector<ReadoutRange>& v_readout_ranges, bool verbose)
{
    v_readout_ranges.clear();

    if (json_path == nullptr || std::string(json_path).empty())
    {
        cout << "No readout-index JSON was found; readout_number merge disabled.\n";
        return false;
    }

    ifstream in(json_path);
    if (!in.is_open())
    {
        cout << Form("Cannot open readout-index JSON: %s\n", json_path);
        return false;
    }

    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::regex obj_re(R"(\{[^{}]*\})");

    for (std::sregex_iterator it(content.begin(), content.end(), obj_re), obj_end; it != obj_end; ++it)
    {
        const std::string obj = it->str();

        long long readout_number    = -1;
        long long byte_offset_start = -1;
        long long byte_offset_end   = -1;

        const bool has_readout = extract_ll_field(obj, "readout_number", readout_number);
        const bool has_start   = extract_ll_field(obj, "byte_offset_start", byte_offset_start);
        const bool has_end     = extract_ll_field(obj, "byte_offset_end", byte_offset_end);
        if (!has_readout || !has_start || !has_end) continue;

        ReadoutRange r;
        r.readout_number = readout_number;
        r.byte_offset_start = byte_offset_start;
        r.byte_offset_end = byte_offset_end;
        v_readout_ranges.push_back(r);
    }

    std::sort(v_readout_ranges.begin(), v_readout_ranges.end(),
			[](const ReadoutRange& a, const ReadoutRange& b)
			{ return a.byte_offset_start < b.byte_offset_start; }
			);

    if (verbose)
    {
        cout << Form("Loaded %zu readout ranges from %s\n", v_readout_ranges.size(), json_path);
        if (!v_readout_ranges.empty())
        {
            const auto& first = v_readout_ranges.front();
            const auto& last = v_readout_ranges.back();
            cout << Form("First readout: #%lld [%lld, %lld)\n",
					first.readout_number, first.byte_offset_start, first.byte_offset_end);
            cout << Form("Last readout:  #%lld [%lld, %lld)\n",
					last.readout_number, last.byte_offset_start, last.byte_offset_end);
        }
    }

    return !v_readout_ranges.empty();
}//load_readout_index

bool read_file_bytes(const char* path, vector<uint8_t>& bytes)
{
    ifstream in(path, ios::binary);
    if (!in.is_open()) { cout << Form("Cannot open binary file: %s\n", path); return false; }

    in.seekg(0, ios::end);
    const streamoff size = in.tellg();
    in.seekg(0, ios::beg);
    if (size < 0) { cout << Form("Invalid binary file size: %s\n", path); return false; }

    bytes.resize(static_cast<size_t>(size));
    if (!bytes.empty()) in.read(reinterpret_cast<char*>(bytes.data()), static_cast<streamsize>(bytes.size()));
    return true;
}//read_file_bytes

int listup_scan_index(const std::string& file, const std::string& pattern, int fallback)
{
	// Examples: <out_thr_scan>_0_run.bin <-> <out_thr_scan>_9_run.bin
	std::string re_str = pattern + R"(_([0-9]+)_run\.bin$)";

	try
	{
		std::regex re(re_str);
		std::smatch m;
		const std::string base = fs::path(file).filename().string();
		if (std::regex_search(base, m, re)) return std::stoi(m[1].str());
	}
	catch (const std::regex_error& e)
	{
		cout <<"ERROR: special character has been detected in the pattern!\n";
		return fallback;
	}

	return fallback;
}//search_scan_index

int lookup_readout_number(size_t record_offset, const vector<ReadoutRange>& v_readout_ranges)
{
    if (v_readout_ranges.empty()) return -1;

    const long long off = static_cast<long long>(record_offset);
    for (const auto& r : v_readout_ranges)
    {
        if (r.byte_offset_start <= off && off < r.byte_offset_end)
		{
			return static_cast<int>(r.readout_number);
		}
    }

    return -1;
}//lookup_readout_number

vector<string> listup_bin_files(const char* path, bool verbose)
{
    vector<string> files;
    fs::path p(path);

    if (fs::exists(p) && fs::is_regular_file(p) && p.extension() == ".bin")
    {
        files.push_back(p.string());
    }
    else if (fs::exists(p) && fs::is_directory(p))
    {
        for (const auto& entry : fs::directory_iterator(p))
        {
            if (entry.is_regular_file() && entry.path().extension() == ".bin")
                files.push_back(entry.path().string());
        }
    }

    sort(files.begin(), files.end());
    if (verbose) for (const auto& f : files) cout << f << '\n';
    if (files.empty()) cout << "WARNING: no binary files found from: " << path << endl;
    return files;
}//listup_bin_files

DecodeResult decode_astep(
    const char* data_bin,
    bool use_readout_index = true,
    bool verbose = false)
{
    const size_t astep_packet_size = 11;
    const int sampleclock_period_ns = 5;

    DecodeResult result;
    result.stats.file = data_bin;
    result.stats.used_readout_index = use_readout_index;

    vector<uint8_t> v_bytes;
    vector<ReadoutRange> v_readout_ranges;

    if (!read_file_bytes(data_bin, v_bytes)) return result;
    result.stats.total_bytes = v_bytes.size();

    if (use_readout_index)
    {
		string json_path_org = data_bin;
		string json_path_prx = std::regex_replace(json_path_org, std::regex(R"(\.bin$)"), "_readout_index.json");
        result.stats.readout_index_loaded = load_readout_index(json_path_prx.c_str(), v_readout_ranges, verbose);
        result.stats.readout_ranges_loaded = v_readout_ranges.size();
    }

    size_t i = 0;
    size_t skipped = 0;
    while (i + astep_packet_size <= v_bytes.size())
    {
        if (!is_valid_astep_header(v_bytes, i, astep_packet_size))
        {
            ++i;
            ++skipped;
            continue;
        }

        V3Hit hit;
        hit.record_offset = i;
        hit.readout_number = use_readout_index ? lookup_readout_number(i, v_readout_ranges) : -1;

        hit.astep_header = v_bytes[i + 0];
        hit.layer_id     = v_bytes[i + 1];

        const uint8_t header     = v_bytes[i + 2];
        const uint8_t location_b = v_bytes[i + 3];
        const uint8_t ts_b       = v_bytes[i + 4];
        const uint8_t tot_msb_b  = v_bytes[i + 5];
        const uint8_t tot_lsb_b  = v_bytes[i + 6];

        hit.fpga_ts =
            (static_cast<uint32_t>(v_bytes[i +  7]) <<  0) |
            (static_cast<uint32_t>(v_bytes[i +  8]) <<  8) |
            (static_cast<uint32_t>(v_bytes[i +  9]) << 16) |
            (static_cast<uint32_t>(v_bytes[i + 10]) << 24);

        hit.chip_id   = (header >> 3) & 0x1F;
        hit.payload   = header        & 0x07;
        hit.isCol     = (location_b >> 7) & 0x01;
        hit.location  = location_b        & 0x3F;
        hit.timestamp = ts_b;
        hit.tot_msb   = tot_msb_b & 0x0F;
        hit.tot_lsb   = tot_lsb_b;
        hit.tot_total = (hit.tot_msb << 8) | hit.tot_lsb;
        hit.tot_us    = (static_cast<double>(hit.tot_total) * sampleclock_period_ns) * 0.001;
        result.hits.push_back(hit);

        if (verbose && result.hits.size() <= 20)
        {
            if (result.hits.size() == 1)
				cout << "offset readout layer chip payload location isCol ts tot fpga_ts\n";
            cout << Form("%6zu %4d %d %2d %d %2d %d %3d %4d %10u\n",
                hit.record_offset, hit.readout_number, hit.layer_id, hit.chip_id, hit.payload,
                hit.location, hit.isCol, hit.timestamp, hit.tot_total, hit.fpga_ts);
        }

        i += astep_packet_size; // Once synced to a valid record, advance by the full record size
    }//while

    result.stats.decoded_hits       = result.hits.size();
    result.stats.skipped_bytes      = skipped;
    result.stats.trailing_bytes     = (i < v_bytes.size()) ? (v_bytes.size() - i) : 0;
    result.stats.malformed_bytes    = result.stats.skipped_bytes + result.stats.trailing_bytes;
    result.stats.malformed_fraction = (result.stats.total_bytes > 0)
        ? static_cast<double>(result.stats.malformed_bytes) / static_cast<double>(result.stats.total_bytes)
        : 0.0;

    set<int> unique_readouts;
    size_t hits_without_readout = 0;
    for (const auto& h : result.hits)
    {
        if (h.readout_number >= 0) unique_readouts.insert(h.readout_number);
        else ++hits_without_readout;
    }
    result.stats.unique_readout_count = unique_readouts.size();
    result.stats.hits_without_readout = hits_without_readout;

	if (verbose)
	{
		TString decode_sum = "Decoding summary:\n";
		decode_sum.Append(Form("- Number of hits: %zu\n", result.stats.decoded_hits));
		decode_sum.Append(Form("- Skipped bytes during %zu-bytes A-STEP header search: %zu\n",
					astep_packet_size, result.stats.skipped_bytes));
		decode_sum.Append(Form("- Trailing bytes after parser stop: %zu\n", result.stats.trailing_bytes));
		decode_sum.Append(Form("- Malformed fraction: %.6f\n", result.stats.malformed_fraction));
		if (use_readout_index) decode_sum.Append(Form("- Number of unique readout index: %zu\n",
					result.stats.unique_readout_count));
		cout <<decode_sum.Data();
	}

    return result;
}//decode_astep
