#include "decode.h"

TGraph* make_graph(const vector<double>& x, const vector<double>& y, const char* name, const char* title)
{
    auto* g = new TGraph(static_cast<int>(x.size()));
    for (int i = 0; i < static_cast<int>(x.size()); ++i) g->SetPoint(i, x[i], y[i]);

    g->SetName(name);
    g->SetTitle(title);
    g->SetMarkerStyle(24);
    g->SetLineWidth(2);
    return g;
}

vector<DecodeResult> ana_03_threshold_scan(const char* path, bool use_readout_index = true, bool verbose = false)
{
    vector<string> files = listup_bin_files(path, verbose);
    vector<DecodeResult> results;
    results.reserve(files.size());

    // Stable order by file name, uses file order as x axis
    for (size_t a=0; a<files.size(); ++a)
    {
        cout <<"Processing " <<files[a] <<endl;
        results.push_back(decode_astep(files[a].c_str(), use_readout_index, verbose));
    }

	// Print decoding result
    cout << "\nindex decoded_hits unique_readouts skipped_bytes total_bytes malformed_frac\n";
    for (size_t a=0; a<results.size(); ++a)
    {
        const auto& s = results[a].stats;
        cout << Form("%5zu %12zu %15zu %13zu %11zu %14.6f\n",
				a, s.decoded_hits, s.unique_readout_count, s.skipped_bytes, s.total_bytes, s.malformed_fraction);
    }

	// Populate containers
	// ++++++++++++++++++++++++++++++++++++++++++

	vector<double> x_values;
    vector<double> decoded_hits;
    vector<double> unique_readouts;
    vector<double> skipped_bytes;
    vector<double> total_bytes;

    for (size_t a=0; a<results.size(); ++a)
    {
		string pattern = "out_thr_scan";
		x_values.push_back(static_cast<double>(listup_scan_index(files[a], pattern, static_cast<int>(a))));

        const auto& s = results[a].stats;
        decoded_hits.   push_back(static_cast<double>(s.decoded_hits));
        unique_readouts.push_back(static_cast<double>(s.unique_readout_count));
        skipped_bytes.  push_back(static_cast<double>(s.skipped_bytes));
        total_bytes.    push_back(static_cast<double>(s.total_bytes));
    }

    TGraph* g_hits = make_graph(x_values, decoded_hits, "g_hits", "Decoded hit count;threshold point;");
    TGraph* g_ro   = make_graph(x_values, unique_readouts, "g_ro", "Unique readout count;threshold point;");
    TGraph* g_skip = make_graph(x_values, skipped_bytes, "g_skips", "Skipped bytes;threshold point;");
    TGraph* g_raw  = make_graph(x_values, total_bytes, "g_raw", "Total raw bytes;threshold point;");

	// Draw
	// ++++++++++++++++++++++++++++++++++++++++++

	//if (verbose)
	{
		TCanvas* c1 = new TCanvas("c1_thr_scan_noise", "", 1400, 900);
		c1->SetTitle("Decoding summary of threshold scan vs. noise");
		c1->Divide(2, 2);
		c1->cd(1); g_hits->Draw("APL");
		c1->cd(2); g_ro->Draw("APL");
		c1->cd(3); g_skip->Draw("APL");
		c1->cd(4); g_raw->Draw("APL");
		c1->Print(Form("%s/%s.png", fs::path(path).c_str(), c1->GetName()));
	}

    return results;
}//ana_03_threshold_scan

//----------------------------------------------------
void qa_ana(const char* path, bool use_readout_index = true, bool verbose = false)
{
    ana_03_threshold_scan(path, use_readout_index, verbose);
    return;
}//analysis
