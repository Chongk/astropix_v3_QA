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
}//make_graph

float GetPol1Slope(TGraph* g)
{
	if (!g || g->GetN() < 2) return 999;

	const float x0 = g->GetPointX(0);
	const float x1 = g->GetPointX(g->GetN()-1);
	TF1* f = new TF1(Form("f1_pol1_%s", g->GetName()), "pol1", x0, x1);
	f->SetParameters(g->GetPointY(0), 0);
	g->Fit(f->GetName(), "EQR0", "", x0, x1);

	const float slope = f->GetParameter(1);
	delete f;
	return slope;
}//GetPol1Slope

//-----------------------------------------------------------------------------------------------
bool ana_03_threshold_scan(const char* path, bool use_readout_index = true, bool verbose = false)
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

	// Populate containers
	vector<double> x_values;
    vector<double> decoded_hits;
    vector<double> unique_readouts;
    vector<double> skipped_bytes;
    vector<double> total_bytes;
	vector<double> malformed_frac;

    for (size_t a=0; a<results.size(); ++a)
    {
		string pattern = "out_thr_scan";
		x_values.push_back(static_cast<double>(listup_scan_index(files[a], pattern, static_cast<int>(a))));

        const auto& s = results[a].stats;
        decoded_hits.   push_back(static_cast<double>(s.decoded_hits));
        unique_readouts.push_back(static_cast<double>(s.unique_readout_count));
        skipped_bytes.  push_back(static_cast<double>(s.skipped_bytes));
        total_bytes.    push_back(static_cast<double>(s.total_bytes));
        malformed_frac. push_back(static_cast<double>(s.malformed_fraction));
    }

    TGraph* g_raw  = make_graph(x_values, total_bytes, "g_raw", "Total raw bytes;threshold point;");
    TGraph* g_hits = make_graph(x_values, decoded_hits, "g_hits", "Decoded hit count;threshold point;");
    TGraph* g_skip = make_graph(x_values, skipped_bytes, "g_skips", "Skipped bytes;threshold point;");
    TGraph* g_ro   = make_graph(x_values, unique_readouts, "g_ro", "Unique readout count;threshold point;");
    TGraph* g_malf = make_graph(x_values, malformed_frac, "g_malf", "");
	g_malf->SetTitle("Malformed frac: (skipped + trailling)/total;threshold_point;");

	// Rough pass/fail analysis
	const float s_raw  = GetPol1Slope(g_raw);
	const float s_hits = GetPol1Slope(g_hits);
	const float s_skip = GetPol1Slope(g_skip);
	const float s_malf = GetPol1Slope(g_malf);

	// Draw
	if (verbose)
	{
		TCanvas* c1 = new TCanvas("c1_thr_scan_noise", "", 1600, 900);
		c1->SetTitle("Decoding summary of threshold scan vs. noise");
		c1->Divide(3, 2);
		c1->cd(1); g_raw->Draw("APL");
		c1->cd(2); g_hits->Draw("APL");
		c1->cd(3); g_skip->Draw("APL");
		c1->cd(4); g_malf->Draw("APL");
		c1->cd(5); g_ro->Draw("APL");
		c1->Print(Form("%s/%s.png", fs::path(path).c_str(), c1->GetName()));
	}
	else
	{
		cout << "\nindex decoded_hits unique_readouts skipped_bytes total_bytes malformed_frac\n";
		for (size_t a=0; a<results.size(); ++a)
		{
			const auto& s = results[a].stats;
			cout << Form("%5zu %12zu %15zu %13zu %11zu %14.6f\n",
					a, s.decoded_hits, s.unique_readout_count, s.skipped_bytes, s.total_bytes,
					s.malformed_fraction);
		}

		delete g_raw;
		delete g_hits;
		delete g_skip;
		delete g_ro;
		delete g_malf;
	}

	if ( (s_raw < 0) && (s_hits < 0) && (s_skip < 0) && (s_malf < 0) ) return true;
	else return false;
}//ana_03_threshold_scan

//--------------------------------------------------------------------------------
void qa_ana(const char* path, bool use_readout_index = true, bool verbose = false)
{
    bool pass_thr_noise = ana_03_threshold_scan(path, use_readout_index, verbose);

    return;
}//analysis
