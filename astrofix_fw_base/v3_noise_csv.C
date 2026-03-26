#include "TF1.h"
#include "TH1.h"
#include "TH2.h"
#include "TString.h"

#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <sstream>
#include <vector>
using namespace std;

typedef struct
{
	int lineNo;
	int location;
	int isCol;
	int timestamp;
	float tot_us;
} st_noise;

void v3_noise_csv(const char* inFile = "data/20251118-110853_30s_150mV.csv")
{
    //Dummy variables
    string str, strfrag;
    int nLine = 0;
    int lineNo, readout, chipid, payload, location, isCol;
    int timestamp, tot_msb, tot_lsb, tot_total;
	float tot_us;
	double hittime;

    //-------------------------------------------

	int cReadout = 0; //Current readout #
	set <int> nReadout; //Total number of readout
	st_noise stn; //Dummy struct
	vector<st_noise> vec_stn;
	vector<vector<st_noise>> vec_readout;

	const int nCh = 35; //# of pixels of AstroPix_v3 per axis (35 x 35)
	TH1F* H1x = new TH1F("H1_col", "Raw (col);col", nCh, 0, nCh); H1x->Sumw2();
	TH1F* H1y = new TH1F("H1_row", "Raw (row);row", nCh, 0, nCh); H1y->Sumw2();

    //-------------------------------------------

    //Read data
    ifstream in(inFile);
    while (in.is_open())
    {
        nLine++;
        getline(in, str);
        if (!in.good() || in.eof()) break;
        if (nLine == 1) continue; //Skip header

        //Read each element in a line
        stringstream ss(str);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else lineNo    = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else readout   = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else chipid    = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else payload   = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else location  = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else isCol     = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else timestamp = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else tot_msb   = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else tot_lsb   = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else tot_total = stoi(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else tot_us    = stof(strfrag);
        getline(ss, strfrag, ','); if (strfrag.empty()) continue; else hittime   = stod(strfrag);

		if (cReadout != readout)
		{
			cReadout = readout;
			vec_readout.push_back(vec_stn);
			vec_stn.clear();
		}

		stn.lineNo    = lineNo;
		stn.location  = location;
		stn.isCol     = isCol;
		stn.timestamp = timestamp;
		stn.tot_us    = tot_us;
		vec_stn.push_back(stn);

		#if 0
		if (nLine == 2) cout <<"lineNo, readout, chipID, payload, loaction, isCol, ";
		if (nLine == 2) cout <<"timestamp, tot_msb, tot_lsb, tot_total, tot_us, hittime\n";
		cout <<Form("%i %i %i %i %2i %i %3i %2i %3i %4i %7.4f %17.6f\n",
				lineNo, readout, chipid, payload, location,	isCol,
				timestamp, tot_msb, tot_lsb, tot_total, tot_us, hittime);
		if (nLine > 100) break;
		#endif

		nReadout.insert(readout);
		(isCol == true) ? H1x->Fill(location):H1y->Fill(location);
    }//while
    in.close();

    //-------------------------------------------

	TH2F* H2_hit = new TH2F("H2_hit", "Hit match;col;row", nCh,0,nCh, nCh,0,nCh); H2_hit->Sumw2();
	TH1F* H1_hit_totus = new TH1F("H1_hit_tot_us", "Hit match;tot_us", 1000, 0, 20); H1_hit_totus->Sumw2();

	//Hit matching via iteration over readout
	for (int a=0; a<(int)vec_readout.size(); a++)
	{
		const int n_stn = vec_readout[a].size(); //Number of st_noise w/ same readout #
		const int cut_timestamp = 2;
		const float cut_tot_us = 10;
		bool matched[n_stn];
		for (int b=0; b<n_stn; b++) matched[b] = false;

		for (int b=0; b<n_stn; b++)
		{
			if (matched[b] == true) continue;
			for (int c=0; c<n_stn; c++)
			{
				bool isCol_1 = vec_readout[a][b].isCol;
				bool isCol_2 = vec_readout[a][c].isCol;
				int tstamp_1 = vec_readout[a][b].timestamp;
				int tstamp_2 = vec_readout[a][c].timestamp;
				float tot_us_1 = vec_readout[a][b].tot_us;
				float tot_us_2 = vec_readout[a][c].tot_us;

				if ( (isCol_1 != isCol_2) &&
					 fabs( (tstamp_1 - tstamp_2) < cut_timestamp ) &&
					 fabs( (tot_us_1 - tot_us_2)/tot_us_1 < cut_tot_us ) )
				{
					matched[c] = true;
					//cout <<a <<" " <<vec_readout[a][b].location <<" " <<vec_readout[a][c].location <<endl;

					int col, row;
					float tot_us;
					if (vec_readout[a][b].isCol == true)
					{
						col = vec_readout[a][b].location;
						row = vec_readout[a][c].location;
					}
					else
					{
						col = vec_readout[a][c].location;
						row = vec_readout[a][b].location;
					}
					tot_us = (vec_readout[a][b].tot_us + vec_readout[a][c].tot_us) * 0.5;

					H2_hit->Fill(col, row);
					H1_hit_totus->Fill(tot_us);
				}
			}//c
		}//b
	}//a


    //-------------------------------------------

	#if 1
	TString dat = inFile;
	dat.ReplaceAll(".csv", "");
	dat.ReplaceAll("data/", "");
	TF1* F1x = new TF1("F1_pol0x", "pol0", 0, nCh);
	TF1* F1y = new TF1("F1_pol0y", "pol0", 0, nCh);
	F1x->SetLineStyle(2);
	F1y->SetLineStyle(2);

    TCanvas* c1 = new TCanvas("c1_raw", "", 400*1*3, 300*2*3);
	c1->Divide(1, 2);
	c1->cd(1)->SetLogy();
	H1x->SetTitle(Form("%s, %s", H1x->GetTitle(), dat.Data()));
	H1x->SetMinimum(1);
	H1x->SetMaximum(H1x->GetMaximum()*4);
	//H1x->SetStats(false);
	H1x->DrawCopy("hist e");
	H1x->Fit(F1x->GetName(), "EQR0", 0, 35);
	F1x->Draw("same");
	c1->cd(2)->SetLogy();
	H1y->SetTitle(Form("%s, %s", H1y->GetTitle(), dat.Data()));
	H1y->SetMinimum(1);
	H1y->SetMaximum(H1y->GetMaximum()*4);
	//H1y->SetStats(false);
	H1y->DrawCopy("hist e");
	H1y->Fit(F1y->GetName(), "EQR0", 0, 35);
	F1y->Draw("same");
	c1->Print(Form("%s_%s.png", c1->GetName(), dat.Data()));

	TCanvas* c2 = new TCanvas("c2_match", "", 400*2*3, 300*2*3);
	c2->Divide(2, 2);
	c2->cd(1);
	H1_hit_totus->SetTitle(Form("%s, %s", H1_hit_totus->GetTitle(), dat.Data()));
	H1_hit_totus->Rebin(10);
	H1_hit_totus->DrawCopy("hist e");
	c2->cd(3)->SetGrid();
	H2_hit->SetTitle(Form("%s, %s", H2_hit->GetTitle(), dat.Data()));
	H2_hit->SetStats(false);
	H2_hit->SetMarkerSize(0.7);
	H2_hit->DrawCopy("colz text45");
	c2->cd(2)->SetLogy();
	TH1F* H1_hit_x = (TH1F*)H2_hit->ProjectionX();
	H1_hit_x->SetMinimum(1);
	//H1_hit_x->SetStats(false);
	H1_hit_x->DrawCopy("hist e");
	c2->cd(4)->SetLogy(); 
	TH1F* H1_hit_y = (TH1F*)H2_hit->ProjectionY();
	H1_hit_y->SetMinimum(1);
	//H1_hit_y->SetStats(false);
	H1_hit_y->DrawCopy("hist e");
	c2->Print(Form("%s_%s.png", c2->GetName(), dat.Data()));
	#endif

    return;
}//Main
