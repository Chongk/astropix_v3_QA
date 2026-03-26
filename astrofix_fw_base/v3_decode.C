#include "TF1.h"
#include "TH1.h"
#include "TH2.h"
#include "TString.h"
#include "TStopwatch.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <sstream>
#include <vector>
#include <thread>

using namespace std;

//Bitwise reverse
uint8_t v3_reverse_bits(uint8_t x)
{
    x = (x & 0xF0) >> 4 | (x & 0x0F) << 4;
    x = (x & 0xCC) >> 2 | (x & 0x33) << 2;
    x = (x & 0xAA) >> 1 | (x & 0x55) << 1;
    return x;
}

void v3_decode(
		const char* inFile = "data/20251118-110853_30s_150mV.dat", //Binary data file (hexadecimal)
		size_t data_bytes = 5, //Data size per readout (bytes, including marker "20")
		int sampleclock_period_ns = 5
		)
{
	TStopwatch sw;
	sw.Start();

	//Open
	ifstream in;
	in.open(inFile, std::ios::binary);
	if (!in.is_open()) { cout <<Form("Cannot open the file %s: stop. \n", inFile); return; }

	//Get the file size, and then read the file into a vector of bytes
	in.seekg(0, in.end);
	streamsize size = in.tellg();
	in.seekg(0, in.beg);
	vector<char> dat(size);

	//Make char vector into a string vector
	vector<string> dat_lines;
	const char bit_null = 'f';
	const char bit_prime = '\'';
	bool prime_1st = false;
	bool prime_2nd = false;
	string str;

	//Read raw data within two primes, discard null characters (f) 
	if (in.read(dat.data(), size))
	{
		for (char bit : dat)
		{
			if (bit == bit_null) continue;

			if (!prime_1st && !prime_2nd && bit==bit_prime) 
			{
				prime_1st = true;
				continue;
			}

			if (prime_1st)
			{
				if (bit == bit_prime) prime_2nd = true;

				if (!prime_2nd)
				{
					str += bit;
					//cout <<bit;
				}
				else
				{
					prime_1st = false;
					prime_2nd = false;
					dat_lines.push_back(str);
					str.clear();
					//cout <<endl;
				}
			}//prime_1st
		}//for
	}//if
	in.close();
	//sw.Print("u");
	//sw.Reset();
	//sw.Start();

	//---------------------------------------------------------------

	//std::this_thread::sleep_for(std::chrono::microseconds(500));

	ofstream out;
	TString outFile = inFile;
	outFile.ReplaceAll(".dat", ".txt");
	out.open(outFile.Data());

	//Extract data hit by hit (specific bytes including '20')
	const string data_marker = "20";
	const size_t data_length = (data_bytes - 1) * 2; //2 hex characters per byte: -1 means w/o "20"
	const unsigned int dat_lines_size = dat_lines.size();
	int readout = 0;
	for (unsigned int a=0; a<dat_lines_size; a++)
	{
		//Each index corresponds to a string line: i.e., a readout
		string line = dat_lines[a];
		//cout <<line.c_str() <<endl;

		size_t pos = 0;
		while (true)
		{
			pos = line.find(data_marker, pos);
			if (pos == string::npos) break;

			size_t start = pos + data_marker.size();
			if (start + data_length <= line.size())
			{
				string line_split = data_marker + line.substr(start, data_length);
				//cout <<line_split <<" ";

				if ( line_split.find("bcbc") == string::npos &&
						line_split.find("bcbcbc") == string::npos &&
						line_split.find("bcbcbcbc") == string::npos ) //bc: heartbeat marker, meaningless
				{
					//Data is written in LSB-first format: reverse it to MSB-first
					uint8_t byte[(int)data_bytes];
					for (int b=0; b<(int)data_bytes; b++)
					{
						uint8_t bit_raw = static_cast<uint8_t>(stoi(line_split.substr(2*b, 2), nullptr, 16));
						byte[b] = v3_reverse_bits(bit_raw);
					}

					//Decoding
					uint8_t payload   = byte[0]        & 0x07; //Bits [2, 0]
					uint8_t id        = (byte[0] >> 3) & 0x1F; //Bits [7, 3]
					uint8_t location  = byte[1]        & 0x3F; //Bits [5, 0]
					uint8_t isCol     = (byte[1] >> 7) & 0x01; //Bit 7
					uint8_t timestamp = byte[2];
					uint16_t tot_msb   = byte[3] & 0x0F;
					uint16_t tot_lsb   = byte[4];
					uint16_t tot_total = (tot_msb << 8) | tot_lsb;
					double   tot_us    = (tot_total * sampleclock_period_ns) * 0.001;

					#if 0
					cout <<Form("%i %i %i %2i %i %3i %2i %3i %4i %7.4f\n",
							readout, id, payload, location, isCol, timestamp,
							tot_msb, tot_lsb, tot_total, tot_us);
					#endif

					out <<Form("%i %i %i %i %2i %i %3i %2i %3i %4i %7.4f\n",
							a, readout, id, payload, location, isCol, timestamp,
							tot_msb, tot_lsb, tot_total, tot_us);

				}//Only if data don't include the heartbeats
				//else cout <<endl;
			}//Read only complete (no truncated) lines

			pos = start;
		}//While
		readout++;
	}//a
	out.close();

	sw.Print("u");
	sw.Stop();

	return;
}//Main
