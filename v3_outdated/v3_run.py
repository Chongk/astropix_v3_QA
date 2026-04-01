from astropix import astropixRun
from contextlib import contextmanager
from modules.setup_logger import logger
from tqdm import tqdm

import argparse
import binascii
import logging
import modules.hitplotter as hitplotter
import os
import pandas as pd
import numpy as np
import time
import warnings

# ========================================
def init(astro:astropixRun, args) -> None:

	# Init asic and voltages
	astro.asic_init(yaml = args.yml, analog_col = args.analog)
	astro.init_voltages(vthreshold = args.threshold)

	# Enable/Disable pixels considering injection options
	for c in range(0, 35, 1): # Disable everything
		for r in range(0, 35, 1):
			astro.disable_pixel(c, r)
	if args.inj is None: # Default, columns [0, 2] are not used
		for c in range(3, 35, 1):
			for r in range(0, 35, 1):
				astro.enable_pixel(c, r)
	else: # Injection
		icol = args.inj[0]
		irow = args.inj[1]
		if (icol >= 0) and (irow >= 0): # Single pixel injection
			astro.enable_pixel(icol, irow)
			astro.enable_injection(icol, irow)
		elif (icol < 0) and (irow >= 0): # Column scan for a given row
			for c in range(3, 35, 1):
				astro.enable_pixel(c, irow)
				astro.enable_injection(c, irow)
		elif (icol >= 0) and (irow < 0): # Row scan for a given column
			for r in range(0, 35, 1):
				astro.enable_pixel(icol, r)
				astro.enable_injection(icol, r)
		elif (icol < 0) and (irow < 0): # Diagonal scan: all columns and rows
			for d in range(0, 35, 1):
				astro.enable_pixel(d, d)
				astro.enable_injection(d, d)

	# Send final configuration to the chip
	astro.enable_spi() # self.nexys.spi_reset_fpga_readout() is called
	astro.asic_configure() # self.nexys.chip_reset() is called
	astro.dump_remnants() # ckim, repeat readout until it's empty
	astro.dump_fpga() # Clear FPGA memory

	return None # init

""" Obsolete
#=====================================================
def run_daq(astro:astropixRun, args, datfile) -> None:

	try: # Enclose the main loop for clean keyboard interrupt

		i: int = 0
		start_time = time.time()
		end_time = time.time() + float(args.runtime)
		logger.info(f'Start data taking for {args.runtime} sec...')
		with tqdm(total = args.runtime, desc = 'DAQ is running', unit = 's') as pbar:
			while time.time() <= end_time:
				readout = astro.get_readout() # Check if data exists in the readout stream
				if readout:
					logger.debug(binascii.hexlify(readout))
					datfile.write(f"{i}\t{str(binascii.hexlify(readout))}\n")
					datfile.flush()

				elapsed_time = time.time() - start_time
				diff = elapsed_time - pbar.n
				if (diff > 1.0): pbar.update(diff)

	except KeyboardInterrupt:
		logger.error('Keyboard interrupt: stop.\n')
	except Exception as e:
		logger.error(f'Unexpected error: stop.\n{e}')

	return None # run_daq
"""

#==========================================================================
def idle_readout(readout: bytes, cut_frac_idle = 0.99, cut_n_nonidle = 20):

	IDLE_BYTES = {0xBC, 0xFF}
	n_idle = sum(b in IDLE_BYTES for b in readout)
	n_nonidle = len(readout) - n_idle
	frac_idle = n_idle/len(readout)

	if frac_idle == 1.0:
		return True

	if (frac_idle >= cut_frac_idle) and (n_nonidle < cut_n_nonidle):
		return True

	return False

#===================================================================
def run_daq_irq(astro, args, datfile, *,
				bufferlength = 6, # 6 * 8 = 48 bytes
				min_payload_bytes = 40,
				consecutive_nohit_reads = 2,
				poll_sleep_s = 0.0005):

	try: # Enclose the main loop for clean keyboard interrupt

		i: int = 0
		start_time = time.time()
		end_time = start_time + float(args.runtime)

		with tqdm(total = float(args.runtime), desc = 'DAQ is running (IRQ)', unit = 's') as pbar:
			while time.time() < end_time:

				# Wait for IRQ
				if not astro.hits_present():
					time_diff = (time.time() - start_time) - pbar.n
					if (time_diff > 1.0): pbar.update(time_diff)

					time.sleep(poll_sleep_s)
					continue

				# Drain the burst during IRQ deassert
				nohit_streak: int = 0
				while time.time() < end_time:
					time_diff = (time.time() - start_time) - pbar.n
					if (time_diff > 1.0): pbar.update(time_diff)

					readout = astro.get_SW_readout(bufferlength = bufferlength)
					if readout and (not idle_readout(readout)):
						datfile.write(f"{i}\t{binascii.hexlify(readout).decode()}\n")
						datfile.flush()
						i += 1

					if (readout is None) or (len(readout) < min_payload_bytes) or idle_readout(readout):
						nohit_streak += 1
					else:
						nohit_streak = 0

					irq_now = astro.hits_present()
				if (not irq_now) and (nohit_streak >= consecutive_nohit_reads): break

				if nohit_streak > 0: time.sleep(poll_sleep_s)

	except KeyboardInterrupt:
		logger.error('Keyboard interrupt: stop.\n')
	except Exception as e:
		logger.error(f'Unexpected error: stop.\n{e}')

	return None # run_daq_irq

# =======================================================
def write_csv(astro:astropixRun, csv_prefix:str) -> None:

	i_dat = csv_prefix + '.dat'
	o_csv = csv_prefix + '.csv'

	csv_frame = pd.DataFrame(columns = [
			'readout', 'chipID', 'payload', 'location', 'isCol',\
			'timestamp', 'tot_msb', 'tot_lsb', 'tot_total', 'tot_us', 'hittime'])
	csv_input = np.loadtxt(i_dat, dtype = str)
	csv_str = csv_input[:, 1]

	for i, hexstr in tqdm(
			enumerate(csv_str),
			mininterval = 1,
			desc = f'\n- CSV file is being generated based on {i_dat}',
			ncols = 100,
			unit = 'readouts',
			total = len(csv_str),
			bar_format = '{l_bar}{bar}| {n_fmt}/{total_fmt} [{percentage:10.0f}%]'):

		hexstr = hexstr.strip() # Remove space and linebreak
		if hexstr.startswith("b'") and hexstr.endswith("'"): hexstr = hexstr[2:-1] # Remove if b' exists
		if len(hexstr) % 2 != 0: # Odd-length string
			raise ValueError(f"Invalid hex length: {len(hexstr)}")
			continue

		try:
			rawdata = list(binascii.unhexlify(hexstr))
		except binascii.Error:
			continue

		try:
			hits = astro.decode_readout(rawdata, i, printer = False, chip_version = 3)

			# If the decoded frame is empty or all NaN
			if hits.empty or hits.isna().all().all():
				if csv_frame.empty: # initialize columns if csv_frame is still empty
					csv_frame = pd.DataFrame(columns = hits.columns if not hits.empty else ['i'])
				# fill NaN row with proper column names
				hits = pd.DataFrame([[np.nan] * len(csv_frame.columns)], columns = csv_frame.columns)

			# If csv_frame was empty but hits has data, initialize it properly
			elif csv_frame.empty:
				csv_frame = hits.copy()
				continue
						
			csv_frame = pd.concat([csv_frame, hits], ignore_index=True)
		except IndexError: continue # Cannot decode empty bitstream

	csv_frame.index.name = i_dat
	csv_frame.to_csv(o_csv)

	return None # write_csv

# ==========================================================
def test_injection(astro:astropixRun, args, datname) -> None:

	datfile = open(datname, 'w')

	astro.init_injection(
			inj_voltage = args.injv,
			inj_period = 162, # [0, 255]; @ clkdiv300, 10: ~125 Hz, 100: ~13 Hz, 162: 8.0 Hz, 255: 5.1 Hz
			clkdiv = 300, # [1, 65535]
			initdelay = 100, # [0, 65535]
			pulseperset = 1,
			onchip = True) # onchip is true if chipVer > 2
	astro.start_injection()
	run_daq_irq(astro, args, datfile)
	astro.stop_injection()

	datfile.close()

	return None # test_injection

# ====================================================
def main(args) -> None:

	# Make object and initialize 
	astro = astropixRun(chipversion = 3)#, inject = args.inj)
	init(astro, args)

	# Output directory and log/yml files
	if os.path.exists(args.outdir) == False: os.mkdir(args.outdir)
	suffix = "" if not args.name else "_" + args.name
	start_time = time.strftime("%Y%m%d-%H%M%S")

	ymlname = os.path.join(args.outdir, f"{start_time}{suffix}_{args.yml}.yml")
	with open (ymlname, 'w') as ymlfile:
		astro.write_conf_to_yaml(ymlname)

	logname = os.path.join(args.outdir, f"{start_time}{suffix}.log")
	with open (logname, 'a') as logfile:
		logform = '%(asctime)s:%(msecs)d.%(name)s.%(levelname)s:%(message)s'
		logging.basicConfig(filename = logname, level = args.loglevel, format = logform, filemode = 'a')
		logfile.write(astro.get_log_header())
		logfile.write(str(args))
		logger = logging.getLogger(__name__)

	# -------------------------------------------

	# Injection
	if args.inj != None:
		datprx = os.path.join(args.outdir, f"{start_time}{suffix}_inj")
		test_injection(astro, args, datprx + ".dat")
		if args.savecsv: write_csv(astro, datprx)
	else:
		datname = os.path.join(args.outdir, f"{start_time}{suffix}.dat")
		datfile = open(datname, 'w')
		run_daq_irq(astro, args, datfile)
		datfile.close()

	# -------------------------------------------
	
	astro.close_connection() # Closes SPI

	return None # main

# ====================================================
if __name__ == "__main__":

	# Arguments
	parser = argparse.ArgumentParser(description = 'AstroPix driver code')

	parser.add_argument('-a', '--analog', action = 'store', required = False, type = int, default = 0,
			help = 'Turn on analog output in the given column. Deafult: column 0')
	parser.add_argument('-l', '--loglevel', action = 'store', type = int, default = 20,
			help = "Log level, always print out critical/error/warning; -1: silence; 10: debug; 20: normal")
	parser.add_argument('-n', '--name', default = '', required = False,
			help = 'Optional suffix name to be attached to output files. Default: none')
	parser.add_argument('-o', '--outdir', default = 'data', required = False,
			help = 'Directory for all output files. Deafult: data')
	parser.add_argument('-T', '--runtime', type = int, action = 'store', default = 10,
			help = 'DAQ running time (sec), continual if negative. Default: 12')
	parser.add_argument('-t', '--threshold', type = float, action = 'store', default = 150,
			help = 'Threshold for digital ToT in mV. Default: value in yml or 150 mV if no voltagecard in yml')
	parser.add_argument('-y', '--yml', action = 'store', required = False, type = str, default = 'testconfig_v3',
			help = '.yml file containing chip configuration. Deafult: config_v3_none.yml')

	parser.add_argument('-csv', '--savecsv', action = 'store_true', required = False, default = False,
			help = 'Save output file as CSV by decoding it after data taking. Default: False')
	parser.add_argument('-inj', '--inj', action = 'store', type = int, default = None, nargs = 2,
			help = 'Turn on injection in the given row and column. Default: no injection')
	parser.add_argument('-injv', '--injv', action = 'store', type = float, default = None,
			help = 'Specify injection voltage in mV. Default: none (use value in yml)')

	args = parser.parse_args()

	main(args)
