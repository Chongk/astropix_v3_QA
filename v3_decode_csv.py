

def main(args):

# ====================================================

if __name__ == "__main__":

	# Arguments
	parser = argparse.ArgumentParser(description = 'AstroPix driver code')
	parser.add_argument('-n', '--name', default = '', required = False,
			help = 'Optional suffix name to be attached to output files. Default: none')
	parser.add_argument('-o', '--outdir', default = 'data', required = False,
			help = 'Directory for all output files. Deafult: data')

	main(args)
