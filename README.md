# astropix\_v3\_QA
Codes for mass chip QA

- May 6, 2026: cpp based decoding and analysis modules added:
	1. decode:\
		a. decode.h\
		b. decode.cpp
	2. analysis: qa\_ana.cpp

- Apr. 1st, 2026:
	1. Remarks:\
		a. Base QA framework is completed and tested; Base fw: A-STEP\
		b. The codes are written with AI assistance: they'll be cleaned up later
	2. Structure:\
		a. v3/config.py:     only true source for the chip configuration\
		b. v3/protocol.py:   generate SR bitstream and SPI frame\
		c. v3/transport.py:  board I/O primitive\
		d. v3/controller.py: application and flow control\
		e. v3/daq.py:        IRQ based readout state machine\
		f. v3/qa.py:         QA routines\
		g. v3_qa_run.py:     QA runner script

- Mar. 26, 2026: Base fw updated

- Feb. 17, 2026: 1st commit
