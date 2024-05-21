# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

.PHONY: all export clean filter segment pdf2text scrape

all: export

export: clean
	cd $@ && make

clean: filter
	cd $@ && make

filter: segment
	cd $@ && make

segment: pdf2text
	cd $@ && make

pdf2text: scrape
	cd $@ && make

scrape: 
	cd $@ && make
# done.
