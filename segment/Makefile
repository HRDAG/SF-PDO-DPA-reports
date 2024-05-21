# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     TS
# Maintainers: TS
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

txts := $(shell find ../pdf2text/output/txt-test -iname '*.txt')
pqs  := $(patsubst ../pdf2text/output/txt-test/%.txt,output/each/%.parquet,$(txts))

.PHONY: all clean

all: output/reference-table.parquet

clean: 
	-rm -r output/*

output/reference-table.parquet: src/ref-table.py $(pqs)
	python $< \
		--inputdir=output/each \
		--index=../scrape/output/reference-table.parquet \
		--hand=hand/blanks.yml \
		--output=$@

output/each/%.parquet: src/parse-sections.py ../pdf2text/output/txt-test/%.txt
	-mkdir -p $(shell dirname $@)
	python $< \
		--input=../pdf2text/output/txt-test/$*.txt \
		--output=$@
	
# done.
