# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

input := ../filter/output/complaints.parquet
output := output/complaints.parquet

.PHONY: all clean

all: $(output)

clean:
	-rm -r output/*

$(output): src/clean.py $(input)
	-mkdir output
	python $< \
		--input=$(input) \
		--output=$@
