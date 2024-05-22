# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

input := ../clean/output/complaints.parquet
dpacat := hand/dpa_allegation_category.yml
newcat := hand/test_allegation_category.yml
sust := hand/sustained.yml
finds := hand/findings.yml
output := output/complaints.parquet

.PHONY: all clean

all: $(output)

clean:
	-rm -r output/*

$(output): src/indicate.py $(input) $(newcat)
	-mkdir output
	python $< \
		--input=$(input) \
		--dpacat=$(dpacat) \
		--newcat=$(newcat) \
		--sust=$(sust) \
		--finds=$(finds) \
		--output=$@
