# vim: set ts=4 sts=0 sw=4 si fenc=utf-8
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:#
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/scrape/Makefile

# ---- dependencies {{{
dpa_link := https://sf.gov/information/reports-policing-complaints
users := hand/useragents.yml
output := output/reference-table.parquet
# }}}

# ---- standard {{{
.PHONY: all

all: $(output)

clean: 
	-rm -r output/*

# }}}

# ---- task-specific {{{
$(output): \
		src/scrape.py
	-mkdir output output/pdfs
	python $< \
		--url=$(dpa_link) \
		--hand=$(users) \
		--output=$@
# }}}
 
# done.
