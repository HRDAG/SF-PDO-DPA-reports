# vim: set ts=4 sts=0 sw=4 si fenc=utf-8
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:#
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/Makefile

# ---- dependencies {{{
txtdir := ../pdf2text/output/txt
ref_table := ../segment/output/reference-table.parquet
complaints := output/complaints.parquet

updated := output/reference-table.parquet
allegations := output/allegations.parquet
category_of_conduct := output/category_of_conduct.parquet
dates := output/dates.parquet
findings := output/findings.parquet
findings_of_fact := output/findings_of_fact.parquet
named_officers := output/named_officers.parquet
# }}}

# ---- standard {{{
.PHONY: clean all

all: $(first_pages) $(dates) $(complaints)

clean: 
	-rm -r output/*
# }}}

# ---- task-specific {{{
output/%.parquet: src/%.py $(ref_table)
	python $< \
		--input=$(ref_table) \
		--output=$@

$(complaints): \
		src/merge.py \
		$(allegations) \
		$(category_of_conduct) \
		$(dates) \
		$(findings) \
		$(findings_of_fact) \
		$(named_officers) \
		$(ref_table)
	python $< \
		--allegations=$(allegations) \
		--category_of_conduct=$(category_of_conduct) \
		--dates=$(dates) \
		--findings=$(findings) \
		--findings_of_fact=$(findings_of_fact) \
		--named_officers=$(named_officers) \
		--ref=$(ref_table) \
		--output=$@
# }}}
 
# done.
