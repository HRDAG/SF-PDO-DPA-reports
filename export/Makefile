# vim: set ts=4 sts=0 sw=4 si fenc=utf-8
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:#
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/export/Makefile

# ---- dependencies {{{
complaints := ../indicate/output/complaints.parquet
output := output/complaints.xlsx
useful := ../filter/output/useful.yml
# }}}

# ---- standard {{{
.PHONY: clean all

all: $(output) output/useful.done

clean:
	-rm -r output/*
# }}}

# ---- task-specific {{{
output/useful.done: $(useful)
	-cd output && ln -s ../$(useful) .
	touch $@

$(output): \
		src/to_xlsx.py \
		$(complaints)
	-mkdir output
	python $< \
		--input=$(complaints) \
		--output=$@
# }}}

# done.
