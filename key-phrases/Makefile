# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     TS
# Maintainers: TS
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

.PHONY: all clean

complaints := ../filter/output/complaints.parquet

model := output/models/english-ewt-ud-2.5-191206.udpipe
annotations := output/annotated-complaints.parquet
raked_keyphrases := output/raked-keyphrases.parquet

all: $(raked_keyphrases)

clean: 
	-rm -r output/*

$(raked_keyphrases): src/rake.R $(annotations)
	Rscript --vanilla $< \
		--annotations=$(annotations) \
		--output=$@

$(annotations): src/annotate.R $(complaints) $(model)
	-mkdir output
	Rscript --vanilla $< \
		--complaints=$(complaints) \
		--model=$(model) \
		--output=$@

$(model): src/download-language-model.R
	-mkdir -p $(shell dirname $@)
	Rscript --vanilla $< --model_dir=$(shell dirname $@)

# done.
