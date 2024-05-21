# vim: set ts=8 sts=0 sw=8 si fenc=utf-8 noet:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

pdfs := $(shell find ../scrape/output/pdfs -iname '*.pdf')
txts := $(patsubst ../scrape/output/pdfs/%.pdf,output/txt-test/%.txt,$(pdfs))

ref_table := ../scrape/output/reference-table.parquet
output := output/reference-table.parquet

.PHONY: all clean

all: $(txts) $(output)

clean:
	-rm -r output/*

output/txt-test/%.txt: ../scrape/output/pdfs/%.pdf
	-mkdir -p output/txt-test
	pdftotext -layout $< $@

$(output): \
	src/update-ref-table.py \
	$(ref_table)
	python $< \
		--input=$(ref_table) \
		--output=$@

# done.
