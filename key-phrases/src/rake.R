# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     TS
# Maintainers: TS
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    stringr,
    udpipe
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--annotations")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

cmpl <- read_parquet(args$annotations)

keyw_rake <- keywords_rake(cmpl,
                           term = "token",
                           group = c("doc_id", "paragraph_id", "sentence_id"), 
                           relevant = cmpl$upos %in% c("NOUN", "ADJ", "VERB"), 
                           ngram_max = 7, n_min = 5) %>%
    as_tibble

write_parquet(keyw_rake, args$output)

# done.
