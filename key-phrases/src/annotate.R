# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    arrow,
    dplyr,
    udpipe
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--complaints")
parser$add_argument("--model")
parser$add_argument("--output")
args <- parser$parse_args()
# }}}

dpa <- read_parquet(args$complaints)

ud_model <- udpipe_load_model(args$model)

tagged <- udpipe_annotate(ud_model,
                          x = dpa$findings_of_fact,
                          doc_id = dpa$allegation_id)

out <- as.data.frame(tagged) %>% as_tibble

write_parquet(out, args$output)

# done.
