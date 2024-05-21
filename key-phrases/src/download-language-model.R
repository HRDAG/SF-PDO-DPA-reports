# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:

# load libs {{{
pacman::p_load(
    argparse,
    udpipe
)
# }}}

# args {{{
parser <- ArgumentParser()
parser$add_argument("--model_dir",
                    default = "output/models")
args <- parser$parse_args()
# }}}

# could use `overwrite=FALSE` here but relying on the makefile structure to
# handle that
udpipe_download_model(language = "english", model_dir = args$model_dir)

# done.
