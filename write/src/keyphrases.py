#!/usr/bin/env pytho3
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2024, HRDAG, GPL v2 or later
# =========================================

# ---- dependencies {{{
from pathlib import Path
from sys import stdout
import argparse
import logging
import pandas as pd
import pke
#}}}

# --- support methods --- {{{
def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="../export/output/complaints.parquet")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    assert Path(args.input).exists()
    return args


def getlogger(sname, file_name=None):
    logger = logging.getLogger(sname)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s " +
                                  "- %(message)s", datefmt='%Y-%m-%d %H:%M:%S')
    stream_handler = logging.StreamHandler(stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if file_name:
        file_handler = logging.FileHandler(file_name)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    return logger


def setupbatches(df):
    assert 'allegations' in df.columns
    hasallegs = df.loc[df.allegations.notna()]
    nrows = hasallegs.shape[0]
    batches = []
    for i in range(0, nrows, 900):
        if i + 900 > nrows: end = nrows
        else: end = i + 900
        newbatch = "\n".join(hasallegs.iloc[i:end].allegations.values)
        batches.append(newbatch)
    return batches


def getextractor():
    ext = pke.unsupervised.TopicRank()
    return ext


def processdoc(extractor, doc, n_phrases=10):
    logger.info('loading document')
    extractor.load_document(input=doc, language='en')
    logger.info('selecting candidates')
    extractor.candidate_selection()
    logger.info('weighting candidates')
    extractor.candidate_weighting()
    logger.info(f'preparing {n_phrases} best keyphrases')
    keyphrases = extractor.get_n_best(n=n_phrases)
    return keyphrases
# }}}

# --- main --- {{{
if __name__ == '__main__':
    args = getargs()
    logger = getlogger(__name__, "output/keyphrases.log")

    complaints = pd.read_parquet(args.input)
    batches = setupbatches(complaints)
    extractor = getextractor()
    keyphrases = processdoc(extractor=extractor, doc=batches[10], n_phrases=20)
    print(keyphrases)

    logger.info('done')
# }}}
