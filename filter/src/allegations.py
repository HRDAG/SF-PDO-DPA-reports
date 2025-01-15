#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/allegations.py

# ---- dependencies {{{
from os import listdir
from pathlib import Path
from sys import stdout
import argparse
import logging
import re
import pandas as pd
#}}}

# ---- support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="output/pages.parquet")
    parser.add_argument("--output", default="output/allegations.parquet")
    args = parser.parse_args()
    assert Path(args.input).exists()
    return args


def get_logger(sname, file_name=None):
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


def find_allegs(line):
    if not line: return None
    if all([kw in line.lower() for kw in ('complaint date', 'completion date', 'page')]):
        # line is metadata, not allegations
        return None
    patts = ("s[u]*mmary[a-z\\s\\-]*allegation[s\\s:#-]*([a-zA-Z0-9\\s\\W\\D]+)category",
             "s[u]*mmary\\s[a-z\\s@\\-]*allegation[s\\s:#-]*([a-zA-Z0-9\\s\\W\\D]+)category",
             "s[u]*mmary[a-z\\s\\-]*allegation[s\\s:#-]*([a-zA-Z\\s\\n\\W\\D'\\(\\)\\.]+.*)category")
    found = []
    for patt in patts:
        if not found: found = re.findall(patt, line, flags=re.I) # line.replace("-", "@")?
    if not found: return None
    narrowed = [item[:item.find('CATEGORY OF CONDUCT')] if 'CATEGORY OF CONDUCT' in item else item
                for item in found]
    return narrowed
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/allegations.log")

    # arg handling
    args = get_args()
    logger.info('loading data')
    allegs = pd.read_parquet(args.input, \
                                 columns=['allegation_id', 'allegation_text'])
    allegs = allegs.drop_duplicates().dropna(how='all')
    logger.info('adding SUMMARY OF ALLEGATION(S) as allegations')
    allegs['allegations'] = allegs.allegation_text.apply(find_allegs)
    allegs = allegs.explode('allegations')
    allegs = allegs.dropna(subset=['allegations'])
    assert not any(allegs.allegations.isna()), f"\
        {allegs.allegations.isna().sum()} rows missing `allegations`"
    allegs.allegations.sample(5)
    allegs.allegations = allegs.allegations.str.replace('@', '-', regex=False)
    allegs.drop(columns='allegation_text', inplace=True)
    allegs.to_parquet(args.output)

    logger.info("done.")

#}}}
# done.
