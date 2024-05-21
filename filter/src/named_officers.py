#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/named_officers.py

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
    parser.add_argument("--output", default="output/named-officers.parquet")
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


def find_officer_names(line):
    pattern = re.compile("(OFFICER\s[A-Z]+\s[A-Z]+\s[#][0-9]{4,})", flags=re.I|re.M)
    found = re.findall(pattern, line)
    if (not found) | (found == []): return None
    return found
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/named_officers.log")

    # arg handling
    args = get_args()
    
    logger.info('loading data')
    allegs = pd.read_parquet(args.input, columns=['allegation_id', 'allegation_text'])
    logger.info('adding NAMED OFFICERS extraction as named_officers')
    allegs['named_officers'] = allegs.allegation_text.apply(find_officer_names)
    logger.info(f'OFFICERS identified (count):\t{allegs.named_officers.notna().sum()}')
    vc = allegs.named_officers.value_counts()
    for k,v in vc.items(): logger.info(f'{k}:\t{v}')
    allegs.drop(columns='allegation_text', inplace=True)
    allegs.to_parquet(args.output)
    logger.info("done.")
    
#}}}

# done.
