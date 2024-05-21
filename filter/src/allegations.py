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
    #if "page" not in line.lower(): return None
    found = re.findall("summary[a-z\s]*allegation[s\s:#-]*([a-zA-Z0-9\s\W\D]+)category", 
                       line, flags=re.I)
    if found: return found
    if (not found) | (found == []): 
        found = re.findall("summary\s[a-z\s@]*allegation[s\s:#-]*([a-zA-Z0-9\s\W\D]+)", 
                           line.replace("-", "@"), flags=re.I)
    return found
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/allegations.log")

    # arg handling
    args = get_args()
    
    logger.info('loading data')
    complaints = pd.read_parquet(args.input, \
                                 columns=['allegation_id', 'allegation_text'])
    logger.info('adding SUMMARY OF ALLEGATION(S) as allegations')
    complaints['allegations'] = complaints.allegation_text.apply(find_allegs)
    complaints = complaints.explode('allegations')
    assert not any(complaints.allegations.isna())
    complaints.allegations.sample(5)
    complaints.allegations = complaints.allegations.str.replace('@', '-', regex=False)
    complaints.drop(columns='allegation_text', inplace=True)
    complaints.to_parquet(args.output)

    logger.info("done.")
    
#}}}
# done.
