#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/findings-of-fact.py

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
    parser.add_argument("--output", default="output/findings_of_fact.parquet")
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


def find_facts(line):
    if not line: return None
    if "fact" not in line.lower(): return None
    found = re.findall("dept[.]*\saction:*\sfindings\sof\sfact:*\s*([a-zA-Z0-9\s\W\D]+)summary", line, flags=re.I)
    if not found:
        found = re.findall("\sfindings\sof\sfact:*\s*([a-zA-Z0-9\s\W\D]+)(?:summary|$)", line, flags=re.I)
    if not found: 
        found = [val[val.find('FINDINGS OF FACT: ') + 18:] 
                 for val in line.split("SUMMARY OF ALLEGATION")[1:]]
    if (not found) | (found == []): return None
    return found
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/findings-of-fact.log")

    # arg handling
    args = get_args()
    
    logger.info('loading data')
    allegs = pd.read_parquet(args.input, columns=['allegation_id', 'allegation_text'])
    logger.info('adding FINDINGS OF FACT as findings_of_fact')
    allegs['findings_of_fact'] = allegs.allegation_text.apply(find_facts)
    allegs = allegs.explode('findings_of_fact')
    miss_n = allegs.findings_of_fact.isna().sum()
    logger.info(f'missing findings_of_fact (count):\t{miss_n}')
    #assert not any(allegs.findings_of_fact.isna())
    allegs.drop(columns='allegation_text', inplace=True)
    allegs.to_parquet(args.output)
    
    logger.info("done.")
    
#}}}
# done.
