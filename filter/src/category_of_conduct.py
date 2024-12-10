#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/conduct-category.py

# ---- dependencies {{{
from os import listdir
from pathlib import Path
from sys import stdout
import argparse
import logging
import yaml
import re
import pandas as pd
#}}}

# ---- support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="output/pages.parquet")
    parser.add_argument("--hand", default="hand/category.yml")
    parser.add_argument("--output", default="output/conduct-category.parquet")
    args = parser.parse_args()
    assert Path(args.input).exists()
    assert Path(args.hand).exists()
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


def readyaml(fname):
    with open(fname, 'r') as f:
        rules = yaml.safe_load(f)
    return rules


def find_cat(line):
    if not line: return None
    if "category of conduct" not in line.lower(): return None
    found = re.findall("category of conduct\\s*:([a-z\\s]+)finding", line, flags=re.I)
    if (not found) | (found == []): return None
    if not found[0].strip(): return None
    return found[0].strip()


def resolve_cat(x):
    if not x: return None
    found = []
    for label, options in rules.items():
        full = options + [label]
        if x in full: found.append(label)
    if not found: return f'Unknown category: {x}'
    return "|".join(found)
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/conduct-category.log")

    # arg handling
    args = get_args()
    rules = readyaml(args.hand)

    logger.info('loading data')
    allegs = pd.read_parquet(args.input, \
                                 columns=['allegation_id', 'allegation_text'])
    logger.info('adding CATEGORY OF CONDUCT as category_of_conduct')
    allegs['category_of_conduct'] = allegs.allegation_text.apply(find_cat)
    allegs['category_found'] = allegs.category_of_conduct.notna()
    allegs['conduct_category'] = allegs.category_of_conduct.apply(resolve_cat)
    assert not (allegs.category_found & allegs.conduct_category.isna()).any()
    allegs.drop(columns=['allegation_text', 'category_of_conduct'], inplace=True)
    allegs.to_parquet(args.output)
    logger.info("done.")

#}}}
# done.
