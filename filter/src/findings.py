#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/findings.py

# ---- dependencies {{{
from os import listdir
from pathlib import Path
from sys import stdout
import argparse
import logging
import re
import yaml
import pandas as pd
#}}}

# ---- support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="output/pages.parquet")
    parser.add_argument("--hand", default="hand/sustained.yml")
    parser.add_argument("--output", default="output/finding.parquet")
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


def read_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f)
        f.close()
    return data.split()


def find_outcomes(line):
    if not line: return None
    if "finding" not in line.lower(): return None
    found = re.findall("finding:*([a-z/\)\(\s]*)dept|finding:*([a-z/\s\)\(]*)findings", 
                       line, flags=re.I)
    if not found: return None
    for (a,b) in found:
        if a.strip(): return a.strip()
        if b.strip(): return b.strip()
    return None


def add_sustained_col(df, sustained_kws):
    assert 'finding' in df.columns
    copy = df.copy()
    copy["sustained"] = False
    copy.loc[copy.finding.str.lower().isin(sustained_kws), 'sustained'] = True
    assert copy.sustained.sum() > 800
    return copy
#}}}
# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/findings.log")

    # arg handling
    args = get_args()

    # TODO: there used to be a lot of nan filtering/handling/reporting here
    # but since the rewrite and not using explode here, that needs to happen elsewhere
    logger.info('loading data')
    allegs = pd.read_parquet(args.input, columns=['allegation_id', 'allegation_text'])
    sustained_kws = read_yaml(args.hand)
    print(sustained_kws)
    logger.info('adding FINDING as finding')
    allegs['finding'] = allegs.allegation_text.apply(find_outcomes)
    allegs = allegs.explode('finding')
    rate_miss = round(allegs.finding.isna().sum() / allegs.shape[0], 2)
    logger.info(f'missing finding (prop):\t{rate_miss}')
    logger.info('adding SUSTAINED allegation indicator')
    allegs = add_sustained_col(allegs, sustained_kws)
    logger.info(f'SUSTAINED allegations (count):\t {allegs.sustained.sum()}')
    rate_sus = round(allegs.sustained.sum() / allegs.shape[0], 2)
    logger.info(f'SUSTAINED allegations (prop):\t{rate_sus}')
    allegs.drop_duplicates(inplace=True)
    vc = allegs.finding.fillna("MISSING").value_counts()
    for k,v in vc.items(): logger.info(f'{k}:\t{v}')
    allegs.drop(columns='allegation_text', inplace=True)
    allegs.to_parquet(args.output)

    logger.info("done.")

#}}}
# done.
