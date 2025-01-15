#!/usr/bin/env python3
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

# dependencies {{{
from pathlib import Path
from sys import stdout
import argparse
import logging
import yaml
import re
import pandas as pd
# }}}

# support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--dpacat", default=None)
    parser.add_argument("--newcat", default=None)
    parser.add_argument("--sust", default=None)
    parser.add_argument("--finds", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    assert Path(args.input).exists()
    assert Path(args.dpacat).exists()
    assert Path(args.newcat).exists()
    assert Path(args.sust).exists()
    assert Path(args.finds).exists()
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
        data = yaml.safe_load(f)
    return data


def apply_dpafindgroup(df, rules):
    copy = df.copy()
    copy.rename(columns={'finding': 'finding_original'}, inplace=True)
    copy['finding'] = copy.finding_original
    for label, catlist in rules.items():
        formcats = [cat.lower() for cat in catlist]
        copy.loc[(
            copy.finding.str.lower().isin(formcats)) | (
            copy.finding.str.lower() == label.lower()),
        'finding'] = label
    return copy


def apply_dpacatgroup(df, rules):
    assert 'conduct_category' in df.columns, f"`conduct_category` not found in {df.columns}"
    copy = df.copy()
    copy.rename(columns={'conduct_category': 'conduct_category_original'}, inplace=True)
    copy['conduct_category'] = copy.conduct_category_original
    for label, catlist in rules.items():
        formcats = [cat.lower() for cat in catlist]
        copy.loc[(
            copy.conduct_category.str.lower().isin(formcats)) | (
            copy.conduct_category.str.lower() == label.lower()),
        'conduct_category'] = label
    return copy


def apply_newcatgroup(df, rules):
    copy = df.copy()
    colsadded = []
    for colname, patt in rules.items():
        copy[colname] = copy.allegations.str.contains(patt, na=False, flags=re.I)
        colsadded.append(colname)
    colstr = ", ".join(colsadded)
    logger.info(f'added indicator columns:\t{colstr}')
    return copy


def group_td(x):
    if pd.isna(x): return None
    if x < pd.to_timedelta(0, unit="s"): return "NEGATIVE"
    elif x < pd.to_timedelta(30, unit="d"): return "Less than 1 month"
    elif x < pd.to_timedelta(90, unit="d"): return "1 to 3 months"
    elif x < pd.to_timedelta(180, unit="d"): return "3 to 6 months"
    elif x < pd.to_timedelta(365, unit="d"): return "6 months to 1 year"
    elif x < pd.to_timedelta(365*2, unit="d"): return "1 to 2 years"
    else: return "Over 2 years"


def set_indicators(df, rules):
    copy = df.copy()
    copy['mediated'] = copy.finding == "Mediated"
    copy['sustained'] = copy.finding.str.lower().isin(rules)
    copy['time_to_complete'] = copy.date_completed - complaints.date_complained
    copy['ttc_group'] = copy.time_to_complete.apply(group_td)
    assert len(copy.ttc_group.value_counts()) > 2
    logger.info(copy.ttc_group.value_counts())
    return copy
# }}}

# main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/indicate.log")

    # arg handling
    args = get_args()

    logger.info('loading data')
    complaints = pd.read_parquet(args.input)
    logger.info(f'read in {complaints.shape[0]} rows')
    dpacat_rules = readyaml(args.dpacat)
    newcat_rules = {k: "|".join(vlist) for k,vlist in readyaml(args.newcat).items()}
    sust_rules = readyaml(args.sust)
    finds_rules = {k: [re.escape(v) for v in vlist]
                   for k,vlist in readyaml(args.finds).items()}

    complaints = apply_dpafindgroup(df=complaints, rules=finds_rules)
    complaints = apply_dpacatgroup(df=complaints, rules=dpacat_rules)
    complaints = apply_newcatgroup(df=complaints, rules=newcat_rules)
    complaints = set_indicators(df=complaints, rules=sust_rules)

    logger.info(f'writing table with {complaints.shape[0]} rows, {complaints.shape[1]} columns')
    complaints.to_parquet(args.output)
# }}}
# done
