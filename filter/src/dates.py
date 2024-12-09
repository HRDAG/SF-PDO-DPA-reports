#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/dates.py

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
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default="output/dates.parquet")
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


def find_date_complained(line):
    if not line: return None
    clean = line.replace("-", "/")
    complained = re.findall("[date\\sof]*co[\\s]*mplain[date:\\s]*[a-z0-9\\s.,]*:[\\s]*([0-9]+\\s*\\/[0-9]+\\/*\\s*[0-9]+)", clean, flags=re.I)
    if not complained: return None
    return complained[0]


def find_date_completed(line):
    if not line: return None
    clean = line.replace("-", "/")
    completed = re.findall("[date\\sofc]*omplet[ion\\sdate:]*[a-z0-9\\s.,]*:[\\s]*([0-9]+[\\s\\/;]*[0-9]+[\\s\\/]*[0-9]+)", clean, flags=re.I)
    if not completed: return None
    return completed[0]


def find_mediation_info(line):
    if not line: return None
    mediated = re.findall("(mediated\\s*[a-z0-9\\-,\\s/]*resolved.*$)", line, flags=re.I)
    if not mediated: return None
    return mediated
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/dates.log")

    # arg handling
    args = get_args()
    logger.info('loading data')
    allegs = pd.read_parquet(args.input, columns=['allegation_id', 'complaint_meta', 'allegation_text'])
    logger.info('adding DATE OF COMPLAINT as date_complained')
    allegs['date_complained'] = allegs.complaint_meta.apply(
        find_date_complained)
    allegs.date_complained.sample(5)
    logger.info('adding DATE OF COMPLETION as date_completed')
    allegs['date_completed'] = allegs.complaint_meta.apply(
        find_date_completed)
    allegs.date_completed.sample(5)
    logger.info('adding mediation info as mediation_status')
    allegs['mediation_status'] = allegs.allegation_text.apply(
        find_mediation_info)
    allegs.mediation_status.sample(5)

    miss_complain = allegs.date_complained.isna().sum()
    miss_complete = allegs.date_completed.isna().sum()
    logger.info(f'missing date_complained (count):\t{miss_complain}')
    logger.info(f'missing date_completed (count):\t{miss_complete}')
    assert allegs.date_complained.isna().sum() < (allegs.shape[0]*.5)
    assert allegs.mediation_status.notna().sum() > 400
    allegs.drop(columns=['allegation_text', 'complaint_meta'], inplace=True)
    allegs.to_parquet(args.output)
    logger.info("done.")

#}}}
# done.
