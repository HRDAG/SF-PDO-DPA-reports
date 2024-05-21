#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/export/src/to_xlsx.py

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
    parser.add_argument("--input", default="../filter/output/complaints.parquet")
    parser.add_argument("--output", default="output/complaints.xlsx")
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


def read_yaml(fname):
    with open(fname, 'r') as f_handle:
        out = yaml.safe_load(f_handle)
    return out


def get_notes():
    notes = [\
        ("column_name", "notes"),\
        ("jlp", "'justified, lawful, and proper' in `allegation_text`"),\
        ("default_finding", "'insufficient evidence to prove or disprove' in `allegation_text`"),\
        ("intimidation", "'threatening, intimidating or harassing behavior' in `allegation_text`"),\
        ("suggest_review", "`finding` not identified, nor any of the above phrases")\
        ]
    return pd.DataFrame(notes[1:], columns=notes[0])


def get_real_cols():
    return [\
    'complaint_id', 'allegation_id', \
    'date_complained', 'date_completed', 'year_complained', 'year_completed',\
    'time_to_complete', 'ttc_group',\
    'report_type', 'n_complaint_pages',\
    'dpa_added', 'occ_added',\
    'allegations', 'findings_of_fact',\
    'category_of_conduct', 'finding', 'sustained', 'mediated', 'mediation_status', \
    'complaint_meta',\
    'allegation_text', \
    'pdf_url'
]


def get_kw_cols():
    return [ \
            'allegation_id',\
            'named_officers', 'no_officer_id',\
            'default_finding', 'jlp',\
            'resisting', 'force', 'bwc', \
            'intimidation', 'racial_bias', \
            'pursuit', 'swat', 'firearm', 'taser', \
            'home', 'minor', 'crisis', 'missing_person', \
            'pdf_url'
    ]
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/to_xlsx.log")

    # arg handling
    args = get_args()
    complaints = pd.read_parquet(args.input)
    initial = complaints.shape[0]
    logger.info(f'initial shape:\t{complaints.shape[0]}')
    complaints = complaints.loc[complaints.outside_jurisdiction == False]
    logger.info(f'after filtering OOB:\t{complaints.shape[0]}')
    sustained = complaints.loc[complaints.sustained == 1]
    added = complaints.loc[(complaints.dpa_added) | (complaints.occ_added)]
    mediated = complaints.loc[complaints.mediation_status.notna()]
    officers = complaints.loc[complaints.named_officers.notna()]
    notes = get_notes()
    colorder = get_real_cols()
    kws = get_kw_cols()
    with pd.ExcelWriter(args.output) as writer:
        complaints[colorder].to_excel(writer, sheet_name='data')
        sustained[colorder].to_excel(writer, sheet_name='sustained')
        added[colorder].to_excel(writer, sheet_name='added')
        mediated[colorder].to_excel(writer, sheet_name='mediation')
        officers[colorder].to_excel(writer, sheet_name='named_officers')
        complaints[kws].to_excel(writer, sheet_name='keywords')
        notes.to_excel(writer, sheet_name='notes')
    complaints.to_parquet("output/complaints.parquet")
    
    logger.info("done.")
    
#}}}
# done.
