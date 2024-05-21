#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/pdf2text/src/update-ref-table.py

# ---- dependencies {{{
import os
from pathlib import Path
from sys import stdout
import argparse
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
#}}}

# ---- support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="../scrape/output/reference-table.parquet")
    parser.add_argument("--output", default="output/reference-table.parquet")
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


def guess_check_txt(filename):
    if pd.isna(filename): return None
    txtfile = filename.replace('pdfs', 'txt').replace('.pdf', '.txt')
    if not Path(txtfile).exists():
        if '.pdf.pdf' in filename:
            try_file = filename.replace('pdfs', 'txt')[:-4] + '.txt'
            if (Path(try_file).exists()): return try_file
            else: return 'not found'
    return txtfile
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/update-ref-table.log")

    # arg handling
    args = get_args()
    
    # read data, initial verification
    logger.info("Loading data.")
    ref_table = pd.read_parquet(args.input)
    assert ('filename' in ref_table.columns) | ('pdf_file' in ref_table.columns)
    if 'filename' in ref_table.columns:
        ref_table.rename(columns={'filename': 'pdf_file'}, inplace=True)
    ref_table['txtfile'] = ref_table.pdf_file.apply(guess_check_txt)
    assert not any(ref_table.txtfile == 'not found')
    ref_table['pdf_file'] = "../scrape/" + ref_table.pdf_file
    ref_table.to_parquet(args.output)
    logger.info("done.")
    
#}}}
# done.
