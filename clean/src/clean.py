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
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import pandas as pd
# }}}

# support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
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


def easy_date(line):
    if pd.isna(line): return None
    if '/' not in line: return None
    if len(line.split('/')) != 3: return None
    chunks = [chunk.strip() for chunk in line.split('/') 
              if (chunk.strip() not in ('', '0')) & (chunk.strip().isdigit())]
    if len(chunks) < 3: return None
    if (len(chunks[0]) > 2) | (len(chunks[1]) > 2) | (len(chunks[2]) % 2 != 0): return None
    if not ((1 <= int(chunks[0]) <= 12) & (1 <= int(chunks[1]) <= 31)): return None
    return '/'.join(chunks)


# there are two known allegation records from 2006 with a typo in the year
# pdf_url: https://wayback.archive-it.org/org-571/20230120175459/https://sfgov.org/dpa/ftp/uploadedfiles/occ/OCC_05_06_openness.pdf
def fix_oob_dates(df):
    assert 'date_completed_text' in df.columns
    copy = df.copy()
    if any(copy.date_completed_text == '05/26/26'):
        assert len(copy.loc[copy.date_completed_text == '05/26/26', 'pdf_url'].unique()) == 1
        assert copy.loc[copy.date_completed_text == '05/26/26', 'pdf_url'].unique()[0] == \
        'https://wayback.archive-it.org/org-571/20230120175459/https://sfgov.org/dpa/ftp/uploadedfiles/occ/OCC_05_06_openness.pdf'
        copy.loc[copy.date_completed_text == '05/26/26', 'date_completed_text'] = '05/26/06'
    return copy


def group_td(x):
    if pd.isna(x): return None
    if x < pd.to_timedelta(0, unit="s"): return "NEGATIVE"
    elif x < pd.to_timedelta(30, unit="d"): return "Under 1 month"
    elif x < pd.to_timedelta(90, unit="d"): return "Under 3 months"
    elif x < pd.to_timedelta(180, unit="d"): return "Under 6 months"
    elif x < pd.to_timedelta(365, unit="d"): return "Under 1 year"
    elif x < pd.to_timedelta(365*2, unit="d"): return "Under 2 years"
    else: return "Over 2 years"
# }}}

# main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/clean.log")

    # arg handling
    args = get_args()

    logger.info('loading data')
    complaints = pd.read_parquet(args.input)
    logger.info(f'read in {complaints.shape[0]} rows')

    logger.info('prepping fields for formatting')
    complaints.date_complained = complaints.date_complained.str.replace(' ', '')
    complaints.date_completed = complaints.date_completed.str.replace(' ', '')
    complaints.rename(columns={'date_complained': 'date_complained_text', \
                               'date_completed': 'date_completed_text'}, inplace=True)
    # remove illegal characters prior to exporting to excel
    complaints = complaints.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)

    logger.info('fixing known typos with OUT-OF-BOUNDS dates')
    complaints = fix_oob_dates(complaints)
    # convert 'easy' vals to datetime 
    logger.info('applying formatting')
    complaints['date_complained'] = complaints.date_complained_text.apply(easy_date)
    complaints.date_complained = pd.to_datetime(complaints.date_complained, 
                                                     dayfirst=False, yearfirst=False, 
                                                     format="mixed")
    complaints['date_completed'] = complaints.date_completed_text.apply(easy_date)
    complaints.date_completed = pd.to_datetime(complaints.date_completed, 
                                                     dayfirst=False, yearfirst=False, 
                                                     format="mixed")
    
    # during the initial creation of this method, roughly 96% of the dates were 'easy' and well-formatted this way
    logger.info('verifying formatted results')
    assert complaints.date_complained.notna().sum() > (complaints.shape[0] / 2)
    complain_rate = complaints.date_complained.notna().sum() / complaints.shape[0]
    complete_rate = complaints.date_completed.notna().sum() / complaints.shape[0]
    logger.info(f'conversion rate (date_complained):\t{complain_rate}')
    logger.info(complaints.date_complained.describe())
    logger.info(f'conversion rate (date_completed):\t{complete_rate}')
    logger.info(complaints.date_completed.describe())

    logger.info('creating new fields `year_completed`, `time_to_complete`, `ttc_group`, etc')
    complaints['mediated'] = complaints.finding == "M"
    complaints['year_complained'] = complaints.date_complained.dt.year
    complaints['year_completed'] = complaints.date_completed.dt.year
    complaints['time_to_complete'] = complaints.date_completed - complaints.date_complained
    complaints['ttc_group'] = complaints.time_to_complete.apply(group_td)
    assert len(complaints.ttc_group.value_counts()) > 2
    logger.info(complaints.ttc_group.value_counts())
    
    logger.info(f'writing table with {complaints.shape[0]} rows, {complaints.shape[1]} columns')
    complaints.to_parquet(args.output)
# }}}
# done
