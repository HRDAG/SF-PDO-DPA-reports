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
import time
import argparse
import logging
import re
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
    """
    At the initial creation of this method, roughly 96% of the dates were 'easy' and well-formatted this way
    """
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
    """
    Fixes a specific OUT-OF-BOUNDS date value
    - did it cause an issue with `pd.to_datetime()`?
    - should this be deprecated in favor of just `drop_impossible()?`
    """
    assert 'date_completed_text' in df.columns
    copy = df.copy()
    if any(copy.date_completed_text == '05/26/26'):
        assert len(copy.loc[copy.date_completed_text == '05/26/26', 'pdf_url'].unique()) == 1
        assert copy.loc[copy.date_completed_text == '05/26/26', 'pdf_url'].unique()[0] == \
        'https://wayback.archive-it.org/org-571/20230120175459/https://sfgov.org/dpa/ftp/uploadedfiles/occ/OCC_05_06_openness.pdf'
        copy.loc[copy.date_completed_text == '05/26/26', 'date_completed_text'] = '05/26/06'
    return copy


def format_datetimecols(df):
    """
    Formats `date_complained` and `date_completed` as Datetime objects.
    """
    copy = df.copy()
    copy.rename(columns={'date_complained': 'date_complained_text',
                         'date_completed': 'date_completed_text'}, inplace=True)
    for field_text in ('date_complained_text', 'date_completed_text'):
        field = field_text[:field_text.find('_text')]
        copy[field_text] = copy[field_text].str.replace(' ', '')
        copy = fix_oob_dates(copy)
        # convert 'easy' vals to datetime
        copy[field] = copy[field_text].apply(easy_date)
        copy[field] = pd.to_datetime(copy[field],
                                     dayfirst=False, yearfirst=False,
                                     format="mixed")
    return copy


def drop_impossible(df, field):
    """
    Date values later than {today} are OUT-OF-BOUNDS and not possible as correct entries.
    Convert OOB values to None.
    """
    copy = df.copy()
    today = pd.to_datetime(time.strftime("%Y/%m/%d"))
    copy.loc[copy[field] > today, field] = None
    return copy


def recover_complaintdates(df, field):
    """
    In the original reports, an entry for a given complaint uses the same
    `date_complained` and `date_completed` for all pages about that complaint.
    Therefore, these values can be borrowed from other pages to correct potential data entry errors.
    Replace the field values for a given complain id with the value that occurs the most.
    """
    assert field in df.columns
    copy = df.copy()
    n_unique_byid = copy[['complaint_id', field]
            ].groupby('complaint_id')[field].apply(lambda x: len(x.unique()))
    fix_ids = n_unique_byid.loc[n_unique_byid > 1].index.values
    print(f"correcting {len(fix_ids)} complaint_ids with broken {field} info")
    for compid in fix_ids:
        top_date = copy.loc[copy.complaint_id == compid, field].value_counts().head(1).index[0]
        copy.loc[copy.complaint_id == compid, field] = top_date
    copy['n_unique'] = copy[['complaint_id', field]
        ].groupby('complaint_id')[field].apply(lambda x: len(x.unique()))
    assert not (copy.n_unique > 1).any()
    copy.drop(columns='n_unique', inplace=True)
    return copy


def verify_datecols(df):
    logger.info('verifying formatted results')
    assert df.date_complained.notna().sum() > (complaints.shape[0] / 2)
    complain_rate = df.date_complained.notna().sum() / complaints.shape[0]
    complete_rate = df.date_completed.notna().sum() / complaints.shape[0]
    assert complain_rate >= .8 <= complete_rate
    logger.info(f'conversion rate (date_complained):\t{complain_rate}')
    logger.info(df.date_complained.describe())
    logger.info(f'conversion rate (date_completed):\t{complete_rate}')
    logger.info(df.date_completed.describe())
    return 1


def add_partial_datecols(df):
    copy = df.copy()
    copy['year_complained'] = copy.date_complained.dt.year
    copy['month_complained'] = copy.date_complained.dt.month
    copy['year_completed'] = copy.date_completed.dt.year
    copy['month_completed'] = copy.date_completed.dt.month
    return copy


def recover_filedates(df):
    """
    Reports are completed and published once a month.
    So, every report file should share a year and month completed.
    """
    copy = df.copy()
    fileids = copy.fileid.unique()
    for fileid in fileids:
        topyear = copy.loc[copy.fileid == fileid, 'year_completed'].value_counts().head(1).index[0]
        copy.loc[copy.fileid == fileid, 'year_completed'] = topyear
        topmonth = copy.loc[copy.fileid == fileid, 'month_completed'].value_counts().head(1).index[0]
        copy.loc[copy.fileid == fileid, 'month_completed'] = topmonth
    return copy


def find_cut_prefix(alleg):
    """
    The `allegations` field is derived from the 'SUMMARY OF ALLEGATIONS: ' line.
    When a complaint includes more than 1 allegation, this line is prefixed with a number
    that will add noise to otherwise identical allegations.
    Additionally, some allegation summaries are actually very long,
    though the full text remains available in the metadata field.

    Note that this field is not the same as the `findings_of_fact` data which includes a
    full description of the allegation by the complainant.
    """
    if pd.isna(alleg): return None
    patt1 = "[0-9]+[" + re.escape(":- /") + "]+"
    patt2 = "[0-9]+[a-z" + re.escape("-& ") + "]+" + "[0-9]+[" + re.escape(":- /") + "]+"
    found2 = re.findall(patt2, alleg)
    if any(found2):
        for ea in found2: alleg = alleg.replace(ea, '')
    else:
        found1 = re.findall(patt1, alleg)
        if any(found1):
            for ea in found1: alleg = alleg.replace(ea, '')
    if len(alleg) > 200:
        alleg = alleg[:alleg.find("CATEGORY")]
        if len(alleg) > 200: alleg = alleg[:200]
    if pd.isna(alleg.strip()): return None
    return alleg.strip()
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

    logger.info('cleaning and applying minor formatting')
    complaints.allegations = complaints.allegations.apply(find_cut_prefix).replace("", None)
    complaints = format_datetimecols(complaints)
    datecols = ('date_complained', 'date_completed')
    for datecol in datecols:
        complaints = drop_impossible(df=complaints, field=datecol)
        complaints = recover_complaintdates(df=complaints, field=datecol)
    assert verify_datecols(complaints)
    complaints = add_partial_datecols(df=complaints)
    complaints = recover_filedates(df=complaints)

    logger.info(f'writing table with {complaints.shape[0]} rows, {complaints.shape[1]} columns')
    complaints.to_parquet(args.output)
# }}}
# done
