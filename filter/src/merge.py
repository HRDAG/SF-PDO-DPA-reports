#!/usr/local/bin/python
# -*- coding: utf-8 -*-
# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/filter/src/merge.py

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
    parser.add_argument("--allegations", default="output/allegations.parquet")
    parser.add_argument("--category_of_conduct", default="output/category_of_conduct.parquet")
    parser.add_argument("--dates", default="output/dates.parquet")
    parser.add_argument("--findings", default="output/findings.parquet")
    parser.add_argument("--findings_of_fact", default="output/findings_of_fact.parquet")
    parser.add_argument("--named_officers", default="output/named_officers.parquet")
    parser.add_argument("--ref", default="../segment/output/reference_table.parquet")
    parser.add_argument("--output", default="output/complaints.parquet")
    args = parser.parse_args()
    assert Path(args.allegations).exists()
    assert Path(args.category_of_conduct).exists()
    assert Path(args.dates).exists()
    assert Path(args.findings).exists()
    assert Path(args.findings_of_fact).exists()
    assert Path(args.named_officers).exists()
    assert Path(args.ref).exists()
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


def write_yaml(yaml_file, data):
    with open(yaml_file, 'w') as f:
        yaml.dump(data, f, default_flow_style=False)
        f.close()
    print(f'{yaml_file} written successfully')
    return 1


def make_meta_indic(df):
    copy = df.copy()
    copy['report_type'] = None
    copy.loc[copy.txt_file.str.contains("DPA|Openness", flags=re.I), \
             'report_type'] = 'DPA'
    copy.loc[copy.txt_file.str.contains("OCC", flags=re.I), 'report_type'] = 'OCC'
    copy['dpa_added'] = copy.allegation_text.str.contains('dpa added', \
                                                          flags=re.I) | \
    copy.allegation_text.str.contains('dpa-added', flags=re.I)
    copy['occ_added'] = copy.allegation_text.str.contains('occ added', \
                                                          flags=re.I) | \
    copy.allegation_text.str.contains('occ-added', flags=re.I)
    return copy


# phrases that suggest something about DPA's investigation
def make_proc_indic(df):
    copy = df.copy()
    copy['no_officer_id'] = copy.allegation_text.str.contains('unable to identify the officer', flags=re.I)
    copy['default_finding'] = copy.allegation_text.str.contains('insufficient evidence to prove or disprove', flags=re.I)
    copy['withdrawn'] = (copy.allegation_text.str.contains('withdraw[a-z\s]+complaint', flags=re.I)) | (copy.finding.isin(("W", "NF/W")))
    copy['intimidation'] = copy.allegation_text.str.contains('threatening, intimidating or harassing behavior', flags=re.I)
    copy['jlp'] = copy.allegation_text.str.contains('justified, lawful, and proper', flags=re.I)
    return copy


# potentials values for use of force allegation: CA P.C. 835, SFPD G.O. 5.01
def make_kw_indic(df):
    copy = df.copy()
    copy['racial_bias'] = copy.allegation_text.str.contains('racial bias', \
                                                            flags=re.I) | \
    copy.allegation_text.str.contains('biased policing', flags=re.I)
    copy['resisting'] = copy.allegation_text.str.contains('148') | \
    copy.allegation_text.str.contains('69') | \
    copy.allegation_text.str.contains('resist[a-z\s]*arrest', flags=re.I)
    copy['force'] = copy.allegation_text.str.contains('835', flags=re.I) | \
    copy.allegation_text.str.contains('5[\.]*01', flags=re.I) | \
    copy.allegation_text.str.contains('force', flags=re.I)
    copy['bwc'] = copy.allegation_text.str.contains(
    'bwc', flags=re.I) | copy.allegation_text.str.contains(
    'worn camera', flags=re.I)
    copy['pursuit'] = copy.allegation_text.str.contains('pursuit', flags=re.I)
    copy['swat'] = copy.allegation_text.str.contains('swat', flags=re.I)
    copy['firearm'] = copy.allegation_text.str.contains( 'shotgun|handgun|rifle|firearm|\sgun', flags=re.I)
    copy['taser'] = copy.allegation_text.str.contains('taser|stungun', flags=re.I)
    
    copy['home'] = copy.allegation_text.str.contains("complainant's residence|complainant's home|entered[a-z\s']* home", flags=re.I)
    copy['missing_person'] = copy.allegation_text.str.contains('missing person', flags=re.I)
    copy['minor'] = copy.allegation_text.str.contains("child|underage", flags=re.I)
    copy['crisis'] = copy.allegation_text.str.contains("health crisis", flags=re.I)
    return copy


def get_pages(line):
    if not line: return None
    if "page" not in line.lower(): return None
    found = re.findall("page[\s#]*[0-9]+[a-z\s]*([0-9]+)", line, flags=re.I)
    if (not found) | (found == []): return None
    return found[0]
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/merge.log")

    # arg handling
    args = get_args()
    
    # load data
    base = pd.read_parquet(args.ref)
    allegations = pd.read_parquet(args.allegations)
    category_of_conduct = pd.read_parquet(args.category_of_conduct)
    dates = pd.read_parquet(args.dates)
    findings = pd.read_parquet(args.findings)
    findings_of_fact = pd.read_parquet(args.findings_of_fact)
    named_officers = pd.read_parquet(args.named_officers)
    
    # need to merge page data to fields identified
    l = pd.merge(base, dates, on="allegation_id", how="outer")
    logger.info('base cols')
    logger.info(l.columns)
    for r in (allegations, category_of_conduct, 
              findings, findings_of_fact, named_officers):
        assert 'allegation_id' in r.columns
        logger.info('merging cols')
        logger.info(r.columns)
        l = pd.merge(l, r, on="allegation_id", how='outer')
        logger.info(l.columns)
    assert 'date_completed' in l.columns
    both = l.copy()
    both['n_complaint_pages'] = both.complaint_meta.apply(get_pages)
    both['complaint_id'] = both.fileid.astype(str) + '_' + both.complaint_no.astype(str)
    
    logger.info('creating indicator variables')
    both = make_meta_indic(both)
    both = make_proc_indic(both)
    both = make_kw_indic(both)
    both.info()
    vc = both[[col for col in both.columns \
               if both[col].dtype == bool]].sum().to_dict()   
    vc['reports_found'] = len(both.fileid.unique())
    vc['complaints'] = len(both.complaint_id.unique())
    vc['allegations'] = len(both.allegation_id.unique())
    vc['DPA_report'] = both.loc[both.report_type == 'DPA'].shape[0]
    vc['sustained'] = int(both.sustained.sum())
    vc['mediated'] = int(both.mediation_status.notna().sum())
    for v,c in vc.items(): logger.info(f'{v}:\t\t{c}')
    write_yaml("output/useful.yml", vc)
    
    both.to_parquet(args.output)
    
    logger.info("done.")
    
#}}}
# done.
