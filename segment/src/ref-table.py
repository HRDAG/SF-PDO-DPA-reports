# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     TS
# Maintainers: TS
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================

from pathlib import Path
import os
import argparse
import hashlib
from functools import partial
import yaml
import re
import pandas as pd


def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputdir')
    parser.add_argument('--index')
    parser.add_argument('--hand')
    parser.add_argument('--output')
    return parser.parse_args()


def make_hash(rec, dataset=None):
    '''given a pandas record and a string dataset name,
       hash the dataset name and the record's fields w sha1,
       return the hexdigest.
    '''
    hasher = hashlib.sha1()
    hasher.update(bytearray(str(dataset), encoding='utf-8'))
    for f in rec:
        hasher.update(bytearray(str(f), encoding='utf-8'))
    return hasher.hexdigest()[:16]


def read_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f)
        f.close()
    return data


def empty_allegation(line):
    for blank in blanks:
        if blank in line: return True
    return False


def outside_juris(line):
    if not line: return False
    if "jurisdiction" not in line.lower(): return False
    found = re.search("(not.*within.*jurisdiction)|(outside.*jurisdiction)", line, flags=re.I)
    if (not found) | (found == ''): return False
    return True


if __name__ == '__main__':
    args = getargs()
    blanks = read_yaml(args.hand)

    ind = pd.read_parquet(args.index)
    segmented = pd.concat(pd.read_parquet(f)
                          for f in Path(args.inputdir).rglob('*.parquet'))

    # note: currently this would be better using `os.path.basename`,
    #       but keeping like this in case we ever have nested subdirs
    ind['joincol']       = ind.pdf_file.str.extract('output/pdfs/(.+)\\.pdf$')
    segmented['joincol'] = segmented.parquetfile.str.extract('output/each/(.+)\\.parquet$')

    # note: there are duplicate files that get downloaded, but don't need to have
    # multiple rows since the files are different, we'll run them all through
    # segmentation, but should only hold on to the ones whose fileid is in `ind`.
    #assert set(ind.joincol).issubset(segmented.joincol)
    print(set(ind.joincol).difference(segmented.joincol))
    print(set(segmented.joincol).difference(set(ind.joincol)))

    out = pd.merge(ind, segmented, how = 'inner', on = 'joincol') \
            .drop(['parquetfile', 'joincol'], axis = 1)
    print('dropping blank allegations')
    print(f'records before drop:\t\t\t{out.shape[0]}')
    out['blank_allegation'] = out.allegation_text.apply(empty_allegation)
    blank_n = (out.blank_allegation == True).sum()
    print(f'blank allegations identified (count):\t\t{blank_n}')
    out.drop(out.loc[out.blank_allegation].index, inplace=True)
    print(f'records after drop:\t\t\t{out.shape[0]}')
    print('labeling allegations outside jurisdiction')
    out['outside_jurisdiction'] = out.allegation_text.apply(outside_juris)
    oob_n = (out.outside_jurisdiction == True).sum()
    print(f'outside_jurisdiction allegations identified (count):\t{blank_n}')
    print('preparing allegation_ids')
    hash_recs = partial(make_hash, dataset=out)
    out['allegation_id'] = out.apply(hash_recs, axis=1)
    out.replace("", None, inplace=True)
    print(out.allegation_id.duplicated().sum())
    #assert not any(out.allegation_id.duplicated())
    assert not any(out.allegation_id.isna())
    out.to_parquet(args.output, index=False)


# done.
