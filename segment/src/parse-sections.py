#import pandas as pd
import argparse
import pandas as pd
import re
import time

def getargs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', default='../pdf2text/output/txt/OCC_03_07_openness.txt')
    parser.add_argument('--output')
    return parser.parse_args()


def flush_allegation_buffer(curstate):
    if curstate['allegation_buffer']:
        curstate['allegations'].append((curstate['curcomplaint'],
                                        ' '.join(curstate['complaintheaders']),
                                        curstate['curallegation'],
                                        curstate['allegation_buffer'],
                                        curstate['alleg_pg_start']))
        curstate['allegation_buffer'] = ''
    return 1


def transition_to_allegation(curstate, line):
    flush_allegation_buffer(curstate)
    curstate['state'] = 'alleg_summary'
    curstate['alleg_pg_start'] = curstate['curpage']
    curstate['allegation_buffer'] = line
    return 1


def transition_to_newcomplaint(curstate):
    flush_allegation_buffer(curstate)
    curstate['state'] = 'preallegation'
    curstate['curcomplaint'] += 1
    curstate['curallegation'] = 0
    return 1


def update_states(curstate, line):
    orig_state = curstate['state']
    if re.match('\x0c', line, flags=re.I):
        curstate['curpage'] += 1

    if re.match('DEPARTMENT OF POLICE ACCOUNTABILITY', line.strip(), flags=re.I):
        curstate['state'] = 'page_header'
    elif re.match('COMPLAINT SUMMARY REPORT', line.strip(), flags=re.I):
        curstate['state'] = 'page_header'
    elif re.findall('(DATE OF COMPLAINT)|(DATE OF COMPLETION)', line.strip(), flags=re.I):
        curstate['state'] = 'preallegation'
        curstate['complaintheaders'].add(line.strip())
    elif re.match('S[U]*MMARY.+ALLEGATION', line, flags=re.I):
        patt = "|".join([
            'allegation[:\\s\\(]*continued[:\\)]*',
            '[0-9]+[:\\s\\(]*continued[:\\)]*',
            'fact[:\\s\\(]*continued[:\\)]*'
        ])
        continuation = (pd.notna(re.match(patt, line, flags=re.I))) | ('continued' in line[:50].lower())
        if (line.strip() == 'SUMMARY OF ALLEGATION #4: Continued)') & (
            'DPA_04_20_openness' in str(curstate['txt_file'])):
            return 1 # this line is uninformative and will be a false pos error in the audit
        elif continuation:
            curstate['state'] = 'alleg_summary'
            curstate['allegation_buffer'] += (' ' + line)
        elif re.match('SUMMARY.+ALLEGATION[S]*:|SUMMARY.+ALLEGATION[S\\s#]*1[:\\-\\s,]+',
                      line, flags=re.I):
            if re.match('SUMMARY.+ALLEGATION[S\\s]*#[2-9]+|SUMMARY.+ALLEGATION[S\\s]*#[0-9]{2,}',
                        line, flags=re.I):
                transition_to_allegation(curstate, line)
                curstate['curallegation'] += 1
            else:
                transition_to_newcomplaint(curstate)
                transition_to_allegation(curstate, line)
                curstate['curallegation'] = 1
        else:
            transition_to_allegation(curstate, line)
            curstate['curallegation'] += 1
    elif orig_state == 'alleg_summary':
        curstate['allegation_buffer'] += (' ' + line)
    elif re.match('^\\w', line, flags=re.I):
        curstate['state'] = 'alleg_summary'
        curstate['allegation_buffer'] += (' ' + line)
    return 1


def initialize_state(curpage=1, curcomplaint=0, curallegation=0, alleg_pg_start=1):
    f"""Template dictionary for the segmentation process.
    - `curpage`: A counter for the number of pages processed. Starts as {curpage} and should increment with observation of '\x0c' in a line. Since the input arg is a single file, this counter is not reset.
    - `curcomplaint`: A counter for the number of complaints processed. Starts as {curcomplaint} and should increment when 'PAGE# 1' is observed in a line. Since the input arg is a single file, this counter is not reset.
    - `complaintheaders`: A collection of the complaint metadata lines containing the date of the complaint and of the investigation's closure, and page numbering.
    - `curallegation`: A counter for the number of allegations processed. Starts as {curallegation} and should increment when '' is observed in a line. Resets when a new complaint is identified in a line.
    - `alleg_pg_start`: When the '^SUMMARY.+ALLEGATION' pattern is identified, the `curpage` is captured as `alleg_pg_start`. Starts as {alleg_pg_start}. Since the input arg is a single file, this variable should .
    - `allegations`: When the a new complaint or allegation is identified, the current info being tracked (`curcomplaint`, `complaintheaders`, `curallegation`, `allegation_buffer`, `alleg_pg_start`) is appended to the collection of `allegations`.
    - `state`: The label of the segment being processed. Options are 'page_header', 'preallegation', and 'alleg_summary'.
    - `allegation_buffer`: When a new allegation is identified and until the next complaint or allegation is identified, the `allegation_buffer` collects the lines processed as one string. Resets when a new complaint or allegation is identified.
    - `txt_file`: The Path of the file being processed as input. Since the input arg is a single file, this should be the same for the whole collection of `allegations`.
    """
    return {'curpage'           : curpage,
            'curcomplaint'      : curcomplaint,
            'complaintheaders'  : set(),
            'curallegation'     : curallegation,
            'alleg_pg_start'    : alleg_pg_start,
            'allegations'       : [],
            'state'             : 'page_header',
            'allegation_buffer' : '',
            'txt_file': ''}


def procfile(fn):
    state=initialize_state()
    with open(fn, 'r') as f:
        state['txt_file'] = fn
        lines = f.readlines()
        for line in lines:
            line = line.strip('\\s\\n')
            if line == '': continue
            update_states(state, line)
    # collect the last allegation in the report
    flush_allegation_buffer(state)
    return state


def setup_audit(df):
    """Fields used to validate the data."""
    copy = df.copy()
    copy['continued_early'] = copy.allegation_text.apply(lambda x: 'continued' in x[:50].lower())
    copy['first_allegation'] = copy.allegation_text.str.contains(
        'SUMMARY.+ALLEGATION[S]*:|SUMMARY.+ALLEGATION[S\\s#]*1[:\\-\\s,]+', flags=re.I)
    copy['subseq_allegation'] = copy.allegation_text.str.contains(
        'SUMMARY.+ALLEGATION[S\\s]*#[2-9]+|SUMMARY.+ALLEGATION[S\\s]*#[0-9]{2,}', flags=re.I)
    copy.first_allegation = copy.first_allegation & ~copy.subseq_allegation
    copy['marked_first'] = copy.allegation_no == 1
    return copy


def report_mismatch(out):
    """Allows for identification of new and unhandled exceptions to the process prior to assertion. \
        Printed output is not necessarily a precursor to an AssertionError."""
    if any(out.first_allegation != out.marked_first):
        print(out[['marked_first', 'first_allegation', 'subseq_allegation',]].value_counts())
        if any((out.first_allegation) & ~(out.marked_first)):
            print('first_allegation but not marked_first')
            print(out.loc[(out.first_allegation) & ~(out.marked_first), [
                'complaint_no','allegation_no',
                'marked_first', 'first_allegation', 'subseq_allegation', 'allegation_text']])
        if any(~(out.first_allegation) & (out.marked_first)):
            print('marked_first but not first_allegation')
            print(out.loc[~(out.first_allegation) & (out.marked_first), [
                'complaint_no','allegation_no',
                'marked_first', 'first_allegation', 'subseq_allegation', 'allegation_text']])
        print(out.loc[out.first_allegation != out.marked_first].allegation_text.values[0])
    return 1


def check_data(out):
    """Validates the data based on minimum expected criteria:
        - No instances of '(continued)' appearing in the first 50 characters of an allegation. \
        This suggests the text is carryover from a prior allegation and was incorrectly separated.
        - All `first_allegation` records should also be `marked_first`. Valid exceptions are \
        instances of a missing allegation number identified by 'ALLEGATION #:' in the text.
    """
    assert not out.continued_early.any(), f"\
        {out.loc[out.continued_early, 'allegation_text'].sample().values[0]}"
    if out.allegation_text.str.contains("ALLEGATION #:").any():
        eligible = out.loc[~out.allegation_text.str.contains("ALLEGATION #:")]
        assert (eligible.first_allegation == eligible.marked_first).all(), f"\
            Non-compliant records found {(eligible.first_allegation != eligible.marked_first).sum()}"
    else:
        assert (out.first_allegation == out.marked_first).all(), f"\
            Non-compliant records found {(out.first_allegation != out.marked_first).sum()}"
    return 1


if __name__ == '__main__':
    args = getargs()
    done = procfile(args.input)
    keys = list(done.keys())
    out = pd.DataFrame(done['allegations'])
    out.columns = ['complaint_no',
                   'complaint_meta',
                   'allegation_no',
                   'allegation_text',
                   'allegation_start_page']
    out = setup_audit(df=out)
    assert report_mismatch(out)
    assert check_data(out)
    out.drop(columns=['continued_early', 'marked_first',
                      'first_allegation', 'subseq_allegation',], inplace=True)
    out['txt_file'] = done['txt_file']
    out['parquetfile'] = args.output
    out.to_parquet(args.output)

# done.
