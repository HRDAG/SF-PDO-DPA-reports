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


def flush_allegation_buffer(curstate, line):
    if curstate['allegation_buffer']:
        curstate['allegations'].append((curstate['curcomplaint'],
                                        ' '.join(curstate['complaintheaders']),
                                        curstate['curallegation'],
                                        curstate['allegation_buffer'],
                                        curstate['alleg_pg_start']))
    curstate['curallegation'] += 1
    curstate['alleg_pg_start'] = curstate['curpage']
    curstate['allegation_buffer'] = line
    return 1


def transition_to_allegation(curstate, line):
    flush_allegation_buffer(curstate, line)
    curstate['state'] = 'alleg_summary'
    return 1


def transition_to_newcomplaint(curstate, line):
    flush_allegation_buffer(curstate, line)
    curstate['state'] = 'preallegation'
    curstate['curcomplaint'] += 1
    #curstate['complaintmeta'] = curstate['complaintheaders']
    curstate['curallegation'] = 0
    curstate['allegation_buffer'] = ''
    return 1


def update_states(curstate, line):
    orig_state = curstate['state']

    if re.match('\x0c', line):
        curstate['curpage'] += 1
        curstate['complaintheaders'] = set()

    if re.match('DEPARTMENT OF POLICE ACCOUNTABILITY', line.strip()):
        return 1
    if re.match('COMPLAINT SUMMARY REPORT', line.strip()):
        return 1

    if re.findall('DATE OF (COMPLAINT)|(COMPLETION)', line.strip()):
        curstate['complaintheaders'].add(line.strip())
        if not re.findall('PAGE', line):
            return 1

    if re.findall('PAGE# 1 of ([0-9]+)', line):
        return transition_to_newcomplaint(curstate, line)
    elif re.match('^SUMMARY.+ALLEGATION', line):
        return transition_to_allegation(curstate, line)
    elif orig_state == 'alleg_summary':
        curstate['allegation_buffer'] += (' ' + line)
    return 1


def initialize_state():
    return {'curpage'           : 1,
            'curcomplaint'      : 0,
            'complaintheaders'  : set(),
            'curallegation'     : 0,
            'alleg_pg_start'    : 1,
            'allegations'       : [],
            'state'             : 'page_header',
            'allegation_buffer' : '', 
            'txt_file': ''}


def procfile(fn):
    state=initialize_state()
    with open(fn, 'r') as f:
        state['txt_file'] = fn
        for line in f:
            line = line.strip('\s\n')
            if line == '': continue
            update_states(state, line)
    # collect the last allegation in the report
    state['curallegation'] += 1
    flush_allegation_buffer(state, line)
    return state


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
    out['txt_file'] = done['txt_file']
    out['parquetfile'] = args.output
    out.to_parquet(args.output)

# done.
