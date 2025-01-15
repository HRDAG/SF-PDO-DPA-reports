# vim: set ts=4 sts=0 sw=4 si fenc=utf-8 et:
# vim: set fdm=marker fmr={{{,}}} fdl=0 foldcolumn=4:
# Authors:     BP
# Maintainers: BP
# Copyright:   2023, HRDAG, GPL v2 or later
# =========================================
# SFO-pubdef-documents/individual/DPA/scrape/src/scrape.py

# ---- dependencies {{{
import os
from pathlib import Path
from sys import stdout
import argparse
import logging
import re
import hashlib
from random import randint
import yaml
from datetime import date as dt
import requests
from bs4 import BeautifulSoup
import pandas as pd
#}}}

# ---- support methods {{{
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="https://sf.gov/information/reports-policing-complaints")
    parser.add_argument("--hand", default="hand/useragents.yml")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
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


def read_yaml(yaml_file):
    with open(yaml_file, 'r') as f:
        data = yaml.safe_load(f)
        f.close()
    return data


def get_user_agent():
    idx = randint(0, len(users)-1)
    return users[idx]


# should play nice with any webpage we need step through
def parse_link(url):
    # i had the "allow_redirects" arg set to False but it didnt work?
    res = requests.get(url, headers={
        "User-Agent" : get_user_agent()
})
    html = res.content
    parsed = BeautifulSoup(html, "html.parser")
    print(f"page url:\t{url}")
    try:
        print(f"page title:\t{parsed.title.string}\n")
    except:
        return parsed
    return parsed

# bc we want links, this by default only looks for that type attribute
# flexible enough for the first few rounds of html sifting
def find_content(parsed, tagtype, classname, kw):
    # there are other sections on the page outside this scope
    if classname:
        tags = parsed.find_all(tagtype, class_=classname)
    else:
        tags = parsed.find_all(tagtype)
    if kw:
        return [link['href'] for tag in tags for link in tag.find_all("a")
                 if kw in tag.text.lower()]
    return [link['href'] for tag in tags for link in tag.find_all("a")]


# the initial links are separated by years but all appear under the same section name
# the focus of this work is complaints so that is the default kw used to filter results
def find_complaint_years(parsed, tagtype="div", classname="sfgov-section__content", kw="complaints"):
    return find_content(parsed, tagtype, classname, kw)


# make initial links more organized
# initial links can very in source / host platform, so this separation helps organizes the solution
def sort_links_by_year(links):
    """2025 link is picked up as a partial url missing domain, 
    but other years still work as expected."""
    links = {link[link.find("20"):link.find("20")+4]: link
            for link in links}
    domain = "https://www.sf.gov"
    links = {year: link if ('sf.gov' in link) | ('wayback.archive-it.org' in link)
             else f"{domain}{link}" for year, link in links.items()}
    return links


# this function can be passed to `soup.find_all()` via the `href` arg
# then it filters the soup for links matching the kw
def openness_files(href, kw="openness|CSR"):
    return href and re.compile(kw, flags=re.IGNORECASE).search(href)


# assumes we want to download the full content without doing any filtering
# p sure this would work for not-pdfs but that is the type of file we expect here
def download_file(pdf_url, filename):
    response = requests.get(pdf_url, headers={
        "User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
})
    if os.path.exists(filename):
        print(f"*** WARNING: filename '{filename}' already exists. Skipping write. ***\n")
        return 0
    pdf = open(filename, 'wb')
    pdf.write(response.content)
    pdf.close()
    print(f"successful download:\t{pdf_url}\n")
    return 1


def download_pdfs(pdf_links, output_dir):
    print("begin downloading pdfs from list of urls")
    curr = 1
    successes = {pdf_link:None for pdf_link in pdf_links}
    for pdf_link in pdf_links:
        print("------------------------------------")
        print(f"link:\t{curr} of {len(pdf_links)}")
        print("------------------------------------")
        # assumes our save location/env might change
        # + we want to preserve the uploaded pdf's filename
        output_stub = f"{output_dir}"
        corrfname = f"{pdf_link[pdf_link.rfind('files/')+6:]}".replace("/", "-")
        filename = f"{output_stub}/{corrfname}"
        curr += 1
        try:
            print(f"\nattempting to download:\t{pdf_link}")
            attempt = download_file(pdf_link, filename)
            if attempt == 1:
                print(f"\nsaved content as:\n\t\t{filename}\n")
            successes[pdf_link] = filename
        except:
            print(f"\t\t!!\t\tERROR WHILE DOWNLOADING:\t{pdf_link}\n")
            continue
    print()
    print("--->   end of download list   <---")
    return successes


# maybe I'm doing the link scraping wrong but the links need to be prefixed with the domain to work
def add_domain(pdf_links, domain):
    return [f"{domain}{link}" if domain not in link else link for link in pdf_links]


def download_yearly_pdfs(yearly_pdfs, output_dir):
    downloaded = {year:{} for year in yearly_pdfs.keys()}
    print(downloaded.keys())
    for year, pdf_list in yearly_pdfs.items():
        print(f"attempting to download files from {year}")
        downloaded[year] = download_pdfs(pdf_list, output_dir)
        # TODO: check again for 2025 reports; none posted as of 14-JAN-25
        if year != '2025': assert downloaded[year]
    doc_data = [(url, filename) for year, file_status in downloaded.items()
                for url, filename in file_status.items()]
    doc_df = pd.DataFrame(doc_data, columns=['pdf_url', 'pdf_file'])
    return doc_df


def hashid(fname):
    if pd.isna(fname): return None
    with open(fname, 'rb') as f:
        digest = hashlib.file_digest(f, "sha1")
    return digest.hexdigest()[:8]
#}}}

# ---- main {{{
if __name__ == '__main__':
    # setup logging
    logger = get_logger(__name__, "output/scrape.log")

    # arg handling
    args = get_args()
    output_dir = 'output/pdfs'
    users = read_yaml(args.hand)

    print(users)
    print(get_user_agent())

    # read data, initial verification
    logger.info("Loading data.")
    expected = [str(year) for year in range(2020, 2026, 1)]
    main_parsed = parse_link(args.url)

    # find_content() and find_complaint_years() take us from 1 home page link
    # with other stuff on the page we don't want to scrape
    # to a dict of years and the associated urls for that year's complaint reports
    yearly_links = find_complaint_years(main_parsed)
    yearly_links = sort_links_by_year(yearly_links)

    # initial parsing of the links for complaint reports
    years_parsed = {year: parse_link(link)
                    for year, link in yearly_links.items()}

    # since the 2020 link goes to the wayback page
    # and the wayback page just organizes the years (1998, 2021) by anchors
    # we can just read 2020 and collect all the pdfs
    yearly_pdf_links = {year: [tag["href"]
                               for tag in parsed.find_all(href=openness_files)]
                        for year, parsed in years_parsed.items()
                        if year in expected}
    yearly_pdf_links = {year:
                        add_domain(parsed, domain="https://sf.gov")
                        if year in ("2025", "2024", "2023", "2022", "2021")
                        else add_domain(parsed, domain="https://wayback.archive-it.org")
                        for year, parsed in yearly_pdf_links.items()}
    ref_table = download_yearly_pdfs(yearly_pdf_links, output_dir)
    ref_table['fileid'] = ref_table.pdf_file.apply(hashid)
    logger.info(f'{ref_table.fileid.isna().sum()} null fileids')

    #assert not any(ref_table.fileid.isnull())
    ref_table.drop_duplicates(['fileid'], inplace=True)
    ref_table.to_parquet(args.output)

    logger.info("done.")
#}}}
# done.
