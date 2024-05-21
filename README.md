# Complaints against SFPD Closed by the Department of Public Accountability

- the closed complaint data is publically available in the form of a monthly report (pdf)
- the collection of monthly reports can be scraped from where they are posted on the current sf.gov website as well as the previous, archived version
- the current sf.gov website contains reports for 2021 - present
- the archived version of the site contains reports for 1998 - 2020 in full, and part of 2021 records. Note that some of the older reports were written by OCC, not DPA, and the format of the published report changes in this timeline as well. So, more recent records are the priority.
- to collect the available data, both sources are scraped and relevant pdfs are downloaded blindly with their uploaded filename preserved

Regarding the actual data,
- One downloaded pdf contains many sets of pages representing at least one allegation from a complainant(s) per set
- Each allegation in a set has its own facts and findings but shares a completed date

On every page:
- Report header, "DEPARTMENT OF POLICE ACCOUNTABILITY\nCOMPLAINT SUMMARY REPORT"
- DATE OF COMPLAINT
- DATE OF COMPLETION
- PAGE# K of N


On every complainant set:
- SUMMARY OF ALLEGATION #I
- CATEGORY OF CONDUCT
- FINDING
- FINDINGS OF FACT


I'm not sure if it will be easier or the same amount of work to divide pages up by the complainant group or by unique allegation first, but dividing by complainant group seems like a safer step that limits the chance of accidentally cutting off relevant pages, so that will be my first approach. I think I can do some clever regexing for the "PAGE# k of n" piece of a pdf and avoid digesting too much of every page in order to figure out where to section...

Then the pdf content can be extracted and loaded into a database, which we can filter for sustained complaints.

From there, we can add indicator fields and explore linkage between these complaints and other pubdef documents.

### regex patterns to look into or test
sustained complaints
- observed finding tag: 
	* "FINDING: IC (Sustained)"
	* "FINDING: Sustained"
	* "FINDING: S"

named officers
- observed officer mention: "OFFICER KEVIN BURKE #2101"

in at least one sustained complaint, DPA writes that the officers' actions were a liability to the City and exposed it to a costly lawsuit. So, 
- "liability" fixed string


