# Post-processor
```
cd Post-Processor
pip3 install -r requirements.txt
python3 processor.py
```
**NOTE: more detailed instructions and testing can be find in compiler.ipynb**

### Required files and folder structure within Post-Processor directory:
- DataDomain: holds all domain crawler output files
- DataTwitter: holds all twitter crawler output files
- crawl_scope.csv: scope file that contains all the crawl domains
- citation_scope.csv: scope file that contains all the citation domains
- Output: a folder to hold the output of the processor, including output.csv, output.xlsx and interest_output.json (can be empty prior to running)
- Saved: a folder to hold saved intermediate states of files (can be empty)
- logs: a folder to hold logs (can be empty)
    
output.csv will include an row for URL x from **DomainOutput** iff:
- x's domain is in **crawl scope** 
- x contains citation (text alias, or twitter handler) with domain from **citation scope**

Note: read_from_memory flag is can be manually turned on and off on processor.py main. If picking up the processor from a previous break, then run the program with read_from memory set to True.
