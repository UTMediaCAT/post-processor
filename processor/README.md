# Post-processor
```
cd Post-Processor
pip3 install -r requirements.txt
python3 compilier.py
```
**NOTE: more detailed instructions and testing can be find in `compiler.ipynb`**

### Required files and folder structure within Post-Processor directory:
- crawl\_scope.csv: scope file that contains all the crawl domains
- citation\_scope.csv: scope file that contains all the citation domains
- data\_domain: holds all domain crawler output files
- data\_twitter: holds all twitter crawler output files
- output: a folder to hold the output of the processor, including output.csv, output.xlsx and interest\_output.json (can be empty prior to running)
- saved: a folder to hold saved intermediate states of files (can be empty)
- logs: a folder to hold logs (can be empty)
    
output.csv will include an row for URL x from **DomainOutput** iff:
- x's domain is in **crawl scope** 
- x contains citation (text alias, or twitter handler) with domain from **citation scope**

Note: read\_from\_memory flag is can be manually turned on and off on processor.py main. If picking up the processor from a previous break, then run the program with read\_from memory set to True.
