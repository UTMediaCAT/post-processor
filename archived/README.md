[![Build Status](https://travis-ci.org/UTMediaCAT/mediacat-backend.svg?branch=master)](https://travis-ci.org/UTMediaCAT/mediacat-backend)
# mediacat-backend

## Post Processor Usage
**Before running, make sure to remove the testing files from the `DomainOutput` and `TwitterOutput` directories**
```
cd Post-Processor
python3 processor.py
```
### Advanced Usage
The post-processor also supports multi-processing for more efficient performance, to utilize this feature, run `python3 processor.py -num_procs=x -limit=y` where `x` is the number of processes to use and `y` is the memory limit (in bytes) of the local data after which it will be written to disk. Increasing `-limit` will prevent memory errors but may reduce performance speed. Recommended usage: `python3 processor.py -num_procs=10 -limit=5000000`

Required files and folder structure within Post-Processor directory:
- DomainOutput: holds all domain crawler output files
- TwitterOutput: holds all twitter crawler output files
- crawl_scope.csv: scope file that contains all the crawl domains
- citation_scope.csv: scope file that contains all the citation domains
- Output: a folder to hold the output of the processor, including output.csv, output.xlsx and interest_output.json (can be empty prior to running)
- Saved: a folder to hold saved intermediate states of files (can be empty)
- logs: a folder to hold logs (can be empty)
- tempFiles: a folder to hold all the temporary referral files created, must contain the two following sub-directories
    - Domain: for temporary domain files
    - Twitter: for temporary twitter files
    
output.xlsx will include an row for URL x from **DomainOutput** iff:
- x's domain is in **crawl scope** 
- x contains citation (text alias, or twitter handler) with domain from **citation scope**

Note: read_from_memory flag is can be manually turned on and off on processor.py main. If picking up the processor from a previous break, then run the program with read_from memory set to True.

# archived branches

`Test` was a branch that was archived. 

Can be restored by the following command: `git checkout -b Test archive/Test`

It was archived like [this](https://gist.github.com/zkiraly/c378a1a43d8be9c9a8f9):

- git tag archive/Test Test
- git branch -d Test
- git push origin :Test
- git push --tags

Another great [resource](https://stackoverflow.com/questions/1307114/how-can-i-archive-git-branches)
