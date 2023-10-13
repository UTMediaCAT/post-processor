# CSV Header Cleaner

This directory hosts useful Python scripts to remove duplicate headers in *.csv* files.  `clean.py` cleans any *.csv* files that have duplicate headers **individually**.  `multiclean.py` extends the functionality of `clean.py` by cleaning whole directories with multiprocessing.

## Instructions for `clean.py`

Call `python3 clean.py <old file> <new file>`, where 
- \<old file\> is the dirty *.csv* file to be cleaned and
- \<new file\> is the name for the newly cleanen *.csv* file.

## Instructions for `multiclean.py`

Call `python3 multiclean.py <source dir> <target dir> [num_proc]`, where
- \<source dir\> is the path of the source directory containing files to be cleaned,
- \<target dir\> is the path of the target directory to hold clean *.csv* files, and
- \[num\_proc\] is an optional argument denoting the number of processes to use for cleaning.  If not specified, the default will be 1 process, which means no multiprocessing.

