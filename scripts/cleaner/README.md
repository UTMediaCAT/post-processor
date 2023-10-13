# CSV Cleaner Automation Script
This multiprocessing script cleans duplicate header and record entries in CSV files.

## Instructions
Call `python3 clean.py dir [n_proc]` where *dir* is the directory of CSV files to be cleaned and *n\_proc* is an optional argument representing the number of processes to use for cleaning.  The default for *n\_proc* is 4.

## Expected Results
After calling the script, the cleaned CSV files will be in *dir* which is the directory you used as argument for the script.
