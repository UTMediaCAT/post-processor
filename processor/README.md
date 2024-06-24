# Post-processor - How to Run
```
cd Post-Processor
pip3 install -r requirements.txt
python3 compilier.py
```

# Post-processor - Additional Flags for Running
```
The following flags serve as break point in case the processor fails at a part:
-lt : disable the twitter loading
  - (use only if `twitter_data` is saved as a parquet)
-ld : disable the domain loading
  - (use only if `domain_data` is saved as parquet)
-ltcsv : disable twitter data to single csv
  - (use only if ./data_twitter/data_twitter_csv/output.csv exists)
-ldcsv : disable domain data to single csv
  - (use only if ./data_domain/data_domain_csv/output.csv exists)
-pt : disable processing twitter data and referral extraction entirely 
  - (use only if `processed_twitter_data` and `twitter_referral_data` is saved as parquet)
-pd : disable processing domain data and referral extraction entirely
  - (only if `processed_domain_data` and `domain_referral_data` is saved as parquet)
-ppt : disable processing twitter data
  - (only if `processed_twitter_data` is saved as parquet)
-ppd : disable processing domain data
  - (only if `processed_domain_data` is saved as parquet)

The following flags are for processing resources purposes:
-w x : specify number of workers to use for processing (x is an int)
-tpw x : specify number of threads per workers to use for processing (x is an int)
-wm x : specify maximum memory to use per worker (x is a float)

The following flags are for cleaning up before the execution:
-cu : cleanup all the parquets and files in ./saved
```

### Required files and folder structure within Post-Processor directory:
- citation\_scope.csv: scope file that contains all the citation domains
- data\_domain: holds all domain crawler output files
- data\_twitter: holds all twitter crawler output files
- output: a folder to hold the output of the processor, including output.csv, output.xlsx and interest\_output.json (can be empty prior to running)
- saved: a folder to hold saved intermediate states of files (can be empty)
- logs: a folder to hold logs (can be empty)

# General Processing Logic Flow
- The processing occurs in the following order in terms of files to inspect: `compiler.py > load_input.py > processor.py (processor_domain.py + processor_twitter.py) > create_output.py`
## compiler.py
- Argument parsing is done here
- File checking is done here too (checking if citation_scope.csv exists and the proper folders are there if not then create the folders)
- Loads the input dataset using helper functions from load_input.py
  - All domain and twitter data set are converted to their own separate singular CSV (Data validation is performed at this step)
    - Conversion code is in `utils.py`
## load_input.py
- Loads citation_scope.csv, domain's output.csv, and twitter's output.csv into the program
- citation scope is converted to two dictionaries for later processing: a twitter handle dictionary and a domain dictionary
- domain and twitter dataset is loaded in here as a Dask Dataframe
  - It's index is set to be the 'Referring URL' of the dataset and any duplicate URL within the dataset is dropped
  - Loaded result is saved as a .parquet file for easier loading to Dask
## processor.py
- Processes the domain and twitter dataset
- Processes the datasets by gathering the information necessary to fill the 'Referring Associate Publisher', 'Referring Tags', 'Referring Name', 'Cited Names', 'Cited Tags', and 'Cited Associated Publishers' using the citation_scope and twitter dictionary
  - Domain will also fill out 'Anchor Text'
  - Once processing is done, result is saved as a .parquet
- Referring dictionary for twitter or domain is generated using the processed dataset .parquets
  - Referral 'Source' is gathered individually and an aggregation is later performed on the same grouped 'Source' to concatenate the domains list the point to together
  - This referral will be saved as a .parquet once done
- When both twitter and domain datasets are done processing then the referral columns will be filled
  - Using the twitter and domain referral dataframe an attempt in cross matching and filling the 'Cited by URL' and 'Number of Cited by URLs' of processed twitter and domain datasets will occur (More info below)
  - The result will be saved as a final .parquet
## create_output.py
- Creates the CSV output by dropping columns whose purpose is only for processing, renaming columns to its proper names, and reordering columns for organization purposes
- Processing is complete once this is reached
    
# Referral Extraction
Purpose of the referral section: a lookup table to see which entry in our dataset also referred to another source in our dataset
Algorithm: 
Each time an article or twitter posts is processed the following is performed for the referral:
  - Each entry in the 'Found URLs' column are added to the dictionary as keys to a list
  - The list will include the currently examined article or twitter posts' url in it (indicating this current article or twitter post had referred to it)
  - Each entry in the 'Found URLs' is now considered as a 'Source' and the currently examined article or twitter post is considered a 'Domain' that referred the 'Source'
Since it is done in partitions, the ending dictionary is converted to a panda dataframe to be reassembled as a dask partition by dask
  - Since it is done in partitions, there would be duplicate source so we use dask aggregation method to groupby the same 'Source' and perform a merging operation on it
  - Which results in the referral lookup dataframe for both twitter and domain datas

# Referral Cross Matching
Key Note: 
  - the domain_data and twitter_data has the 'Referring URL' column as its index to better perform queries
  - the domain_referrals and twitter_referrals has the 'Source' column as its index to better perform queries
Algorithm:
For the currently examined article or twitter posts, it aims to check the twitter_referrals and domain_referrals to see if there exist an entry that has the article or
twitter post 'Referring URL' as it's 'Source' if so simply append all its referrals to the currently examined article or twitter post as a new column called 'Cited By URLs' and count of 
its referral as 'Number of Cited By URLs'

# Debug & Logging
Purpose: Indicate which logging folder is which procedure. (View the logs folder)
Note: The number of workers you specify in the arguments will indicate how many worker logs you will generate for each processing procedure
- processor.log: General processing logs basically simple reporting on how long it took for the procedures
- domain_load: Loading the domain csv into dask
- twitter_load: Loading the twitter csv into dask
- domain_processing: Process the domain to fill the additional data
- twitter_processing: Process the twitter to fill the additional data
- domain_referral_processing: Process the referral dataframe from the processed domain dataframe
- twitter_referral_processing: Process the referral dataframe from the processed twitter dataframe
- processing: Referral cross matching with the domain + twitter referral data and the twitter + domain data
- output_processing: Final CSV output procedure, dropping columns, renaming, and reordering columns