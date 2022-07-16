# Metscraper
This tool can be used to populate the `title_metascraper, author_metascraper, date, html_content, article_text` fields of JSONs produced by the domain crawler.

## Usage
First install the required packages if not already installed, `npm i metascraper metascraper-author metascraper-date metascraper-title got`

Create the output directory inside the metascraper directory, `mkdir DatedOutput`

To run, use `python3 getDates.py /path/to/jsons/` where `/path/to/jsons/` is the full path to the directory of JSONs whose dates will be populated. 
The resulting updated JSONs will be located in the `DatedOutput` directory with the same original name. 

## Additional Notes
These scripts make use of multi-processing for improved efficiency. By default, 50 parallel processes are used but this can be modified by changing the value of `num_procs` on line 44 of `getDates.py`. Since these scripts are relatively light-weight, `num_procs` can safely be made a very large number without significant memory usage. 
