## NYT Archive Explorer

The ```nyt_archive_explorer.py``` script takes a list of months as input within its ```main()``` method on line 103,as well as the user's API Key for the New York Times Archive API as input in the ```send_request()``` method on line 28.
The resulting output is a directory ```/headlines/...``` , with each CSV within the directory representing a month of articles.

The Archive API must be enabled on the user's Developer Portal to use this script.

This script is provided as a way to validate article counts from the NYT on the MediaCAT project, as the output CSVs can be filtered by their ```subsection``` column to obtain counts for articles on a specific topic.
This script is adapted from [this guide](https://towardsdatascience.com/collecting-data-from-the-new-york-times-over-any-period-of-time-3e365504004).

## Insert_missing_dates.py
This scripts operates on the file created by post-proccesso-to-csv and fills in missing publication date for news articles. If the date of publication is present in the csv, it will keep it.

Currently the number of lines read (`limit`) and input size for each thread (`thread_input_size`) are hardcoded

