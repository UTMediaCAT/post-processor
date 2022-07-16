# Get URLs
These scripts can be used to update the `found_urls` key of a crawled domain JSON with all the links found in the HTML content of the corresponding page. 
## Usage
After cloning this repository, 
1. `cd mediacat-backend/utils/getAllUrls`
2. `mkdir Results` to create the directory where the results will be stored
3. `mkdir Complete` to create the directory that will keep track of the completed files
3. Then run the script with `python3 master.py /path/to/files/` where `/path/to/files` is the full path to the directory with the crawled domain JSONs. 

**IMPORTANT: the original files in `path/to/files` will be moved to the `Complete` directory in order to allow the script to keeps its place in the event that it quits. The final resulting files with all the found URLs will be stored in `Results` under their original name**

## Additional Notes
### Time limits
To avoid potential timeouts and memory issues, the script runs for 2 hours before restarting, this time limit can be modified by changing the value of `runtime` in line 136 of getURLs.py to the desired limit in seconds. 

### Monitoring progress
The progress of these scripts can be monitored in two ways. To see how many files still must be processed, you can simply check the number of files remaining in `/path/to/files` since they are moved out as they are completed. This can be done with `ls /path/to/files | wc -l`.

You can also get a more detailed look on what the script is currently doing by looking at the created log called `urlLog.log`. To see the latest infromation, use `tail urlLog.log`.