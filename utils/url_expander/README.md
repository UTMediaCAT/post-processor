# Twitter URL Expander
This script can be used to expand shortened links found in the output of Twitter crawls.

## Usage
After cloning this repository, 
1. `cd mediacat-backend/utils/urlExpander`
2. `python3 source dest num_procs`, where:
    - `source` is the full path to the directory of twitter CSVs
    - `dest` is the full path to the directory where the new CSVs (with the expanded URLs where possible) will be stored. They will be stored under their original name
    - `num_procs` is an integer representing the number of processes to be used for multi-processing. For optimum performance, and if available resources allow it, this will ideally be equal to the number of files in `source`

## Known Issues
### Connection & Client Errors
The [URL-expander python module](https://pypi.org/project/urlexpander/) used by this script may sometimes fail at expanding the URL. If there is a connection error or client error, the expanded link will still contain the domain and will look like `http://www.billshusterforcongress.com/__CONNECTIONPOOL_ERROR__` or `http://speakerryan.com/__CLIENT_ERROR__`. Otherwise, if there is another error and the domain cannot be resolved, then the original, shortened URL will be copied over. 

### Skipping non-shortened URLs
To improve efficiency, the URL-expander module can be programmed to skip over links that are already expanded (see [here](https://nbviewer.org/github/SMAPPNYU/urlExpander/blob/master/examples/quickstart.ipynb?flush_cache=true) for more), however it does not recognize all shortened patterns and may sometimes incorrectly skip over some links. The list of known shortened patterns can be further expanded with user defined input (again, see [here](https://nbviewer.org/github/SMAPPNYU/urlExpander/blob/master/examples/quickstart.ipynb?flush_cache=true) for more).