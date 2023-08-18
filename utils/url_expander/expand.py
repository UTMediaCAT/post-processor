'''
This script expands all shortened URLs in tweet CSVs.
Usage:
    python3 expand.py source_directory destination_directory [num_procs]
'''
import csv
import urlexpander
import os
import ast
import sys
import logging
import timeit
import subprocess
from multiprocessing import Process

logging.basicConfig(filename='./logs/error.log', level=logging.INFO, filemode='w')


def expand_urls(filename):
    # get source and destination directories with proper format
    source = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    dest = sys.argv[2] if sys.argv[2].endswith('/') else sys.argv[2] + '/'
    full_filename = source + filename
    print('processing file:', filename)
    # initialize loop variables
    fieldnames = None
    line_num = 0
    i = 0
    start = timeit.default_timer()
    with open(dest + filename, 'w', encoding='utf-8-sig') as new_csv_file:

        with open(full_filename, mode='r', encoding='utf-8-sig') as csv_file:
            for line in csv.DictReader(csv_file):
                if (i % 1000 == 0):
                    print(filename, i)
                d = {}
                if fieldnames == None:
                    fieldnames = line.keys()

                for key in line.keys():  # copy over non-URL values
                    if key != 'citation_urls':
                        d[key] = line[key]
                    else:  # URLs need to expand
                        if type(line['citation_urls']) is not str:
                            urls = ast.literal_eval(line['citation_urls'].values[0])
                        else:
                            urls = ast.literal_eval(line['citation_urls'])
                        expanded_urls = []

                        for url in urls:  # expand each URL
                            # url is already expanded
                            if ('www' in url) or ('https://twitter.com/' in url):
                                expanded_urls.append(url)
                                continue

                            try:
                                # use the python package to expand the url
                                expanded = urlexpander.expand(url)

                                if ('ERROR' in expanded):
                                    # use puppeteer (node.js) to expand the url
                                    proc = subprocess.Popen(['node', 'getExpandedURL.js',
                                                            url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='')
                                    expanded = proc.communicate(
                                        timeout=5)[0].decode('utf8').strip()

                            except Exception:
                                # if unable to expand URL, use the original URL
                                logging.warning('failed at ' + str(url))
                                expanded = url
                            expanded_urls.append(expanded)

                        d['citation_urls'] = str(expanded_urls)

                # write the header if first time we are seeing csv_file
                if (i == 0):
                    writer = csv.DictWriter(new_csv_file, fieldnames=fieldnames)
                    writer.writeheader()

                # write result to a new file
                writer.writerow(d)
                i = i + 1
                line_num += 1

    # finish looping
    stop = timeit.default_timer()
    print('complete processing', filename)
    print('time:', stop - start)


def initialize(a):
    # expand URLs for each assigned CSV
    for assignment in a:
        expand_urls(assignment)
    # work complete, kill the process
    sys.exit(0)


if __name__ == '__main__':
    '''Set up multi-processing for more efficient performance.'''
    if len(sys.argv) < 3:
        print('usage: python3 expand.py source_directory ' +
                'destination_directory [num_procs]', file=sys.stderr)
        exit(1)
    elif len(sys.argv) < 4:
        # no num_procs specified, use 1 as a default
        num_procs = 1
    else:
        # change num_procs to the desired number of processes,
        # ideally, this should be equal to the number CSV files if possible
        num_procs = int(sys.argv[3])
    i = 0
    source = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    print('running with', num_procs, 'processes')
    assignments = {k: [] for k in range(num_procs)}
    for f in os.listdir(source):
        assignments[i % num_procs].append(f)
        i += 1
    procs = [None] * num_procs
    for proc in range(num_procs):
        procs[proc] = Process(target=initialize, args=(assignments[proc], ))
        procs[proc].start()
    for proc in range(num_procs):
        procs[proc].join()

