"""
This script expands all shortened URLs in tweet CSVs
Usage:
    python3 expand.py source_directory destination_directory
"""
import csv
import urlexpander
import os
import ast
import sys
import requests
import logging
import timeit
import subprocess
from socket import timeout
from multiprocessing import Process, Manager

logging.basicConfig(filename='./logs/error.log', level=logging.INFO, filemode='w')


def expand_urls(filename):
    source = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    fullFilename = source + filename
    print('processing file: ', filename)
    # copy = []
    fieldnames = None
    line_num = 0
    i = 0
    start = timeit.default_timer()
    dest = sys.argv[2] if sys.argv[2].endswith('/') else sys.argv[2] + '/'
    with open(dest + filename, 'w', encoding='utf-8-sig') as csvfile:

        with open(fullFilename, mode='r', encoding='utf-8-sig') as csv_file:
            for line in csv.DictReader(csv_file):
                if (i % 1000 == 0):
                    print(filename, i)
                d = {}
             #    print(line.keys())
                if fieldnames == None:
                    fieldnames = line.keys()
                for key in line.keys():
                    # copy over non-URL values
                    if key != 'citation_urls':
                        d[key] = line[key]
                    else:  # URLs need to expand
                        urls = ast.literal_eval(line['citation_urls'])
                        '''
                        try:
                            expand_urls = urlexpander.expand(urls)
                        except Exception:
                        '''
                        expanded_urls = []
                        for url in urls:  # expand each URL

                            # url is already expanded
                            if ('www' in url) or ('https://twitter.com/' in url):
                                expanded_urls.append(url)
                                continue

                            try:
                                expanded = urlexpander.expand(url)
                                # proc = subprocess.Popen(['python3', 'urlExpanderHelper.py',
                                #                          url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='')

                                # expanded = proc.communicate(
                                #     timeout=10)[0].decode('utf8').strip()

                                if ('ERROR' in expanded):
                                    # try:
                                    #     # try to request the expanded url, timeout after 5s
                                    #     re = requests.get(url, timeout=5)
                                    #     expanded = re.url
                                    # except requests.exceptions.Timeout:
                                    #     logging.info('timeout at ' + str(url))
                                    #     proc = subprocess.Popen(['node', 'getExpandedURL.js',
                                    #                              url], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='')

                                    #     expanded = proc.communicate(
                                    #         timeout=5)[0].decode('utf8').strip()
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
                # copy.append(d)
                if (i == 0):
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                # write result to a new file
                writer.writerow(d)
                i = i + 1
                line_num += 1

    # write the result to a new file
    # with open(dest + filename, 'w', encoding='utf-8-sig') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    #     writer.writeheader()
    #     for elem in copy:
    #         writer.writerow(elem)
    stop = timeit.default_timer()
    print('complete processing', filename)
    print('Time: ', stop - start)
    # Work complete, kill the process


def initialize(a):
    # expand URLs for each assigned CSV
    for assignment in a:
        expand_urls(assignment)
    sys.exit(0)


if __name__ == '__main__':
    """
    Set up multi-processing for more efficient performance
    """
    # change num_procs to the desired number of processes,
    # ideally, this should be equal to the number CSV files if possible
    num_procs = int(sys.argv[3])
    i = 0
    source = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    print('Running with', num_procs, 'processes')
    assignments = {k: [] for k in range(num_procs)}
    for f in os.listdir(source):
        assignments[i % num_procs].append(f)
        i += 1
    procs = [None] * num_procs
    for proc in range(num_procs):
        procs[proc] = Process(target=initialize, args=(assignments[proc], ))
        procs[proc].start()
