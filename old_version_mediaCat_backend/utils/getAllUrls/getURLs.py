import sys
import signal
import os
from bs4 import BeautifulSoup
import json
import urllib.request
from urllib.parse import urlparse
from timeit import default_timer as timer
import validators
import logging
import psutil
from multiprocessing import Process, Manager
import random
import pandas as pd
import time
import subprocess
logging.basicConfig(filename='urlLog.log', level=logging.DEBUG, filemode='w') 

def handler(signum, frame, cause):
    logging.info('Timed out')
    raise Exception("end of time")

def parse_scope(scope):
    """
    This is currently not being used
    """
    domains = []
    try:
        df = pd.read_csv(scope)
        sources = df["Source"].tolist()
        for source in sources:
            if source.startswith('@'):
                continue
            try:
                domain = urlparse(source).netloc
            except Exception:
                domain = source
            #only add non-empty domains
            if len(domain) > 0:
                domains.append(domain)
            else:
                domains.append(source)

    except Exception:
        print('Error reading scope')
        exit(1)
    return domains

def get_urls(f):

    filename = sys.argv[1] + f
    with open(filename, 'r') as json_file:
        d = json.loads(json_file.read())
        #html_page = d['html_content']
        signal.signal(signal.SIGALRM, handler)
        try:
            signal.alarm(30) #timeout on page content after 30 seconds
            html_page = urllib.request.urlopen(d['url']) #get html of page
        except Exception:
            html_page = d['html_content'] #get html of page from file
        signal.alarm(0)
        soup = BeautifulSoup(html_page, "html.parser")
        urls_full = []
        urls = []
        logging.info(str(os.getpid()) + ': Getting links')
        allLinks = soup.findAll('a')
        c = 0
        #get all the links in the html content 
        for link in allLinks:
            signal.signal(signal.SIGALRM, handler)
            try: #try validating the link
                logging.info(str(os.getpid()) + ': Validating ' + link.get('href'))
                if not validators.url(link.get('href')):
                    continue
                try: #try getting the title for the link
                    logging.info(str(os.getpid()) + ': Getting title after validation')
                    signal.alarm(30)
                    title = BeautifulSoup(urllib.request.urlopen(link.get('href')), features="lxml").title.string
                except Exception:
                    title = ""
            except Exception:
                try: #try getting the title for the link
                    logging.info(str(os.getpid()) + ': Getting title without validation')
                    signal.alarm(30) #timeout after 30 seconds
                    title = BeautifulSoup(urllib.request.urlopen(link.get('href')), features="lxml").title.string
                except Exception:
                    title = ""
            signal.alarm(0)
            logging.info(str(os.getpid()) + ': Producing final link')
            final_link = link.get('href')

            logging.info(str(os.getpid()) + ': Adding link')
            urls_full.append({'title': title, 'url': final_link})
            urls.append(final_link)
            c+=1
            logging.info(str(os.getpid()) + ': Found ' + str(c) + '/' + str(len(allLinks)) + ' links')

        logging.info(str(os.getpid()) + ': Done getting links')


        logging.info(str(os.getpid()) + ": Adding new URLs")
        #add any URLs that were already found, if they were not found again 
        for u in d['found_urls']:
            if u['url'] not in urls:
                urls_full.append(u)

        d['found_urls'] = urls_full
        logging.info(str(os.getpid()) + ": Done adding new URLs")

        logging.info(str(os.getpid()) + ": Writing to file")
        #write the new result to file
        with open('Results/' + f, 'w') as outfile:
            outfile.write(json.dumps(d))
        #After finishing everything, move the original file
        #os.popen('rm ' + filename)
        os.popen('mv ' + filename + ' Complete/' + f)
        logging.info(str(os.getpid()) + ": Done writing to file")


def run(assignments):
    """
    Gets the URLs for each file in assignments
    """
    i = 0
    for f in assignments:
        get_urls(f)
        i+=1
        logging.info('Process: ' + str(os.getpid()) + ' finished ' + str(i) +  '/' + str(len(assignments)) + ' JSONs')


if __name__ == '__main__':
    #sources = parse_scope('./input_scope_final.csv')
    man = Manager()
    max_usage = 70 #use no more than 70% of available memory 
    num_procs = 10 #use 10 processes in parallel
    runtime = 7200 #restart every 2 hours
    assignments = {k: [] for k in range(num_procs)}
    i = 0
    # assign files to each process
    for f in os.listdir(sys.argv[1]):
        assignments[i%num_procs].append(f)
        i+=1
    procs = [None] * num_procs
    pids = []
    initial = int(os.popen('ls Results | wc -l').read())
    start = timer()
    # start up each process
    for x in range(num_procs):
        p = Process(target=run, args=(assignments[x], ))
        p.start()
        pids.append(p.pid)
    #time.sleep(runtime)
    while True:
        # if we've exceeded the time or memroy limit, break
        if timer() - start >= runtime or psutil.virtual_memory()[2] >= max_usage:
            break
        time.sleep(10)
    for pid in pids:# kill all child processes
        cmd = 'kill ' + str(pid)
        try:
            subprocess.Popen(cmd, shell=True)
        except Exception:
            logging.info('Process not found ' + str(pid))
    final = int(os.popen('ls Results | wc -l').read())
    print('Processed ' + str(final - initial) + ' JSONs in ' + str(runtime) + ' seconds')
    logging.info('Processed ' + str(final - initial) + ' JSONs in ' + str(runtime) + ' seconds')



        

