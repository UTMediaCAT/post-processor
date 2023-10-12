import os
import json
import csv
import subprocess
from socket import timeout
import sys
import pandas as pd
import time
from multiprocessing import Process, Manager
ROWS_PER_CSV = 10000


def create_csv_title(file_name):
    row = ('id', 'title', 'url', 'author', 'date',
           'html_content', 'article_text', 'domain', 'found_urls')
    with open(file_name, 'a') as new_file:
        csv_writer = csv.writer(new_file)
        csv_writer.writerow(row)
    new_file.close()


def getDate(files, proc, i):
    file_name = './DatedOutput/' + str(proc) + '_output_' + str(i) + '.csv'
    try:
        df = pd.read_csv(file_name)
    except:
        df = pd.DataFrame()

    if (len(df) >= ROWS_PER_CSV):
        print("already processed: " + file_name)
        return
    if (len(df) > 0):
        os.remove(file_name)
    print("try to process: " + file_name)

    for f in files:
        with open(f, 'r') as json_file:
            d = json.loads(json_file.read())

            try:
                dateFile = "datesWithHTML.js" if withHTML else "dates.js"
                proc = subprocess.Popen(['node', dateFile,
                                        d['url']], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='')

                results = proc.communicate(timeout=2)[0].decode(
                    'utf8').split("\nsplit\nsplit\n")
            except Exception:
                results = ['', '', '', '', '']

            try:
                date = results[0]
            except Exception:
                date = ""
            try:
                author = results[1]
            except Exception:
                author = ""
            try:
                title = results[2]
            except Exception:
                title = ""
            try:
                htmlcontent = results[3]
            except:
                htmlcontent = ""
            try:
                textcontent = results[4]
            except:
                textcontent = ""
            row = {
                'id': f[f.rfind('/')+1:].replace('.json', ''),
                'title': title,
                'url': d['url'],
                'author': author,
                'date':  date,
                'html_content': htmlcontent,
                'article_text': textcontent,
                'domain':  d['domain'],
                'found_urls': d['found_urls']
            }

            df = df.append(row, ignore_index=True)
        json_file.close()
    df.to_csv(file_name)
    print("finished processing: " + file_name)
    return


def divide(files):
    for i in range(0, len(files), ROWS_PER_CSV):
        yield files[i:i + ROWS_PER_CSV]


def init(files, proc):
    file_chunks = list(divide(files))
    i = 0
    for chunk in file_chunks:
        getDate(chunk, proc, i)
        i = i + 1
    exit(0)


if __name__ == '__main__':
    withHTML = True
    num_procs = 10
    process_index = 0
    assignments = {k: [] for k in range(num_procs)}
    for folder in os.listdir(sys.argv[1]):
        folder_name = sys.argv[1] + folder + '/'
        print(folder_name)
        for f in os.listdir(folder_name):
            path = folder_name + \
                f if folder_name.endswith('/') else folder_name + '/' + f
            assignments[process_index % num_procs].append(path)
            process_index += 1
    procs = [None] * num_procs
    for proc in range(num_procs):
        procs[proc] = Process(target=init, args=(assignments[proc], proc, ))
        procs[proc].start()
