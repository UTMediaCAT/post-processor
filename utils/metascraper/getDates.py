import os
import json
import csv
import subprocess
from socket import timeout
import sys
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
    create_csv_title(file_name)
    with open(file_name, 'a') as new_file:
        for f in files:
            with open(f, 'r') as json_file:
                d = json.loads(json_file.read())

                try:
                    proc = subprocess.Popen(['node', 'dates.js',
                                            d['url']], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='')

                    results = proc.communicate(timeout=5)[0].decode(
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
                row = (
                    f[f.rfind('/')+1:].replace('.json', ''),
                    title,
                    d['url'],
                    author,
                    date,
                    htmlcontent,
                    textcontent,
                    d['domain'],
                    d['found_urls']
                )

                csv_writer = csv.writer(new_file)
                csv_writer.writerow(row)
            json_file.close()

    new_file.close()


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
    num_procs = 20
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
