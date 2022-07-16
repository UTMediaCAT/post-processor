from cgitb import html
import os
import json
import subprocess
from socket import timeout
import sys
from multiprocessing import Process, Manager


def getDate(f):

    if f.split('/')[-1] in os.listdir('./DatedOutput/'):  # skip completed files
        return
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
        if date != 'null' and len(date) > 0:
            d['date'] = date
        if author != 'null' and len(author) > 0:
            d['author_metascraper'] = author
        if title != 'null' and len(title) > 0:
            d['title_metascraper'] = title
        if not (htmlcontent == 'article.content'):
            d["html_content"] = htmlcontent
        if not (textcontent == 'article.textContent'):
            d["article_text"] = textcontent
    with open('./DatedOutput/' + f.split('/')[-1], 'w') as outfile:
        outfile.write(json.dumps(d))


def init(files):
    for path in files:
        getDate(path)
    exit(0)


if __name__ == '__main__':
    num_procs = 16
    process_index = 0
    assignments = {k: [] for k in range(num_procs)}
    for f in os.listdir(sys.argv[1]):
        path = sys.argv[1] + \
            f if sys.argv[1].endswith('/') else sys.argv[1] + '/' + f
        assignments[process_index % num_procs].append(path)
        process_index += 1
    procs = [None] * num_procs
    for proc in range(num_procs):
        procs[proc] = Process(target=init, args=(assignments[proc], ))
        procs[proc].start()
