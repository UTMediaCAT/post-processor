"""
This scripts operates on the file created by post-proccesso-to-csv
and fills in missing publication date for news articles 
If the date of publication is present in the csv, it will keep it
"""

import psutil
import os
import ijson
import csv
from htmldate import find_date
import threading

THREAD_NAME = "thread"
header = ['id', 'url or alias text',
          'hit count', 'date of publication']


class myThread (threading.Thread):
    def __init__(self, name, data):
        threading.Thread.__init__(self)
        self.name = name
        self.data = data

    def run(self):
        print("Starting " + self.name)
        get_date(self.name, self.data)


def get_date(thread_name, data):
    print(thread_name+" ran")
    with open(thread_name+".csv", 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in data:
            row_with_date = row.copy()
            if not row_with_date['date of publication']:
                date = ""
                try:
                    date = find_date(row['url or alias text'])
                except ValueError:
                    print(row['url or alias text'] + " Failed")
                row_with_date['date of publication'] = date
            writer.writerow(row_with_date)
    f.close()


data = ijson.parse(open('interest_output.json', 'r'))

limit = 2000
thread_input_size = 20
next_thread = thread_input_size
start = 0
count = 0
threads = []


row = {}
all_rows = []
for prefix, event, value in data:
    if count > limit:
        break
    if prefix.endswith('.id'):
        count = count + 1
        if count > limit:
            break
        if count > 1:
            row['id'] = value
            # writer.writerow(row)
            all_rows.append(row)
            row = {}
            if count == next_thread:
                thread = myThread(
                    THREAD_NAME+str(count//thread_input_size), all_rows[start:next_thread])
                threads.append(thread)
                start = next_thread
                next_thread = next_thread + thread_input_size
    elif '.' in prefix and prefix[str.rindex(prefix, '.') + 1:] in header:
        row[prefix[str.rindex(prefix, '.') + 1:]] = value

# Start all threads
for x in threads:
    x.start()

# Wait for all of them to finish
for x in threads:
    x.join()
print(len(all_rows))
print(str(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2) + " MB memory used")

# merging
if os.path.exists("merged.csv"):
    os.remove("merged.csv")
fout = open("merged.csv", "a")
# first file:
for line in open("thread1.csv"):
    fout.write(line)
# now the rest:
count = 0
for num in range(2, limit//thread_input_size + 1):
    full_thread_name = THREAD_NAME+str(num)+".csv"
    f = open(full_thread_name)
    f.__next__()  # skip the header
    for line in f:
        fout.write(line)
    count = count + 1
    f.close()  # not really needed
    os.remove(full_thread_name)
os.remove('thread1.csv')
fout.close()
print(count)
