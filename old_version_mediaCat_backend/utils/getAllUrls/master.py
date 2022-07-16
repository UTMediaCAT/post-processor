import os
import sys
from timeit import default_timer as timer
import time
import subprocess
#start getting the URLs for files in sys.argv[1]
proc = subprocess.Popen("python3 getURLs.py " + sys.argv[1], shell=True)
runtime = 30
#keep restarting up the processes as soon as they quit
while True:
    if len(os.listdir(sys.argv[1])) == 0:
        #all files have been processed, exit
        exit(0)
    poll = proc.poll()
    if not (poll is None): #process has terminated
        print('running again...')
        proc = subprocess.Popen("python3 getURLs.py " + sys.argv[1], shell=True)
    time.sleep(runtime)

