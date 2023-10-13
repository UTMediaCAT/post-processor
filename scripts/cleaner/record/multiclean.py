import os
import subprocess
import sys
from multiprocessing import Process

TAG = '[MediaCAT]'
USAGE = 'python3 mutliclean.py source_dir target_dir [num_proc]'
PYTHON = 'python3'
SCRIPT = 'clean.py'

def proc_err():
    '''Error for when [num_proc] is not a number.'''
    print(f'{TAG} error: got {sys.argv[3]}, expected number', file=sys.stderr)
    exit(1)

def verify_cli():
    '''Verify the arguments of the CLI.'''
    source, target = get_dirs()
    if source == target:
        print(f'{TAG} error: same directory', file=sys.stderr)
        exit(1)
    if not (os.path.exists(source) and os.path.exists(target)):
        print(f'{TAG} error: missing directories', file=sys.stderr)
        exit(1)

def get_dirs():
    '''Return the source and target directories in their proper formats.'''
    source = sys.argv[1] if sys.argv[1].endswith('/') else sys.argv[1] + '/'
    target = sys.argv[2] if sys.argv[2].endswith('/') else sys.argv[2] + '/'
    return source, target

def get_assignments(num_proc):
    '''Assign processes some work equally based on num_proc.'''
    assigner = {k: [] for k in range(num_proc)}
    source = get_dirs()[0]
    i = 0
    for file in os.listdir(source):
        assigner[i % num_proc].append(file)
        i += 1
    return assigner

def clean(assignments):
    '''Clean excess headers based on assignments.'''
    source, target = get_dirs()
    for assignment in assignments:
        dirty = source + assignment
        clean = target + assignment
        subprocess.run([PYTHON, SCRIPT, dirty, clean])
    exit(0)

if __name__ == '__main__':
    if not (3 <= len(sys.argv) <= 4):
        print(f'{TAG} usage: {USAGE}', file=sys.stderr)
        exit(1)
    if len(sys.argv) == 3:
        num_proc = 1
    else:
        num_proc = int(sys.argv[3]) if sys.argv[3].isdigit() else proc_err()
    verify_cli()
    assignments = get_assignments(num_proc)
    processes = [None] * num_proc
    for i in range(num_proc):
        processes[i] = Process(target=clean, args=(assignments[i],))
        processes[i].start()
    for i in range(num_proc):
        processes[i].join()

