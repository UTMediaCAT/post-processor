import sys
from os import getcwd
from os import mkdir
from os.path import abspath
from os.path import exists
from os.path import join
from shutil import rmtree
from shutil import copytree
from subprocess import run
from subprocess import CalledProcessError

TAG = '[MediaCAT]'
USAGE = 'python clean.py dir [num_proc]'
NUM_PROC = 4
PATH = abspath(getcwd())

def perror(*args):
    '''Print args to std err stream.'''
    print(*args, file=sys.stderr)

def verify_directory(directory):
    '''
    Verify that directory is a valid directory in the cwd.
    Return the name of the directory if valid, otherwise
    exit the program.
    '''
    if exists(directory):
        return directory
    perror(TAG, f'{directory} not a valid directory')
    exit(1)

def create_src(directory):
    '''
    Create a source directory for the uncleaned CSV files.
    Return the name of this source directory.
    '''
    old = f'old_{directory}'
    path = join(PATH, old)
    if exists(path):
        rmtree(path)
    else:
        mkdir(path)
    copytree(directory, path)

def run_header(src, directory, n_proc):
    '''Run the header multiclean.py script.'''
    try:
        cmd = ['python3', 'multiclean.py', src, directory, n_proc]
        process = run(cmd, cwd=join(PATH, 'header'))
        process.check_returncode()
    except CalledProcessError:
        exit(1)
    except:
        perror(TAG, 'failed to run header script')
        exit(1)

def run_record(src, directory, n_proc):
    '''Run the record mutliclean.py script.'''
    try:
        cmd = ['python3', 'multiclean.py', src, directory, n_proc]
        process = run(cmd, cwd=join(PATH, 'record'))
        process.check_returncode()
    except CalledProcessError:
        exit(1)
    except:
        perror(TAG, 'failed to run record script')
        exit(1)

if __name__ == '__main__':
    if not (2 <= len(sys.argv) <= 3):
        perror(TAG, 'expected usage:', USAGE)
        exit(1)
    directory = verify_directory(sys.argv[1])
    processes = sys.argv[2] if len(sys.argv) == 3 else NUM_PROC
    src = create_src(directory)
    run_header(src, directory, processes)
    run_record(src, directory, processes)

