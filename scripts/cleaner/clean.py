import sys
from os import getcwd
from os import mkdir
from os.path import abspath
from os.path import dirname
from os.path import exists
from os.path import join
from os.path import realpath
from shutil import copytree
from shutil import move
from shutil import rmtree
from subprocess import run
from subprocess import CalledProcessError

TAG = '[MediaCAT]'
USAGE = 'python3 clean.py dir [num_proc]'
NUM_PROC = 4
PATH = abspath(getcwd())
SCRIPT_PATH = dirname(realpath(__file__))
INVALID = {'header', 'record', 'old', 'temp'}

def perror(*args):
    '''Print args to std err stream.'''
    print(*args, file=sys.stderr)

def verify_directory(directory):
    '''
    Verify that directory is a valid directory in the cwd.
    Return the name of the directory if valid, otherwise
    exit the program.
    '''
    if directory in INVALID:
        perror(TAG, f'{directory} cannot be used')
        exit(1)
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
    copytree(directory, path)
    return path

def run_operations(src, directory, n_proc):
    '''Run all the cleaning operations.'''
    temp = join(PATH, 'temp')
    if exists(temp):
        rmtree(temp)
    copytree(src, temp)
    run_header(src, temp, n_proc)
    run_record(temp, directory, n_proc)
    rmtree(temp)

def run_header(src, directory, n_proc):
    '''Run the header multiclean.py script.'''
    try:
        cmd = ['python3', 'multiclean.py', src, directory, n_proc]
        process = run(cmd, cwd=join(SCRIPT_PATH, 'header'))
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
        process = run(cmd, cwd=join(SCRIPT_PATH, 'record'))
        process.check_returncode()
    except CalledProcessError:
        exit(1)
    except:
        perror(TAG, 'failed to run record script')
        exit(1)

def move_old(directory):
    '''Move the old directory files to the old directory.'''
    old = join(PATH, 'old')
    old_dir = join(PATH, f'old_{directory}')
    if not exists(old):
        mkdir(old)
    move(old_dir, old)
    i = 0
    moved = False
    while not moved:
        new_dir = join(old, f'{directory}_{i}')
        if not exists(new_dir):
            move(join(old, f'old_{directory}'), new_dir)
            moved = True
        i += 1

def terminate():
    '''Exit the process with a message and error code 0.'''
    print(TAG, 'Automated cleaner completed successfully')
    exit(0)

if __name__ == '__main__':
    if not (2 <= len(sys.argv) <= 3):
        perror(TAG, 'expected usage:', USAGE)
        exit(1)
    directory = verify_directory(sys.argv[1])
    processes = sys.argv[2] if len(sys.argv) == 3 else str(NUM_PROC)
    if not processes.isdigit():
        perror(TAG, 'optional arg num_proc NaN')
    src = create_src(directory)
    run_operations(src, join(PATH, directory), processes)
    move_old(directory)
    terminate()

