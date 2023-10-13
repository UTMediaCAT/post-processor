import os
import sys

def eprint(*args):
    '''Print contents (*args) to standard error stream.'''
    print(*args, file=sys.stderr)

def cli_error(nargs):
    '''Display error messages based on number of arguments (nargs).'''
    eprint('[MediaCAT] expected usage: python3 clean.py <old file> <new file>')
    if nargs == 1:
        eprint('[MediaCAT] missing args: <old file> <new file>')
    elif nargs == 2:
        eprint('[MediaCAT] missing arg: <new file>')
    else:
        eprint('[MediaCAT] error: too many args')

def verify(path):
    '''Exit gracefully if a CSV file does not exist at path.'''
    if not (os.path.exists(path) and os.path.splitext(path)[1] == '.csv'):
        if os.path.exists(path):
            eprint(f'[MediaCAT] {path} is not a csv file')
        else:
            eprint(f'[MediaCAT] {path} does not exist') 
        exit(1)

def set_ext(target):
    '''Set the target's file extension to be .csv if it is not already.'''
    if os.path.splitext(target)[1] != '.csv':
        target += '.csv'
    return target

def del_dups(data):
    '''Delete any duplicate headers in data.'''
    header = data[0]
    while header in data:
        data.remove(header)
    data.insert(0, header)

def clean(path, target):
    '''
    Clean the CSV file located at path for any duplicate headers.
    After it is cleaned, the newly cleaned CSV file will exist at target.
    '''
    with open(path, 'r') as file:
        data = file.readlines()
        if data:
            del_dups(data)
    with open(target, 'w') as file:
        file.write(''.join(data))

if __name__ == '__main__':
    if len(sys.argv) != 3:
        cli_error(len(sys.argv))
        exit(1)
    file, target = sys.argv[1], sys.argv[2]
    verify(file)
    clean(file, set_ext(target))

