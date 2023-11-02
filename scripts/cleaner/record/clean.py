import csv
import os
import sys

ID='id'

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

def clean(path, target):
    '''
    Clean the CSV file located at path for any duplicate records.
    After it is cleaned, the newly cleaned CSV file will exist at target.
    '''
    unique = {}
    with open(path, 'r', newline='') as file:
        data = file.readlines()
        if len(data) == 0:
            exit(0)
        header = data[0].strip().split(',')

    with open(path, 'r', newline='') as file:
        reader = csv.DictReader(c.replace('\0', '') for c in file)
        for row in reader:
            if not row[ID] in unique:
                unique[row[ID]] = row

    with open(target, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=header, 
                                quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        ids = sorted(list(unique.keys()), reverse=True)
        for identifier in ids:
            writer.writerow(unique[identifier])

if __name__ == '__main__':
    if len(sys.argv) != 3:
        cli_error(len(sys.argv))
        exit(1)
    file, target = sys.argv[1], sys.argv[2]
    verify(file)
    clean(file, set_ext(target))

