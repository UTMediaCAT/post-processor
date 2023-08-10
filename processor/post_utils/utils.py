import ast
import json
import sys
PARSED_KEY = ['Mentions', 'found_urls']


def eprint(*args):
    '''Print contents (*args) to the standard error stream.'''
    print(*args, file=sys.stderr)


def write_to_file(dictionary, filename):
    '''Writes the dictionary to a json file with the given filename.'''
    # Serializing json
    json_object = json.dumps(dictionary, indent=4)

    # Writing to output.json
    with open(filename, 'w') as outfile:
        outfile.write(json_object)


def row_parser(row):
    '''Parse the cells in row with columns matching PARSED_KEY.'''
    row_dict = {}
    for key in row.keys():
        if key in PARSED_KEY:
            if type(row[key]) is not str:
                row_dict[key] = ast.literal_eval(row[key].values[0])
            else:
                row_dict[key] = ast.literal_eval(row[key])
        else:
            row_dict[key] = row[key]
    return row_dict

