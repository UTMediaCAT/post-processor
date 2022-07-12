import json
import ast
PARSED_KEY = ["Mentions", "found_urls"]


def write_to_file(dict, filename):
    """ Writes the dict to a json file with the given filename."""
    # Serializing json
    json_object = json.dumps(dict, indent=4)

    # Writing to output.json
    with open(filename, "w") as outfile:
        outfile.write(json_object)


def row_parser(row):
    row_dict = {}
    for key in row.keys():
        if key in PARSED_KEY:
            row_dict[key] = ast.literal_eval(row[key])
        else:
            row_dict[key] = row[key]
    return row_dict
