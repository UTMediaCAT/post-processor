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

def json_to_csv(json_dir: str, output_dir: str, filename: str):
    '''Converts a directory containing JSON data to a singular CSV file.
    Credit: Aryan Goel 2024'''
    data = []
    for filename in os.listdir(json_dir):
        if filename.endswith(".json"):
            with open(os.path.join(json_dir, filename), "r") as file:
                json_data = json.load(file)
                data.append(json_data)
    column_names = ['id', 'title', 'url', 'html_content', 'author', 'date', 'article_text', 'domain', 'updated', 'found_urls']
    csv_filename = os.path.join(output_folder, "output.csv")  # Specify the path for the output CSV file
    with open(csv_filename, "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        for i, item in enumerate(data):
            item['id'] = i
            item['html_content'] = item['bodyHTML']
            del item['bodyHTML']
            writer.writerow(item)

