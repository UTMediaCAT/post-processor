import ast
import csv
import json
import os
import sys
import re
from bs4 import BeautifulSoup
import pandas as pd
from dask.distributed import WorkerPlugin

PARSED_KEY = ['Mentions', 'Found URLs']

class LogPlugin(WorkerPlugin):
    def __init__(self, logger, path):
        self.logger = logger
        self.path = path
    def setup(self, worker):
        self.logger.basicConfig(filename=self.path + worker.id + '.log', level=self.logger.DEBUG, filemode='w') 

def eprint(*args):
    '''
    Print contents (*args) to the standard error stream.
    Parameters:
      args: arguments
    '''
    print(*args, file=sys.stderr)


def write_to_file(dictionary, filename):
    '''
    Writes the dictionary to a json file with the given filename.
    Parameter:
      dictionary: the dictionary object
      filename: the file to write the dictionary to
    '''
    # Serializing json
    json_object = json.dumps(dictionary, indent=4)

    # Writing to output.json
    with open(filename, 'w') as outfile:
        outfile.write(json_object)


def row_parser(row):
    '''
    Parse the cells in row with columns matching PARSED_KEY.
    Outputs a extracted dictionary of 'Mentions' and 'found_urls' specially treated
    Puts other data back into a dict returned
    Parameter:
      row: the particular row from panda dataframe
    '''
    row_dict = {}
    for key in row.keys():
        try:
            if pd.isna(row[key]):
                row_dict[key] = []
            elif key in PARSED_KEY:
                if type(row[key]) is not str:
                    row_dict[key] = ast.literal_eval(row[key].values[0])
                else:
                    row_dict[key] = ast.literal_eval(row[key])
            else:
                row_dict[key] = row[key]
        except Exception:
            row_dict[key] = []
    return row_dict

def json_to_csv(json_dir: str, output_dir: str):
    '''
    Convert JSON data into a singular CSV for domain load
    Parameter:
      json_dir: directory where all the json data is located
      output_dir: the output directory for the resultant csv
    '''
    column_names = ['ID', 'Title', 'Referring URL', 'Article Text', 'Author', 'Date', 'Domain', 'Found URLs']
    image_suffixes = re.compile(r'\.(png|jpg|jpeg|gif|bmp|svg|webp)/?$')
    csv_filename = os.path.join(output_dir, "output.csv")  # Specify the path for the output CSV file
    with open(csv_filename, "w", newline="", encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=column_names)
        writer.writeheader()
        i = 0 
        for root, dirs, files in os.walk(json_dir):
            for filename in files:
                if filename.endswith('.json'):
                    file_path = os.path.join(root, filename)
                    with open(file_path, "r", encoding='utf-8') as file:
                        output_row = {}
                        json_data = json.load(file)
                        if image_suffixes.search(json_data['url']):
                            continue
                        if 'bodyHTML' not in json_data:
                            continue
                        output_row['ID'] = i
                        output_row['Title'] = json_data['title']
                        output_row['Referring URL'] = json_data['url']
                        if json_data['article_text']:
                            output_row['Article Text'] = json_data['article_text'].replace('\n', ' ').strip()
                        else:
                            soup = BeautifulSoup(json_data['bodyHTML'], features="html.parser")
                            for content in soup(['script', 'style']):
                                content.decompose()
                            output_row['Article Text'] = soup.get_text().replace('\n', ' ').strip()
                        output_row['Author'] = json_data['author']
                        output_row['Date'] = json_data['date']
                        output_row['Domain'] = json_data['domain']
                        output_row['Found URLs'] = json_data['found_urls']
                        writer.writerow(output_row)
                        i += 1

def compile_and_validate_csv(csv_dir: str, output_dir: str):
    '''
    Convert CSV data into a singular CSV for twitter load
    Parameter:
      csv_dir: directory where all the csv data is located
      output_dir: the output directory for the resultant csv
    '''
    column_names = ['ID', 'Referring URL', 'Article Text', 'Date', 'Domain', 'Found URLs', 'Retweet Count', 'Reply Count', 'Like Count', 'Quote Count', 'Mentions']
    column_regex = {
        'found_urls': r'^\[.*\]$',
        'entities': r'^\{.*\}$',
        'public_metrics': r"\{'retweet_count':\s*\d+,\s*'reply_count':\s*\d+,\s*'like_count':\s*\d+,\s*'quote_count':\s*\d+(?:,\s*'impression_count':\s*\d+)?\}",
    }
    csv_file = os.path.join(output_dir, "output.csv")
    with open(csv_file, 'w', newline="", encoding='utf-8') as output:
        writer = csv.DictWriter(output, fieldnames=column_names)
        writer.writeheader()
        for root, dirs, files in os.walk(csv_dir):
            for filename in files:
                if filename.endswith('.csv'):
                    file_path = os.path.join(root, filename)
                    with open(file_path, 'r', encoding='utf-8-sig') as file:
                        reader = csv.DictReader(file)
                        try:
                            for row in reader:
                                output_row = {}
                                if not row.get('id'):
                                    continue
                                output_row['ID'] = str(row['id'])
                                if not row.get('tweet_url'):
                                    continue
                                output_row['Referring URL'] = row['tweet_url']
                                output_row['Article Text'] = row['text'].replace('\n', ' ')
                                output_row['Date'] = row['created_at']
                                output_row['Domain'] = row['twitter_handle']
                                if row.get('citation_urls') and re.match(column_regex['found_urls'], row['citation_urls']):
                                    output_row['Found URLs'] = row['citation_urls']
                                else:
                                    output_row['Found URLs'] = "[]"
                                if row.get('entities') and re.match(column_regex['entities'], row['entities']):
                                    entities = ast.literal_eval(row['entities'])
                                    output_row['Mentions'] = []
                                    if 'mentions' in entities:
                                        for mention in entities['mentions']:
                                            output_row['Mentions'].append(mention['username'])
                                    output_row['Mentions'] = str(output_row['Mentions'])
                                    if 'urls' in entities:
                                        for url in entities['urls']:
                                            output_row['Article Text'] = output_row['Article Text'].replace(url['url'], url['expanded_url'])
                                else:
                                    output_row['Mentions'] = "[]"
                                if row.get('public_metrics') and re.match(column_regex['public_metrics'], row['public_metrics']):
                                    public_metrics = ast.literal_eval(row['public_metrics'])
                                    output_row['Reply Count'] = public_metrics['reply_count']
                                    output_row['Like Count'] = public_metrics['like_count']
                                    output_row['Quote Count'] = public_metrics['quote_count']
                                    output_row['Retweet Count'] = public_metrics['retweet_count']
                                else:
                                    output_row['Reply Count'] = 0
                                    output_row['Like Count'] = 0
                                    output_row['Quote Count'] = 0
                                    output_row['Retweet Count'] = 0
                                writer.writerow(output_row)
                        except Exception as e:
                            eprint(f"Error reading {filename}: {e}")
