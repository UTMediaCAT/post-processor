import uuid
import logging
import csv
import ast
import sys
import glob
from urllib.parse import urlparse
import os
import os.path
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from post_utils.utils import json_to_csv
from post_utils.utils import compile_and_validate_csv
from post_utils.utils import write_to_file
from post_utils.utils import LogPlugin
from traceback import format_exc
from timeit import default_timer as timer


def init():
    '''
    Initialize input script.
    '''
    csv.field_size_limit(int(2**31 - 1))
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w') 
    os.makedirs('./logs/domain_load', exist_ok=True)
    os.makedirs('./logs/twitter_load', exist_ok=True)

def create_id(row):
    '''Return a unique id for the row.'''
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row.tweet_url))

def make_new_path(path, base):
    '''
    Returns the string path appended with the `base`
    Parameters:
      path: the path
      base: addition to the path
    '''
    return '/'.join(path.split('/')[:-1] + [base])

def load_scope(file):
    '''
    Makes a dictionary of the scope file (csv) with the following:
    'Name', 'Type', 'Text Aliases', 'Twitter Handle', 'Tags', 'Associated Publisher', 'Source'
    Parameters:
      file: the csv file path
    '''
    # initialize script
    init()
    logging.info(f'loading scope {file}')
    # parse all the text aliases from each source using the scope file
    scope = {}
    twitters_scope = {}
    i = 0
    with open(file, encoding='utf-8') as csv_file:
        # reads from csv and takes exactly 'Name', 'Type', 'Text Aliases', 'Twitter Handle', 'Tags', 'Associated Publisher', 'Source'
        for line in csv.DictReader(csv_file):
            name = line['Name'] if 'Name' in line.keys() else ''
            scope_type = line['Type'] if 'type' in line.keys() else ''
            aliases, twitters, tags, publisher, domain, source = process_scope(scope, line)
            if twitters:
                for twitter in twitters:
                    twitters_scope[twitter] = {
                        'Name': name,
                        'Tags': tags,
                        'Publisher': publisher,
                        'Source': source
                    }
            scope[domain] = {
                'Name': name,
                'Source': source,
                'Type': scope_type,
                'Publisher': publisher,
                'Tags': tags,
                'Aliases': aliases,
                'Twitter Handles': twitters
            }
            i += 1
    write_to_file(scope, './saved/processed_' + file.replace('./', '').replace('.csv', '') + '.json')
    write_to_file(twitters_scope, './saved/processed_twitter_scope.json')
    logging.info(f'loaded scope {file} with {i} lines')
    return scope, twitters_scope

def process_scope(scope, line):
    '''
    Process the scope line by line
    Returns the aliases, twitters, tags, publisher, and source of the scope entry
    Also returns the Domain and the associated path for the Domain
    Entry in the scope that do not have an associate URL will be replaced with an UUID
    Parameters:
      scope: the citation_scope dictionary
      line: the line parsed from the scope csv
    '''
    aliases, twitters, tags = [], [], []
    publisher, source = '', None
    if line['Text Aliases']:
      aliases = line['Text Aliases'].strip().split('|') if line['Text Aliases'][-1] != '|' else line['Text Aliases'][:-1].strip().split('|')
    if line['Associated Twitter Handle']:
      twitters = line['Associated Twitter Handle'].strip().split('|') if line['Associated Twitter Handle'][-1] != '|' else line['Associated Twitter Handle'][:-1].strip().split('|')
      for i in range(0, len(twitters)):
        twitters[i] = twitters[i].strip().replace('@', '')
    if line['Topic']:
        tags.append(line['Topic'])
    if line['Source or Referring 2']:
        tags.append(line['Source or Referring 2'])
    if line['Editorial Slant']:
        tags.append(line['Editorial Slant'])
    if line['Country']:
        tags.append(line['Country'])
    if line['Region']:
        tags.append(line['Region'])
    if line['Associated Publisher']:
        publisher = line['Associated Publisher']
    if line['Source']:
        source = line['Source'].strip()
        temp_source = source if "://" in source else "http://" + source
        parsed = urlparse(temp_source)
        domain = parsed.netloc
    else:
        domain = str(uuid.uuid4())
    return aliases, twitters, tags, publisher, domain, source

def create_empty_twitter_dataframe():
    '''
    Return an empty twitter dataframe. Just loads the dask dataframe with empty columns.
    '''
    empty_twitter_pd = {
        'URL': [],
        'Type': [],
        'Tags': [],
        'Author': [],
        'Article Text': [],
        'Date': [],
        'Mentions': [],
        'Retweet Count': [],
        'Reply Count': [],
        'Like Count': [],
        'Quote Count': [],
        'Found URLs': [],
        'Title': [],
        'Domain': []
    }
    df = pd.DataFrame.from_dict(empty_twitter_pd)
    return dd.from_pandas(df, npartitions=1)

def convert_twitter(path):
    '''
    Converts the existing Twitter datasets into a single CSV for Dask to load
    Converted data gets saved in ./data_twitter/data_twitter_csv/output.csv
    Parameters:
      path: path of the twitter data
    '''
    start = timer()
    logging.info(f'converting twitter data located at {path} to a single validated CSV')
    new_path = './twitter_input_csv'
    if os.path.isdir(new_path):
        removables = os.listdir(new_path)
        for removable in removables:
            if not "ipynb" in removable: 
                os.remove(f'{new_path}/{removable}')
    else:
        os.mkdir(new_path)
    # The actual function call to convert the data to a single csv
    compile_and_validate_csv(path, new_path)
    end = timer()
    logging.info(f'time to convert twitter {end - start}')

def load_twitter(path, args):
    '''
    Loads the twitter output csv from
    folder ./data_twitter/data_twitter_csv into dask DataFrame
    Stores data to ./saved/twitter_data.parquet.
    Parameters:
      path: path of the twitter data
      args: arguments for the program
    '''
    # Log the time for loading the twitter data
    if not args.ltcsv:
        convert_twitter(path)
        logging.info('Twitter conversion completed!')
    path = './twitter_input_csv'
    twitter_timer = timer()
    logging.info(f'loading twitter data located at {path}')

    # Initiate the Client Workers from Dask
    client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
    # Instantiate the plugin used for logging the info from each Client
    plugin = LogPlugin(logging, './logs/twitter_load/')
    # Register the plugin for each client to call
    client.register_plugin(plugin)
    # Attempt loading the data now
    try:
        # Loads the CSV into Dask. Usecols specify which CSV columns to use. Blocksize indicates in memory partition size
        twitter_df = dd.read_csv(path + '/output.csv',
                                  dtype={'ID': object},
                                  blocksize='50MB')
    except OSError:
        logging.warning(f'did not find files at {path}, creating empty dataframe...')
        twitter_df = create_empty_twitter_dataframe()
    except FileNotFoundError:
        logging.warning(f'did not find {path}, creating empty dataframe...')
        twitter_df = create_empty_twitter_dataframe()
    except:
        logging.error('error with twitter data at {path}\n' + format_exc())
        print(format_exc(), file=sys.stderr)
        exit(1)

    # Modify and Save the read CSV into a .parquet for Dask
    if len(twitter_df) > 0:
        logging.info(f'Modifying {path} with {len(twitter_df)} records')
        # Drop the duplicate URLs if any
        twitter_df = twitter_df.dropna(subset=['Referring URL'])
        # Set index to be the URL for easier query later
        twitter_df = twitter_df.set_index('Referring URL')

    twitter_df.to_parquet('./saved/twitter_data.parquet', engine='pyarrow')
    # Close the worker instances
    client.close()
    twitter_timer_end = timer()
    logging.info('Reading Twitter data completed!')
    logging.info('Time to read twitter files took ' + str(twitter_timer_end - twitter_timer) + ' seconds')

def create_empty_domain_dataframe():
    '''
    Return an empty dataframe for domain data.
    '''
    empty_domain_pd = {
        'URL': [],
        'Title': [],
        'Author': [],
        'Date': [],
        'Domain': [],
        'ID': [],
        'Found URLs': [],
        'Type': [],
    }
    df = pd.DataFrame.from_dict(empty_domain_pd)
    return dd.from_pandas(df, npartitions=1)

def convert_domain(path):
    '''
    Convert the domain JSON data files to CSV files.
    Parameters:
      path: the path of the domain data
    '''
    start = timer()
    logging.info(f'converting domain data located at {path} to CSV')
    new_path = './domain_input_csv'
    if os.path.isdir(new_path):
        removables = os.listdir(new_path)
        for removable in removables:
            if not "ipynb" in removable: 
                os.remove(f'{new_path}/{removable}')
    else:
        os.mkdir(new_path)
    # Actual function for conversion
    json_to_csv(path, new_path)
    end = timer()
    logging.info(f'time to convert domains {end - start}')

def load_domain(path, args):
    '''
    Loads the domain output csv from
    folder ./data_domain/ into a dictionary.
    Stores data to saved/domain_data.parquet.
    Parameters:
      path: the path of the domain data
      args: arguments for the program
    '''
    if not args.ldcsv:
        convert_domain(path) 
        logging.info('Domain conversion completed!')
    path = './domain_input_csv'
    domain_timer = timer()
    logging.info(f'loading domain data located at {path}')

    # Initiate the Client Workers from Dask
    client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
    # Instantiate the plugin used for logging the info from each Client
    plugin = LogPlugin(logging, './logs/domain_load/')
    # Register the plugin for each client to call
    client.register_plugin(plugin)
    # Attemp loading the data now
    try:
        # Loads the CSV into Dask. Usecols specify which CSV columns to use. Blocksize indicates in memory partition size
        domain_df = dd.read_csv(path + '/output.csv', dtype={'ID': int,
                                                             'Title': object,
                                                             'Referring URL': object,
                                                             'Author': object,
                                                             'Date': object,
                                                             'Domain': object,
                                                             'Found URLs': object,
                                                             'Article Text': object
                                                             },
                                                             blocksize='50MB')
    except OSError:
        logging.warning(f'did not find files at {path}, creating empty dataframe...')
        domain_df = create_empty_domain_dataframe()
    except FileNotFoundError:
        logging.warning(f'did not find {path}, creating empty dataframe...')
        domain_df = create_empty_domain_dataframe()
    except:
        logging.error('error with domain data at {path}\n' + format_exc())
        print(format_exc(), file=sys.stderr)
        exit(1)
    
    # Modify and Save the read CSV into a .parquet for Dask
    if len(domain_df) > 0:
        logging.info(f'Modifying {path} with {len(domain_df)} records')
        # Drop the duplicate URLs if any
        domain_df = domain_df.drop_duplicates(subset=['Referring URL'])
        # Set index to be the URL for easier query later
        domain_df = domain_df.set_index('Referring URL')

    domain_df.to_parquet('./saved/domain_data.parquet', engine='pyarrow')
    # Close the worker instances
    client.close()
    domain_timer_end = timer()
    logging.info('Reading Domain data completed!')
    logging.info('Time to read domain files took ' + str(domain_timer_end - domain_timer) + ' seconds')

