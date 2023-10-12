import uuid
import logging
import csv
import ast
import sys
import glob
import os
import dask.dataframe as dd
import pandas as pd
from post_utils.utils import write_to_file
from traceback import format_exc
from timeit import default_timer as timer


def init():
    '''Initialize input script.'''
    csv.field_size_limit(sys.maxsize)
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG,
            filemode='w') 


def load_scope(file):
    '''
    Loads the scope csv into a dictionary.
    Returns a dict of the scope with Source as key.
    '''
    # initialize script
    init()
    logging.info(f'loading scope {file}')
    # parse all the text aliases from each source using the scope file
    scope = {}
    i = 0
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file):
            name = line['Name'] if 'Name' in line.keys() else ''
            scope_type = line['Type'] if 'type' in line.keys() else ''
            alias, twitter, tag, pub, source = process_scope(scope, line)
            scope[source] = {'Name': name,
                             'Type': scope_type,
                             'Publisher': pub,
                             'Tags': tag,
                             'aliases': alias,
                             'twitter_handles': twitter}
            i = i + 1
    write_to_file(scope, './saved/processed_' + file.replace('./',
                  '').replace('.csv', '') + '.json')
    logging.info(f'loaded scope {file} with {i} lines')
    return scope


def process_scope(scope, line):
    '''Return the processed and important columns of the line in the scope.'''
    aliases, twitters, tags = [], [], []
    if 'Text Aliases' in line.keys() and line['Text Aliases']:
        aliases = line['Text Aliases'].split('|')
    else:
        aliases = []
    if 'Twitter Handle' in line.keys() and line['Twitter Handle']: 
        twitters = line['Twitter Handle'].split('|')
        for i in range(0, len(twitters)):
            twitters[i] = twitters[i].strip()
    else:
        twitters = []
    if 'Tags' in line.keys() and line['Tags']:
        tags = line['Tags']
    else:
        tags = ''
    try:
        publisher = line['Associated Publisher']
    except(Exception):
        publisher = ''
    try:
        source = line['Source']
        if source in scope.keys() or source == '':
            raise 'error'
    except(Exception):
        source = str(uuid.uuid4())
    return aliases, twitters, tags, publisher, source


def create_empty_twitter_dataframe():
    '''Return an empty twitter dataframe.'''
    empty_twitter_pd = {
        'id': [],
        'url': [],
        'id': [],
        'type': [],
        'tags': [],
        'author': [],
        'article_text': [],
        'date': [],
        'Mentions': [],
        'retweet_count': [],
        'reply_count': [],
        'like_count': [],
        'quote_count': [],
        'found_urls': [],
        'title': [],
        'domain': [],
        'completed': []
    }
    df = pd.DataFrame.from_dict(empty_twitter_pd)
    return dd.from_pandas(df, npartitions=1)


def get_mentions(row):
    '''Return the mentions of the row and retweet, reply, like, and quote
    counts using a tuple.'''
    mentions = []
    public_metrics = 0
    reply_count = 0
    like_count = 0
    quote_count = 0
    if (not pd.isna(row['entities'])):
        entities = ast.literal_eval(row.entities)
        if ('mentions' in entities):
            for mention in entities['mentions']:
                mentions.append(mention['username'])
    if (not pd.isna(row['public_metrics'])):
        public_metrics = ast.literal_eval(row.public_metrics)
        retweet_count = public_metrics['retweet_count']
        reply_count = public_metrics['reply_count']
        like_count = public_metrics['like_count']
        quote_count = public_metrics['quote_count']
    return str(mentions), retweet_count, reply_count, like_count, quote_count


def create_id(row):
    '''Return a unique id for the row.'''
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, row.tweet_url))


def read_twitter(path):
    '''Read the csv data at path and return the dataframe.'''
    df = dd.read_csv(path)
    dd.to_datetime(df['created_at'])
    return df


def load_twitter(path):
    '''Loads the twitter output csv from
    folder ./data_twitter/ into dask DataFrame
    Stores data to /saved/twitter_data.parquet.'''
    # Log the time for loading the twitter data
    twitter_timer = timer()
    logging.info(f'loading twitter data located at {path}')

    # Try reading the csv file at path
    try:
        twitter_df = read_twitter(path)
    except FileNotFoundError:
        logging.warning(f'did not find {path}, creating empty dataframe...')
        twitter_df = create_empty_twitter_dataframe()
    except:
        logging.error('error with twitter data at {path}\n' + format_exc())
        print(format_exc(), file=sys.stderr)
        exit(1)

    # Modify the read dataframe
    if len(twitter_df) > 0:
        logging.info(f'modifying {path} with {len(twitter_df)} records')

        # get Mentions
        twitter_df['retweet_count'] = 0
        twitter_df['reply_count'] = 0
        twitter_df['like_count'] = 0
        twitter_df['quote_count'] = 0
        twitter_df['Mentions'] = '[]'
        res_arr = twitter_df.apply(
            getMentions, axis=1, result_type='expand', meta={0: str, 
            1: str, 
            2: str, 
            3: str, 
            4: str})
        res_arr.columns = ['Mentions', 'retweet_count', 'reply_count', 'like_count', 'quote_count']
        res_pd = pd.DataFrame(res_arr, columns=[
            'Mentions',
            'retweet_count',
            'reply_count',
            'like_count',
            'quote_count'
        ], index=res_arr.index)
        data_pd = twitter_df.compute()
        data_pd.update(res_pd)

        twitter_df = dd.from_pandas(data_pd, npartitions=1)
        twitter_df = twitter_df.fillna(value='[]')

        # create new id with uuid
        twitter_df = twitter_df.drop(columns='id')

        twitter_df['id'] = twitter_df.apply(create_id, axis=1, meta=('id', 'str'))

        # rename, add empty and remove keys
        twitter_df = twitter_df.rename(columns={'citation_urls': 'found_urls', 'twitter_handle': 'domain',
                                                'created_at': 'date', 'text': 'article_text', 'tweet_url': 'url'})
        twitter_df = twitter_df.drop(columns=['author_id', 'referenced_tweets', 'public_metrics', 'referenced_entities',
                                              'in_reply_to_user_id', 'conversation_id', 'entities', 'lang', 'possibly_sensitive', 'withheld', 'tags'])
        twitter_df['type'] = 'twitter'
        twitter_df['title'] = ''
        twitter_df['author'] = ''
        twitter_df['completed'] = False
    else:
        twitter_df = create_empty_twitter_dataframe()

    # set url as the index
    twitter_df = twitter_df.set_index('url')

    # store the twitter data in saved and end logging
    twitter_df.to_parquet(
        './saved/twitter_data.parquet', engine='pyarrow')
    twitter_timer_end = timer()
    logging.info('time to read twitter files took ' + 
            str(twitter_timer_end - twitter_timer) + ' seconds')


def create_empty_domain_dataframe():
    '''Return an empty dataframe for domain data.'''
    empty_domain_pd = {
        'url': [],
        'title': [],
        'author': [],
        'date': [],
        'html_content': [],
        'article_text': [],
        'domain': [],
        'retweet_count': [],
        'reply_count': [],
        'like_count': [],
        'quote_count': [],
        'id': [],
        'found_urls': [],
        'completed': [],
        'type': [],
    }
    df = pd.DataFrame.from_dict(empty_domain_pd)
    return df


def load_domain(path):
    '''
    Loads the domain output csv from
    folder ./data_domain/ into a dictionary.
    Stores data to saved/domain_data.parquet.
    '''

    domain_timer = timer()
    logging.info(f'loading domain data located at {path}')

    all_files = glob.glob(os.path.join(path, '*.csv'))
    logging.info('given domains ' + str(list(all_files)))

    try:
        domain_df = pd.concat((pd.read_csv(f)
                              for f in all_files), ignore_index=True)
        domain_df = domain_df.fillna('')
        domain_df['type'] = 'article'
        domain_df['completed'] = False
        domain_df['retweet_count'] = 0
        domain_df['reply_count'] = 0
        domain_df['like_count'] = 0
        domain_df['quote_count'] = 0
    except:
        domain_df = create_empty_domain_dataframe()

    # store the domain data in saved
    domain_df = dd.from_pandas(domain_df, npartitions=1)
    domain_df = domain_df[['url', 'article_text', 'author', 'date', 'domain', 'found_urls',
                           'html_content', 'id', 'title', 'type', 'completed', 'retweet_count',
                           'reply_count', 'like_count', 'quote_count']]
    domain_df = domain_df.drop_duplicates(subset=['url'])
    domain_df = domain_df.set_index('url')
    domain_df.to_parquet('./saved/domain_data.parquet', engine='pyarrow')

    domain_timer_end = timer()
    logging.info('time to read domain files took ' +
            str(domain_timer_end - domain_timer) + ' seconds')

