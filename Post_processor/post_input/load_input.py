import uuid
import logging
import csv
import dask.dataframe as dd
from numpy import partition
import pandas as pd
import ast
from timeit import default_timer as timer
from post_utils.utils import write_to_file
import sys
import glob
import os
csv.field_size_limit(sys.maxsize)
logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')  # nopep8


def load_scope(file):
    """
    Loads the scope csv into a dictionary.
    Returns a dict of the scope with Source as key.
    """
    # parse all the text aliases from each source using the scope file
    scope = {}
    i = 0
    # format: {source: {aliases: [], twitter_handles:[]}}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file):
            aliases, twitters, tags = [], [], []
            if 'Text Aliases' in line.keys() and line['Text Aliases']:
                aliases = line['Text Aliases'].split('|')
            else:
                aliases = []
            if 'Twitter Handle' in line.keys() and line['Twitter Handle']:  # nopep8
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
            scope[source] = {'Name': line['Name'] if 'Name' in line.keys() else '',
                                    #  'RSS': line['RSS feed URLs (where available)'],  # nopep8
                                     'Type': line['Type'] if 'type' in line.keys() else '',
                                     'Publisher': publisher,
                                     'Tags': tags,
                                     'aliases': aliases,
                                     'twitter_handles': twitters}
            i = i + 1
    write_to_file(scope, "./Saved/processed_" + file.replace('./',
                  '').replace('.csv', '') + ".json")
    return scope


def create_empty_twitter_dataframe():
    empty_twitter_pd = {
        "id": [],
        "url": [],
        "id": [],
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


def load_twitter(path):
    '''Loads the twitter output csv from
    folder ./DataTwitter/ into dask DataFrame
    Stores data to /Saved/twitter_data.parquet.'''

    twitter_timer = timer()
    logging.info("loading twitter data")
    try:
        twitter_df = dd.read_csv(path, parse_dates=['created_at'])
        logging.info(len(twitter_df))

        # get Mentions
        def getMentions(row):
            Mentions = []
            public_metrics = 0
            reply_count = 0
            like_count = 0
            quote_count = 0
            if (not pd.isna(row['entities'])):
                entities = ast.literal_eval(row.entities)
                if ('mentions' in entities):
                    for mention in entities['mentions']:
                        Mentions.append(mention['username'])
            if (not pd.isna(row['public_metrics'])):
                public_metrics = ast.literal_eval(row.public_metrics)
                retweet_count = public_metrics['retweet_count']
                reply_count = public_metrics['reply_count']
                like_count = public_metrics['like_count']
                quote_count = public_metrics['quote_count']

            return str(Mentions), retweet_count, reply_count, like_count, quote_count

        twitter_df['retweet_count'] = 0
        twitter_df['reply_count'] = 0
        twitter_df['like_count'] = 0
        twitter_df['quote_count'] = 0
        twitter_df['Mentions'] = '[]'
        res_arr = twitter_df.apply(
            getMentions, axis=1, meta='object')
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

        def createId(row):
            return str(uuid.uuid5(uuid.NAMESPACE_DNS, row.tweet_url))
        twitter_df['id'] = twitter_df.apply(createId, axis=1, meta='object')

        # rename, add empty and remove keys
        twitter_df = twitter_df.rename(columns={'citation_urls': 'found_urls', 'twitter_handle': 'domain',
                                                'created_at': 'date', 'text': 'article_text', 'tweet_url': 'url'})
        twitter_df = twitter_df.drop(columns=['author_id', 'referenced_tweets', 'public_metrics', 'referenced_entities',
                                              'in_reply_to_user_id', 'conversation_id', 'entities', 'lang', 'possibly_sensitive', 'withheld', 'tags'])
        twitter_df['type'] = 'twitter'
        twitter_df['title'] = ''
        twitter_df['author'] = ''
        twitter_df['completed'] = False

    except:
        twitter_df = create_empty_twitter_dataframe()

    # set url as the index
    twitter_df = twitter_df.set_index('url')

    # store the twitter data in Saved
    twitter_df.to_parquet(
        './Saved/twitter_data.parquet', engine="pyarrow")

    twitter_timer_end = timer()
    logging.info("Time to read twitter files took " + str(twitter_timer_end - twitter_timer) + " seconds")  # nopep8


def create_empty_domain_dataframe():
    empty_domain_pd = {
        "url": [],
        "title": [],
        "author": [],
        "date": [],
        "html_content": [],
        "article_text": [],
        "domain": [],
        'retweet_count': [],
        'reply_count': [],
        'like_count': [],
        'quote_count': [],
        "id": [],
        "found_urls": [],
        "completed": [],
        "type": [],
    }
    df = pd.DataFrame.from_dict(empty_domain_pd)
    return df


def load_domain(path):
    """Loads the domain output csv from
    folder ./DataDomain/ into a dictionary.
    Stores data to Saved/domain_data.parquet."""

    domain_timer = timer()
    logging.info("loading domain data")

    all_files = glob.glob(os.path.join(path, "*.csv"))
    print(list(all_files))

    # doesn't create a list, nor does it append to one
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

    domain_df = domain_df.set_index('url')

    # store the domain data in Saved
    domain_df = dd.from_pandas(domain_df, npartitions=1)
    domain_df.to_parquet('./Saved/domain_data.parquet', engine="pyarrow")

    domain_timer_end = timer()
    logging.info("Time to read domain files took " + str(domain_timer_end - domain_timer) + " seconds")  # nopep8
