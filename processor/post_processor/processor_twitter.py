import logging
import re
import ast
import sys
import os
from urllib.parse import urlparse
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from post_utils.utils import row_parser
from timeit import default_timer as timer
from post_utils.utils import LogPlugin

image_pattern = re.compile(r'\.(jpg|jpeg|png|gif|bmp|svg|webp)(\?.*)?$', re.IGNORECASE)

def init():
    '''
    Initialize twitter script.
    '''
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')
    os.makedirs('./logs/twitter_processing', exist_ok=True)
    os.makedirs('./logs/twitter_referral_processing', exist_ok=True)

def find_twitter_citation_aliases(tweet, scope, twitters):
    '''
    Find and return the twitter citation aliases based on 
    a given tweet that satisfies the scope.
    Parameters:
      tweet: the exact row of the tweet
      scope: the citation scope dictionary
      twitters: the twitter dictionary extract from citation_scope
    '''
    cited_url_or_text_aliases = []
    cited_name = []
    cited_tags = []
    cited_publisher = []

    # Go through the found urls of the twitter post
    for url in tweet['Found URLs']:
        # Skip any image
        if image_pattern.search(url):
            continue
        temp_url = url if "://" in url else "http://" + url # To make it compatible for urlparse()
        parsed = urlparse(temp_url)
        domain = parsed.netloc
        # If its a twitter url then check the twitter handle dictionary
        if domain in ('twitter.com', 'www.twitter.com'):
            path_split = parsed.path.split('/')
            if len(path_split) < 2:
                continue
            tweet_handle = path_split[1]
            if tweet_handle == tweet['Domain']:
                continue
            info = twitters.get(tweet_handle)
            if info and url not in cited_url_or_text_aliases:
                cited_url_or_text_aliases.append(url)
                cited_name.append(info['Name'])
                cited_tags.append(info['Tags'])
                cited_publisher.append(info['Publisher'])
            continue
        # If its a domain then check the citation dictionary
        entry = scope.get(domain)
        if not entry and domain.startswith('www.'):
            entry = scope.get(domain[4:])
        if entry and tweet['Domain'] not in entry['Twitter Handles'] and url not in cited_url_or_text_aliases:
            cited_url_or_text_aliases.append(url)
            cited_name.append(entry['Name'])
            cited_tags.append(entry['Tags'])
            cited_publisher.append(entry['Publisher'])
    
    # Check the mentions of the tweets
    for mention in tweet['Mentions']:
        # If the tweet mention is referring to poster then skip
        if mention == tweet['Domain']:
            continue
        info = twitters.get(mention)
        mention = '@' + mention
        if info and mention not in cited_url_or_text_aliases:
            cited_url_or_text_aliases.append(mention)
            cited_name.append(mention)
            cited_tags.append(mention)
            cited_publisher.append(mention)
    
    # Skip if tweet has no content
    if not tweet['Article Text']:
        return str(cited_url_or_text_aliases)[1:-1], str(cited_name)[1:-1], str(cited_publisher)[1:-1], str(cited_tags)[1:-1]
    
    # Check for any existing aliases within the twitter post
    for domain, info in scope.items():
        if tweet['Domain'] in info['Twitter Handles']:
            continue
        for alias in info['Aliases']:
            if not alias:
                continue
            pattern = r"(?:\s|^|\"|')(" + re.escape(alias) + r")(?=\s|$|\"|'|,)"
            if re.search(pattern, tweet['Article Text'], re.IGNORECASE) and alias not in cited_url_or_text_aliases:
                cited_url_or_text_aliases.append(alias)
                cited_name.append(info['Name'])
                cited_tags.append(info['Tags'])
                cited_publisher.append(info['Publisher'])
    return str(cited_url_or_text_aliases)[1:-1], str(cited_name)[1:-1], str(cited_publisher)[1:-1], str(cited_tags)[1:-1]

def get_twitter_handle_info(tweet, twitter_scope):
    '''
    Return the twitter handle information based
    on the tweet and citation scope.
    Parameters:
      tweet: the tweet entry from data frame
      twitter_scope: the twitter dictionary extract from citation_scope
    '''
    publisher = ''
    tags = '[]'
    name = ''
    handle = twitter_scope.get(tweet['Domain'])
    if handle:
        publisher = handle['Publisher']
        tags = str(handle['Tags'])[1:-1]
        name = handle['Name']
    return publisher, tags, name

def tweet_helper(tweet, citation_scope, twitter_scope):
    '''
    Helper script for twitter processor based on tweet,
    and citation_scope.
    Parameters:
      tweet: the tweet entry from data frame
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter dictionary extract from citation_scope
    '''
    tweet = row_parser(tweet)
    cited_url_or_text_aliases, cited_name, cited_publisher, cited_tags = find_twitter_citation_aliases(
        tweet, citation_scope, twitter_scope)
    publisher, tags, name = get_twitter_handle_info(tweet, twitter_scope)
    res = [
        cited_url_or_text_aliases,
        cited_name,
        cited_publisher,
        cited_tags,
        name,
        publisher,
        tags
    ]
    return res

def process_partition(df, citation_scope, twitter_scope):
    '''
    Process each partition of the data frame
    Parameters:
      df: a partition from the twitter data frame
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter dictionary extract from citation_scope
    '''
    for i, row in df.iterrows():
        res_arr = tweet_helper(row, citation_scope, twitter_scope)
        df.loc[i, ['Cited URLs or Text Aliases',
                   'Cited Names',
                   'Cited Associated Publishers',
                   'Cited Tags',
                   'Referring Name',
                   'Referring Associated Publisher',
                   'Referring Tags'
                  ]] = res_arr
    return df

def process_referral(df):
    '''
    Process referral using the 'Found URLs' column.
    Constructs the referrals via a dictionary of lists with the key being
    all the links and aliases referred to by the particular row. Then each
    of the links and aliases from the row will include this current row's URL
    address in its referrals list. 
    Returns a simple dataframe with the source and domains. `Domains` refer to the
    list of sites that referenced a particular `Source`.
    Parameters:
      df: a partition from the twitter data frame
    '''
    referrals = {}
    for i, row in df.iterrows():
        for link in ast.literal_eval(row['Found URLs']):
            if link not in referrals:
                referrals[link] = []
            referrals[link].append(i)
    for source in referrals:
        referrals[source] = str(referrals[source])
    return pd.DataFrame(list(referrals.items()), columns=['Source', 'Domains'])

def merge_domains(series):
    '''
    Aggregate function responsible for merging grouped sources' domains entry into a single
    row.
    Parameters:
      series: an aggregated list series from dask
    '''
    merged = []
    for subseries in series:
        if type(subseries) is str:
            if subseries.startswith('[') and subseries.endswith(']'):
                elements = re.findall(r"'(.*?)'", subseries)
                merged.extend(elements)
        for item in subseries:
            if item.startswith('[') and item.endswith(']'):
                elements = re.findall(r"'(.*?)'", item)
                merged.extend(elements)
    return str(merged)

def process_twitter(citation_scope, twitter_scope, args):
    '''
    Processes the twitter data by finding all the twitter posts
    that are referring to it and additionally doing cross-matching with 
    the scopes to add additional info to the twitter data.
    Parameters:
        citation_scope: the citation scope dictionary
        twitter_scope: the twitter dictionary extract from citation_scope
        args: arguments for the program
    '''
    # initialize script
    init()
    # process twitter
    try:
        logging.info('processing twitter')
        start = timer()
        client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
        if not args.ppt:
            plugin = LogPlugin(logging, './logs/twitter_processing/')
            client.register_plugin(plugin, name='logger')

            # load twitter_data from saved
            data_partitions = dd.read_parquet('./saved/twitter_data.parquet')
            if (len(data_partitions) == 0):
                data_partitions['Cited URLs or Text Aliases'] = ''
                data_partitions['Cited Names'] = ''
                data_partitions['Cited Associated Publishers'] = ''
                data_partitions['Cited Tags'] = ''
                data_partitions['Referring Associated Publisher'] = ''
                data_partitions['Referring Tags'] = ''
                data_partitions['Referring Name'] = ''
                empty_pd = pd.DataFrame()
                return dd.from_pandas(empty_pd, npartitions=1), data_partitions

            logging.info('process twitter data with {} rows and {} partitions'.format(len(data_partitions), data_partitions.npartitions))

            # Declare Metadata types that are returned from a processing
            meta = data_partitions.dtypes.to_dict()
            meta.update({
              'Cited URLs or Text Aliases': 'object',
              'Cited Names': 'object',
              'Cited Associated Publishers': 'object',
              'Cited Tags': 'object',
              'Referring Name': 'object',
              'Referring Associated Publisher': 'object',
              'Referring Tags': 'object'
            })

            # Begin processing the partition
            data = data_partitions.map_partitions(process_partition, citation_scope, twitter_scope, meta=meta)
            data.to_parquet('./saved/processed_twitter_data.parquet', engine='pyarrow')
            logging.info('Finished Twitter processing')
            client.unregister_worker_plugin(name="logger")
            # Restarting the worker here to reset resources (in-case worker is still holding on)
            client.restart()
            
        plugin = LogPlugin(logging, './logs/twitter_referral_processing/')
        client.register_plugin(plugin)
        logging.info('Begin Twitter referral extraction')

        # Read the saved processed_twitter_data to avoid having second calculations here
        # Twitter referral processing begins here
        data = dd.read_parquet('./saved/processed_twitter_data.parquet')
        referrals = data.map_partitions(process_referral, meta={'Source': 'object', 'Domains': 'object'}).set_index('Source', drop=True)
        referral_concat = dd.Aggregation(
            name = 'referral_concat', 
            chunk = lambda x: x.aggregate(list), 
            agg = lambda x: x.aggregate(merge_domains)
        )
        referrals = referrals.groupby('Source').agg(referral_concat)
        referrals.to_parquet('./saved/twitter_referral_data.parquet', engine='pyarrow')
        client.close()
        end = timer() 
        logging.info(f'finished processing twitter')
        logging.info('processing twitter took ' + str(end - start) + ' seconds')
    except Exception:
        logging.warning('exception at processing twitter, data written to saved/')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        raise
