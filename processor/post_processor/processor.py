import ast
import os
import logging
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client
from post_processor.processor_domain import process_domain
from post_processor.processor_twitter import process_twitter
from timeit import default_timer as timer
from post_utils.utils import LogPlugin
import pyarrow as pa

def init():
    '''
    Initialize processor script.
    '''
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')
    os.makedirs('./logs/processing', exist_ok=True)

def parse_referrals(df, domain_referrals, twitter_referrals):
    '''
    Cross-match the referrals for domain referrals and twitter referrals.
    Returns a list of all sources that are referring to this specific article.
    Uses the df's url index to check each domain and twitter referral for the
    list of domain entries to add to the 'referring name' column.
    Parameters:
      df: the dask dataframe partition
      domain_referrals: a data frame of all the referrals in the domain data
      twitter_referrals: a data frame of all the referrals in the twitter data
    '''
    for i, row in df.iterrows():
        referring_articles_set = set()
        try:
            data = domain_referrals.loc[i]['Domains']
            if data:
              referring_articles_set.update(ast.literal_eval(data))
        except Exception:
            pass

        try:
            data = twitter_referrals.loc[i]['Domains']
            if data:
              referring_articles_set.update(ast.literal_eval(data))
        except Exception:
            pass
        referring_articles_set.discard(i)
        referring_articles = list(referring_articles_set)
        df.loc[i, 'Cited by URLs'] = str(referring_articles)[1:-1]
        df.loc[i, 'Number of Cited by URLs'] = len(referring_articles)
    return df

def process_crawler(citation_scope, twitter_scope, args):
    '''
    The main point of entry for the processor.
    Calls the domain and twitter processor seperately.
    Performs the referral extraction for domain and twitter.
    Also performs the referral cross matching for both domain and twitter.
    Parameters:
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter scope dictionary
      args: arguments for the program
    '''
    # initialize script
    init()
    # process crawling results
    if not args.pd:
        process_domain(citation_scope, twitter_scope, args)
        logging.info('Completed Domain Processing and Referral Generation')
    if not args.pt:
        process_twitter(citation_scope, twitter_scope, args)
        logging.info('Completed Twitter Processing and Referral Generation')
    logging.info('processing referrals - cross matching')
    # Start the dask client workers
    client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
    plugin = LogPlugin(logging, './logs/processing/')
    client.register_plugin(plugin)

    # Begin reading the processed data
    domain_data = dd.read_parquet('./saved/processed_domain_data.parquet')
    domain_referrals = dd.read_parquet('./saved/domain_referral_data.parquet')
    twitter_referrals = dd.read_parquet('./saved/twitter_referral_data.parquet')
    twitter_data = dd.read_parquet('./saved/processed_twitter_data.parquet')
    scaled_domain = False # For rescaling purposes in-case the parquet becomes far smaller than it needs to

    # Checking partitions matching. This is so .map_partition() don't throw errors regarding repeated operations when
    # attempting to cross map with the domain and twitter referrals. Mis-match partitions when using
    # dataframe as parameters causes issues in dask. 
    # Ensure: domain_data.npartitions = twitter_referrals.npartitions = domain_referrals.npartitions = twitter_data.npartitions
    if domain_data.npartitions >= twitter_data.npartitions:
        twitter_data = twitter_data.repartition(npartitions=domain_data.npartitions)
        twitter_referrals = twitter_referrals.repartition(npartitions=domain_data.npartitions)
        domain_referrals = domain_referrals.repartition(npartitions=domain_data.npartitions)
    else:
        scaled_domain = True # Rescale the domain if the partitions get spanned to more than it needs to
        domain_data = domain_data.repartition(npartitions=twitter_data.npartitions)
        domain_referrals = domain_referrals.repartition(npartitions=twitter_data.npartitions)
        twitter_referrals = twitter_referrals.repartition(npartitions=twitter_data.npartitions)
    
    logging.info('processing referrals - for domain matching')
    logging.info(f'domain columns: {domain_data.columns}')

    ### Add referrals to domain_data ### 
    meta = domain_data.dtypes.to_dict()
    meta.update({
      'Cited by URLs': 'object',
      'Number of Cited by URLs': 'int64'
    })
    domain_data = domain_data.map_partitions(parse_referrals, domain_referrals, twitter_referrals, meta=meta)
    if scaled_domain:
        # Rescale domain to have 50MB partitions
        domain_data = domain_data.repartition(partition_size='50MB')
    domain_data.to_parquet('./saved/final_processed_domain_data.parquet', engine='pyarrow', schema={
      'ID': pa.int64(),
      'Title': pa.large_string(),
      'Referring URL': pa.large_string(),
      'Author': pa.large_string(),
      'Date': pa.large_string(),
      'Domain': pa.large_string(),
      'Found URLs': pa.large_string(),
      'Article Text': pa.large_string(),
      'Cited URLs or Text Aliases': pa.large_string(),
      'Cited Names': pa.large_string(),
      'Cited Associated Publishers': pa.large_string(),
      'Cited Tags': pa.large_string(),
      'Anchor Text': pa.large_string(),
      'Referring Name': pa.large_string(),
      'Referring Associated Publisher': pa.large_string(),
      'Referring Tags': pa.large_string(),
      'Cited by URLs': pa.large_string(),
      'Number of Cited by URLs': pa.int64()
    })

    logging.info('processing referrals - for twitter matching')
    logging.info(f'twitter columns: {twitter_data.columns}')

    ### Add referrals to twitter_data ###
    meta = twitter_data.dtypes.to_dict()
    meta.update({
      'Cited by URLs': 'object',
      'Number of Cited by URLs': 'int64'
    })
    twitter_data = twitter_data.map_partitions(parse_referrals, domain_referrals, twitter_referrals, meta=meta)
    if not scaled_domain:
        # Rescale twitter to have 50MB partitions
        twitter_data = twitter_data.repartition(partition_size='50MB')
    twitter_data.to_parquet('./saved/final_processed_twitter_data.parquet', engine='pyarrow', schema={
      'Referring URL': pa.large_string(),
      'ID': pa.large_string(),
      'Article Text': pa.large_string(),
      'Date': pa.large_string(),
      'Domain': pa.large_string(),
      'Found URLs': pa.large_string(),
      'Retweet Count': pa.int64(),
      'Like Count': pa.int64(),
      'Reply Count': pa.int64(),
      'Quote Count': pa.int64(),
      'Mentions': pa.large_string(),
      'Cited URLs or Text Aliases': pa.large_string(),
      'Cited Names': pa.large_string(),
      'Cited Associated Publishers': pa.large_string(),
      'Cited Tags': pa.large_string(),
      'Referring Name': pa.large_string(),
      'Referring Associated Publisher': pa.large_string(),
      'Referring Tags': pa.large_string(),
      'Cited by URLs': pa.large_string(),
      'Number of Cited by URLs': pa.int64()
    })
    client.close()