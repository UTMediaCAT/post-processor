import ast
import logging
import pandas as pd
import dask.dataframe as dd
from post_processor.processor_domain import process_domain
from post_processor.processor_twitter import process_twitter
from timeit import default_timer as timer


def init():
    '''Initialize processor script.'''
    logging.basicConfig(filename='./logs/processor.log',
                        level=logging.DEBUG, filemode='w')


def parse_referrals(article, domain_referrals, twitter_referrals):
    '''
    Cross-match the referrals for domain referrals and twitter referrals.
    Returns a list of all sources that are referring to this specific article.
    Parameters:
      article: an article in the data
      domain_referrals: a dictionary of all the referrals in the domain data
      twitter_referrals: a dictionary of all the referrals in the twitter data
    '''
    referring_articles = []
    # get all referrals for this url (who is referring to me)

    if article['url_dup'] in domain_referrals:
        # if str(article['url']) in domain_referrals:
        referring_articles += domain_referrals[article['url_dup']]

    if str(article['url_dup']) in twitter_referrals:
        referring_articles += twitter_referrals[str(article['url_dup'])]
    # print(referring_articles)
    # remove duplicates from list
    referring_articles = list(dict.fromkeys(referring_articles))
    # remove itself from list
    if article['id'] in referring_articles:
        referring_articles.remove(article['id'])

    return str(referring_articles)


def get_num_ref(row):
    '''
    Return the number of referrals.
    '''
    try:
        return len(ast.literal_eval(row['referring name']))
    except Exception:
        return 0


def process_crawler(crawl_scope, citation_scope):
    '''
    The main point of entry for the processor.
    Calls the domain and twitter processor seperately.
    Parameters:
        domain_data: domain dictionary
        twitter_data: twitter dictionary
        scope: scope dictionary
        domain_pairs: domain dictionary mapping article id to url
        twitter_pairs: twitter dictionary mapping tweet id to url
        saved_domain_referrals: A dictionary of saved domain referrals,
                                with mapping of url to id.
        saved_twitter_referrals: A dictionary of saved twitter referrals,
                                with mapping of url to id.

    Outputs the post processing data into output.json and interest_output.json.
    '''
    # initialize script
    init()

    # process crawling results
    domain_referrals, domain_data = process_domain(crawl_scope, citation_scope)
    twitter_referrals, twitter_data = process_twitter(
        crawl_scope, citation_scope)
    logging.info(f'domain columns: {domain_data.columns}')
    logging.info(f'twitter columns: {twitter_data.columns}')

    ### Add referrals to domain_data ### 
    domain_data['url_dup'] = domain_data.index
    domain_data['referring name'] = domain_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta=('referring name', 'str'))
    domain_data['number of referrals'] = domain_data.apply(
        get_num_ref, axis=1, meta=('number of referrals', 'int64'))
    processed_domain_data = domain_data

    ### Add referrals to twitter_data ###    
    twitter_data['referring name'] = ''
    twitter_data['number of referrals'] = ''
    twitter_data['url_dup'] = twitter_data.index
    twitter_data['referring name'] = twitter_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta=('referring name', 'str'))
    twitter_data['number of referrals'] = twitter_data.apply(
        get_num_ref, axis=1, meta=('number of referrals', 'int64'))
    processed_twitter_data = twitter_data

    ### Save the processed data ###
    processed_domain_data.to_parquet(
        './saved/processed_domain_data.parquet', engine='pyarrow')
    processed_twitter_data.to_parquet(
        './saved/processed_twitter_data.parquet', engine='pyarrow')

