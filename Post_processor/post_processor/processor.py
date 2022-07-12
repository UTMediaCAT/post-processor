from post_processor.processor_domain import process_domain
from post_processor.processor_twitter import process_twitter
from timeit import default_timer as timer
import pandas as pd
import ast
import dask.dataframe as dd
import logging
logging.basicConfig(filename='./logs/processor.log',
                    level=logging.DEBUG, filemode='w')


def parse_referrals(article, domain_referrals, twitter_referrals):
    """
    Cross-match the referrals for domain referrals and twitter referrals.
    Returns a list of all sources that are referring to this specific article.
    Parameters:
      article: an article in the data
      domain_referrals: a dictionary of all the referrals in the domain data
      twitter_referrals: a dictionary of all the referrals in the twitter data
    """
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


def getNumRef(row):
    return len(ast.literal_eval(row["referring name"]))


def process_crawler(crawl_scope, citation_scope):
    """
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
    """
    domain_referrals, domain_data = process_domain(crawl_scope, citation_scope)
    twitter_referrals, twitter_data = process_twitter(
        crawl_scope, citation_scope)

    logging.info(domain_data.columns)
    ### Add referrals to domain_data ###
    domain_data['url_dup'] = domain_data.index
    domain_data['referring name'] = domain_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta='object')
    domain_data['number of referrals'] = domain_data.apply(
        getNumRef, axis=1, meta='object')
    # domain_data.assign(referring_name=len(
    #     ast.literal_eval(domain_data["referring name"].str)))
    # logging.info(domain_res.index)
    # domain_res_pd = pd.DataFrame(domain_res, columns=[
    #     'referring name',
    #     'number of referrals'], index=domain_res.index)
    # domian_data_pd = domain_data.compute()  # domian_data_pd is a panda dataframe
    # domian_data_pd.update(domain_res_pd)
    # dd.from_pandas(domian_data_pd, npartitions=1)
    peocessed_domian_data = domain_data

    ### Add referrals to twitter_data ###
    twitter_data['referring name'] = ''
    twitter_data['number of referrals'] = ''
    twitter_data['url_dup'] = twitter_data.index
    twitter_res = twitter_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta='object')
    twitter_res_pd = pd.DataFrame(twitter_res, columns=[
        'referring name',
        'number of referrals'], index=twitter_res.index)
    # twitter_data_pd is a panda dataframe
    twitter_data_pd = twitter_data.compute()
    twitter_data_pd.update(twitter_res_pd)
    peocessed_twitter_data = dd.from_pandas(twitter_data_pd, npartitions=1)

    ### Save the processed data ###
    peocessed_domian_data.to_parquet(
        './Saved/processed_domain_data.parquet', engine="pyarrow")
    peocessed_twitter_data.to_parquet(
        './Saved/processed_twitter_data.parquet', engine="pyarrow")
