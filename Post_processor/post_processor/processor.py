from post_processor.processor_domain import process_domain
from post_processor.processor_twitter import process_twitter
from timeit import default_timer as timer
import pandas as pd
import dask.dataframe as dd


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
        referring_articles += domain_referrals[article['url']]

    if str(article['url_dup']) in twitter_referrals:
        referring_articles += twitter_referrals[str(article['url_dup'])]
    # print(referring_articles)
    # remove duplicates from list
    referring_articles = list(dict.fromkeys(referring_articles))
    # remove itself from list
    if article['id'] in referring_articles:
        referring_articles.remove(article['id'])

    return [str(referring_articles), len(referring_articles)]


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
    domain_referrals, domian_data = process_domain(crawl_scope, citation_scope)
    twitter_referrals, twitter_data = process_twitter(
        crawl_scope, citation_scope)

    ### Add referrals to domain_data ###
    domian_data['referring record id'] = ''
    domian_data['number of referrals'] = ''
    domian_data['url_dup'] = domian_data.index
    domain_res = domian_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta='object')
    domain_res_pd = pd.DataFrame(domain_res, columns=[
        'referring record id',
        'number of referrals'], index=domain_res.index)
    domian_data_pd = domian_data.compute()  # domian_data_pd is a panda dataframe
    domian_data_pd.update(domain_res_pd)
    peocessed_domian_data = dd.from_pandas(domian_data_pd, npartitions=1)

    ### Add referrals to twitter_data ###
    twitter_data['referring record id'] = ''
    twitter_data['number of referrals'] = ''
    twitter_data['url_dup'] = twitter_data.index
    twitter_res = twitter_data.apply(parse_referrals, axis=1, args=(
        domain_referrals, twitter_referrals, ), meta='object')
    twitter_res_pd = pd.DataFrame(twitter_res, columns=[
        'referring record id',
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
