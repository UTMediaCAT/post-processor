from timeit import default_timer as timer
import dask.dataframe as dd
import pandas as pd
import logging
import re
import ast
import sys
import tldextract
from post_utils.utils import row_parser
logging.basicConfig(filename='./logs/processor.log',
                    level=logging.DEBUG, filemode='w')


def find_twitter_citation_aliases(tweet, scope):
    found_aliases = []
    citation_url_or_text_alias = []
    citation_name = []

    node_twitter_handle = tweet['domain']

    for source, info in scope.items():

        # skip recursive citation

        needskip = False
        for i in range(0, len(info['twitter_handles'])):
            if (info['twitter_handles'][i].replace('@', '').strip().lower() == node_twitter_handle.replace('@', '').strip().lower()):
                needskip = True
        if needskip:
            continue

        # find all url with domain matching scope
        if 'http' in source:
            ext = tldextract.extract(source)

            if ext[0] == '':
                domain = ext[1] + '.' + ext[2] + '/'
            else:
                domain = '.'.join(ext) + '/'

            for url in tweet['found_urls']:
                if domain in url:
                    citation_url_or_text_alias.append(url)
                    citation_name.append(info['Name'])
                    if source not in found_aliases:
                        found_aliases.append(source)

        for url in tweet['found_urls']:
            for twitter_handle in info['twitter_handles']:
                twitter_url = 'https://twitter.com/' + \
                    twitter_handle.replace('@', '') + '/'
                if twitter_url in url and (url not in citation_url_or_text_alias):
                    citation_url_or_text_alias.append(url)
                    citation_name.append(info['Name'])
                    if source not in found_aliases:
                        found_aliases.append(source)

        # find all matching mentions of the tweet
        for mention in tweet['Mentions']:
            for twitter_handle in info['twitter_handles']:
                if twitter_handle.replace('@', '').lower() == mention.lower():
                    citation_url_or_text_alias.append(twitter_handle)
                    citation_name.append(info['Name'])
                    if source not in found_aliases:
                        found_aliases.append(source)

        # find all matching text aliases of the tweet text
        aliases = info['aliases']
        for i in range(0, len(aliases)):
            pattern = r"( |\"|')" + re.escape(aliases[i]) + r"( |\"|'|,)"
            if re.search(pattern, tweet['article_text'], re.IGNORECASE) and (aliases[i] not in citation_url_or_text_alias):
                citation_url_or_text_alias.append(aliases[i])
                citation_name.append(info["Name"])
                if source not in found_aliases:
                    found_aliases.append(source)

    return str(citation_url_or_text_alias), str(citation_name), str([]), str(found_aliases)


def get_twitter_handle_info(tweet, crawl_scope):
    for source, info in crawl_scope.items():
        for i in range(0, len(info['twitter_handles'])):
            if (info['twitter_handles'][i].replace('@', '').lower() == tweet["domain"].replace('@', '').lower()):
                try:
                    publisher = crawl_scope[source]['Publisher']
                except Exception:
                    publisher = ""
                try:
                    tags = crawl_scope[source]['Tags']
                except Exception:
                    tags = ""
                try:
                    name = crawl_scope[source]['Name']
                except Exception:
                    name = ""

    return publisher, tags, name


def tweet_helper(tweet, crawl_scope, citation_scope):
    tweet = row_parser(tweet)
    citation_url_or_text_alias, citation_name, anchor_text, found_aliases = find_twitter_citation_aliases(
        tweet, citation_scope)
    publisher, tags, name = get_twitter_handle_info(tweet, crawl_scope)
    res = [
        citation_url_or_text_alias,
        citation_name,
        anchor_text,
        found_aliases,
        publisher,
        tags,
        name
    ]
    return res


def process_twitter(crawl_scope, citation_scope):
    """
    Processes the twitter data by finding all the articles
    that are referring to it and mutating the output dictionary.
    Parameters:
        data: the twitter output dictionary
        scope: the scope dictionary
    Returns 2 dicts, one for the mutated data dictionary,
    and another dict of referrals.
    """
    try:
        logging.info("Processing Twitter")
        start = timer()
        # load domain_data from Saved
        data_partitions = dd.read_parquet('./Saved/twitter_data.parquet')
        data_partitions['citation url or text alias'] = ''
        data_partitions['citation name'] = ''
        data_partitions['anchor text'] = ''
        data_partitions['associated publisher'] = ''
        data_partitions['tags'] = ''
        data_partitions['name'] = ''

        referrals = {}
        processed_data_pd = pd.DataFrame()
        data_partitions = data_partitions.repartition(100).partitions[0]

        data = data_partitions.repartition(
            partition_size="100MB")  # data is a dask dataframe
        logging.info('process twitter data with {} rows and {} partitions'.format(
            len(data_partitions), data_partitions.npartitions))

        res_arr = data.apply(tweet_helper, axis=1, args=(
            crawl_scope, citation_scope,), meta='object')

        # update 'citation url or text alias', 'citation name', 'anchor text' using pd.update
        # update publisher, tags, name
        res_pd = pd.DataFrame(res_arr, columns=[
            'citation url or text alias',
            'citation name',
            'anchor text',
            'found_aliases',
            'associated publisher',
            'tags',
            'name'], index=res_arr.index)
        data_pd = data.compute()  # data_pd is a panda dataframe
        data_pd.update(res_pd)

        # get referrals update
        logging.info("getting referrals update")
        found_aliases_arr = res_pd['found_aliases']

        for node in data_pd.index:
            for link in ast.literal_eval(data_pd.loc[node]['found_urls']):
                # save all referrals where each key is
                # each link in 'found_urls'
                # and the value is this article's id
                if link in referrals:
                    referrals[link].append(data_pd.loc[node]['id'])
                else:
                    referrals[link] = [data_pd.loc[node]['id']]

            # looks for sources in found aliases, and adds it to the linking
            for source in ast.literal_eval(found_aliases_arr.loc[node]):
                if source in referrals:
                    referrals[source].append(data_pd.loc[node]['id'])
                else:
                    referrals[source] = [data_pd.loc[node]['id']]

        # update completed to True
        data_pd.completed = True
        # append to processed_data_pd
        processed_data_pd = processed_data_pd.append(data_pd)

        processed_data = dd.from_pandas(processed_data_pd, npartitions=1).repartition(
            partition_size="100MB")

        end = timer()
        logging.info("Finished Processing Twitter - Took " + str(end - start) + " seconds")  # nopep8
        return referrals, processed_data
    except Exception:
        logging.warning('Exception at Processing Twitter, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        raise
