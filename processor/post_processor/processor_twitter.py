import logging
import re
import ast
import sys
import tldextract
import dask.dataframe as dd
import pandas as pd
from post_utils.utils import row_parser
from timeit import default_timer as timer


def init():
    '''Initialize twitter script.'''
    logging.basicConfig(filename='./logs/processor.log',
                        level=logging.DEBUG, filemode='w')


def find_twitter_citation_aliases(tweet, scope):
    '''Find and return the twitter citation aliases based on 
    a given tweet that satisfies the scope.'''
    found_aliases = []
    citation_url_or_text_alias = []
    citation_name = []
    node_twitter_handle = tweet['domain']

    try:
        for source, info in scope.items():
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
                    if domain.lower() in url.lower():
                        citation_url_or_text_alias.append(url)
                        citation_name.append(info['Name'])
                        if source not in found_aliases:
                            found_aliases.append(source)

            for url in tweet['found_urls']:
                for twitter_handle in info['twitter_handles']:
                    twitter_url = 'https://twitter.com/' + \
                        twitter_handle.replace('@', '') + '/'
                    if twitter_url.lower() in url.lower() and (url not in citation_url_or_text_alias):
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
    except Exception:
        return str(citation_url_or_text_alias), str(citation_name), str([]), str(found_aliases)


def get_twitter_handle_info(tweet, crawl_scope):
    '''Return the twitter handle information based
    on the tweet and crawl_scope.'''
    publisher = ''
    tags = ''
    name = ''
    for source, info in crawl_scope.items():
        for i in range(0, len(info['twitter_handles'])):
            indicator = tweet['domain'].replace('@', '').lower().strip() if type(tweet['domain']) is str else tweet['domain'].values[0].replace('@', '').lower().strip()
            if (info['twitter_handles'][i].replace('@', '').lower().strip() == indicator):
                try:
                    publisher = crawl_scope[source]['Publisher']
                except Exception:
                    publisher = ''
                try:
                    tags = crawl_scope[source]['Tags']
                except Exception:
                    tags = ''
                try:
                    name = crawl_scope[source]['Name']
                except Exception:
                    name = ''
    return publisher, tags, name


def tweet_helper(tweet, crawl_scope, citation_scope):
    '''Helper script for twitter processor based on tweet,
    crawl_scope, and citation_scope.'''
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
    '''
    Processes the twitter data by finding all the articles
    that are referring to it and mutating the output dictionary.
    Parameters:
        data: the twitter output dictionary
        scope: the scope dictionary
    Returns 2 dicts, one for the mutated data dictionary,
    and another dict of referrals.
    '''
    # initialize script
    init()
    # process twitter
    try:
        logging.info('processing twitter')
        start = timer()
        # load domain_data from saved
        data_partitions = dd.read_parquet('./saved/twitter_data.parquet')
        if (len(data_partitions) == 0):
            data_partitions['citation url or text alias'] = ''
            data_partitions['citation name'] = ''
            data_partitions['anchor text'] = ''
            data_partitions['associated publisher'] = ''
            data_partitions['tags'] = ''
            data_partitions['name'] = ''
            return {}, data_partitions

        referrals = {}

        data = data_partitions.repartition(
            partition_size='100MB')  # data is a dask dataframe
        logging.info('process twitter data with {} rows and {} partitions'.format(
            len(data_partitions), data_partitions.npartitions))

        data_pd = data.compute()  # data_pd is a panda dataframe
        res_list = []
        for index in data_pd.index:
            res_arr = tweet_helper(
                data_pd.loc[index], crawl_scope, citation_scope)
            res_list.append(res_arr)

        res_list = list(zip(*res_list))

        data_pd['citation url or text alias'] = list(res_list[0])
        data_pd['citation name'] = list(res_list[1])
        data_pd['anchor text'] = list(res_list[2])
        data_pd['associated publisher'] = list(res_list[4])
        data_pd['tags'] = list(res_list[5])
        data_pd['name'] = list(res_list[6])

        # get referrals update
        logging.info('getting referrals update')
        found_aliases_arr = list(res_list[3])

        i = 0
        for node in data_pd.index:
            try:
                for link in ast.literal_eval(data_pd.loc[node]['found_urls']):
                    # save all referrals where each key is
                    # each link in 'found_urls'
                    # and the value is this article's id
                    if link in referrals:
                        referrals[link].append(data_pd.loc[node]['domain'])
                    else:
                        referrals[link] = [data_pd.loc[node]['domain']]

                # looks for sources in found aliases, and adds it to the linking
                for source in ast.literal_eval(found_aliases_arr[i]):
                    if source in referrals:
                        referrals[source].append(data_pd.loc[node]['domain'])
                    else:
                        referrals[source] = [data_pd.loc[node]['domain']]
                i += 1
            except Exception:
                logging.warning(data_pd.loc[node])

        # update completed to True
        data_pd.completed = True
        # append to processed_data_pd using concat
        data_list = data_pd.to_dict('records')
        processed_data_pd = pd.DataFrame.from_records(data_list)

        processed_data = dd.from_pandas(processed_data_pd, npartitions=1).repartition(
            partition_size='100MB')

        end = timer() 
        logging.info(f'finished processing twitter with {i} records')
        logging.info('processing twitter took ' + str(end - start) + ' seconds')
        return referrals, processed_data
    except Exception:
        logging.warning('exception at processing twitter, data written to saved/')
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        raise

