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
    Initialize domain script.
    '''
    logging.basicConfig(filename='./logs/processor.log',
                        level=logging.DEBUG, filemode='w') 
    os.makedirs('./logs/domain_processing', exist_ok=True)
    os.makedirs('./logs/domain_referral_processing', exist_ok=True)

def find_domain_citation_aliases(article, scope, twitters):
    '''
    Find and return the domain citation aliases based on 
    a given tweet that satisfies the scope.
    Parameters:
      article: the exact row of the domain articles
      scope: the citation scope dictionary
      twitters: the twitter dictionary extract from citation_scope
    '''
    cited_url_or_text_aliases = []
    cited_name = []
    cited_tags = []
    cited_publisher = []
    anchor_text = []
    article_parsed = urlparse(article['Referring URL'])
    article_twitters = []
    article_domain = article_parsed.netloc
    entry = scope.get(article_domain)
    if not entry and article_domain.startswith('www.'):
        entry = scope.get(article_domain[4:])
    if entry:
        article_twitters = entry['Twitter Handles']
    # Go through the found_urls of the article
    for url in article['Found URLs']:
        # If the URL is an image then skip
        if image_pattern.search(url['url']):
            continue
        temp_url = url['url'] if "://" in url['url'] else "http://" + url['url']
        parsed = urlparse(temp_url)
        domain = parsed.netloc
        # If the found domain is from twitter then check its twitter handle
        if domain in ('twitter.com', 'www.twitter.com'):
            path_split = parsed.path.split('/')
            if len(path_split) < 2:
                continue
            twitter_handle = path_split[1]
            # Check if the found twitter handle is the same as the article's twitter if so skip
            if twitter_handle in article_twitters:
                continue
            twitter = twitters.get(twitter_handle)
            if twitter and url['url'] not in cited_url_or_text_aliases:
                cited_url_or_text_aliases.append(url['url'])
                cited_name.append(twitter['Name'])
                cited_tags.append(twitter['Tags'])
                cited_publisher.append(twitter['Publisher'])
            continue
        # If its a domain check the citation dictionary
        entry = scope.get(domain)
        if not entry and domain.startswith('www.'):
            entry = scope.get(domain[4:])
        if entry and url['url'] not in cited_url_or_text_aliases:
            cited_url_or_text_aliases.append(url['url'])
            cited_name.append(entry['Name'])
            cited_tags.append(entry['Tags'])
            cited_publisher.append(entry['Publisher'])
            anchor_text.append(url['title'].strip())

    # Skip if the article has no text
    if not article['Article Text'] or type(article['Article Text']) is not str:
        return str(cited_url_or_text_aliases)[1:-1], str(cited_name)[1:-1], str(cited_publisher)[1:-1], str(cited_tags)[1:-1], str(anchor_text)[1:-1]

    # Check the article to see if there are any twitter handle mentions
    for twitter, info in twitters.items():
        if twitter in article_twitters:
            continue
        pattern = r"@" + re.escape(twitter) + r"\b"
        twitter_at = '@' + twitter
        if re.search(pattern, article['Article Text'], re.IGNORECASE) and twitter_at not in cited_url_or_text_aliases:
            cited_url_or_text_aliases.append(twitter_at)
            cited_name.append(info['Name'])
            cited_tags.append(info['Tags'])
            cited_publisher.append(info['Publisher'])
            anchor_text.append('')

    # Check the article to see if there are any aliases
    for domain, info in scope.items():
        if domain == article_domain:
            continue
        for alias in info['Aliases']:
            if not alias:
                continue
            pattern = r"(?:\s|^|\"|')(" + re.escape(alias) + r")(?=\s|$|\"|'|,)"
            if re.search(pattern, article['Article Text'], re.IGNORECASE) and alias not in cited_url_or_text_aliases:
                cited_url_or_text_aliases.append(alias)
                cited_name.append(info['Name'])
                cited_tags.append(info['Tags'])
                cited_publisher.append(info['Publisher'])
                anchor_text.append('')
    return str(cited_url_or_text_aliases)[1:-1], str(cited_name)[1:-1], str(cited_publisher)[1:-1], str(cited_tags)[1:-1], str(anchor_text)[1:-1]

def get_domain_info(article, citation_scope):
    '''
    Return the article information based
    on the citation_scope.
    Parameters:
      article: the article entry from data frame
      citation_scope: the citation scope dictionary
    '''
    publisher = ''
    tags = ''
    name = ''
    parsed = urlparse(article['Referring URL'])
    entry = citation_scope.get(parsed.netloc)
    if not entry and parsed.netloc.startswith('www.'):
        entry = citation_scope.get(parsed.netloc[4:])
    if entry:
        name = entry['Name']
        publisher = entry['Publisher']
        tags = str(entry['Tags'])[1:-1]
    return publisher, tags, name

def domain_helper(url, article, citation_scope, twitter_scope):
    '''
    Helper script for domain processor based on article, url,
    and citation_scope.
    Parameters:
      url: the url of the article
      article: the article entry from data frame
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter dictionary extract from citation_scope
    '''
    article = row_parser(article)
    article['Referring URL'] = url if "://" in url else "http://" + url
    cited_url_or_text_aliases, cited_name, cited_publisher, cited_tags, anchor_text = find_domain_citation_aliases(
        article, citation_scope, twitter_scope)
    publisher, tags, name = get_domain_info(article, citation_scope)
    res = [
        cited_url_or_text_aliases,
        cited_name,
        cited_publisher,
        cited_tags,
        anchor_text,
        name,
        publisher,
        tags
    ]
    return res

def process_partition(df, citation_scope, twitter_scope):
    '''
    Process each partition of the data frame
    Parameters:
      df: a partition from the domain data frame
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter dictionary extract from citation_scope
    '''
    for i, row in df.iterrows():
        res_arr = domain_helper(i, row, citation_scope, twitter_scope)
        df.loc[i, ['Cited URLs or Text Aliases',
                   'Cited Names',
                   'Cited Associated Publishers',
                   'Cited Tags',
                   'Anchor Text',
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
      df: a partition from the domain data frame
    '''
    referrals = {}
    for i, row in df.iterrows():
        for link in ast.literal_eval(row['Found URLs']):
            if link['url'] not in referrals:
                referrals[link['url']] = []
            referrals[link['url']].append(i)
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

def process_domain(citation_scope, twitter_scope, args):
    '''
    Processes the domain data by finding all the articles
    that are referring to it and additionally doing cross-matching with 
    the scopes to add additional info to the twitter data.
    Parameters:
      citation_scope: the citation scope dictionary
      twitter_scope: the twitter dictionary extract from citation_scope
      args: arguments for the program
    '''
    # initialize script
    init()
    # process the domain
    try:
        logging.info('processing domain')
        start = timer()
        client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
        if not args.ppd:
            plugin = LogPlugin(logging, './logs/domain_processing/')
            client.register_plugin(plugin, name='logger')

            # load domain_data from saved
            data_partitions = dd.read_parquet('./saved/domain_data.parquet')
            if (len(data_partitions) == 0):
                data_partitions['Cited URLs or Text Aliases'] = ''
                data_partitions['Cited Names'] = ''
                data_partitions['Cited Associated Publishers'] = ''
                data_partitions['Cited Tags'] = ''
                data_partitions['Anchor Text'] = ''
                data_partitions['Referring Associated Publisher'] = ''
                data_partitions['Referring Tags'] = ''
                data_partitions['Referring Name'] = ''
                empty_pd = pd.DataFrame()
                return dd.from_pandas(empty_pd, npartitions=1), data_partitions
            
            logging.info('process domain data with {} rows and {} partitions'.format(len(data_partitions), data_partitions.npartitions))

            # Declare Metadata types that are returned from a processing
            meta = data_partitions.dtypes.to_dict()
            meta.update({
              'Cited URLs or Text Aliases': 'object',
              'Cited Names': 'object',
              'Cited Associated Publishers': 'object',
              'Cited Tags': 'object',
              'Anchor Text': 'object',
              'Referring Name': 'object',
              'Referring Associated Publisher': 'object',
              'Referring Tags': 'object'
            })

            # Begin processing the partition
            data = data_partitions.map_partitions(process_partition, citation_scope, twitter_scope, meta=meta)
            data.to_parquet('./saved/processed_domain_data.parquet', engine='pyarrow')
            logging.info('Finished Domain processing')
            client.unregister_worker_plugin(name="logger")
            # Restarting the worker here to reset resources (in-case worker is still holding on)
            client.restart()

        plugin = LogPlugin(logging, './logs/domain_referral_processing/')
        client.register_plugin(plugin)
        logging.info('Begin Domain referral extraction')

        # Read the saved processed_domain_data to avoid having second calculations here
        # Domain referral processing begins here
        data = dd.read_parquet('./saved/processed_domain_data.parquet')
        referrals = data.map_partitions(process_referral, meta={'Source': 'object', 'Domains': 'object'}).set_index('Source', drop=True)
        referral_concat = dd.Aggregation(
            name = 'referral_concat', 
            chunk = lambda x: x.aggregate(list), 
            agg = lambda x: x.aggregate(merge_domains)
        )
        referrals = referrals.groupby('Source').agg(referral_concat)
        referrals.to_parquet('./saved/domain_referral_data.parquet', engine='pyarrow')
        client.close()
        end = timer()
        logging.info(f'finished processing domain')
        logging.info('processing domain took ' + str(end - start) + ' seconds')
    except Exception:
        logging.warning('exception at processing domain, data written to saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        raise

