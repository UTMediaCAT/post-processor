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


def find_domain_citation_aliases(article, scope):
    '''
    Finds all the in scope citations, text aliases and twitter handles in this node's text.
    Append keys 'citation url or text alias', 'citation name', and 'anchor text' to the node.
    Parameters:
        data: the data dictionary
        node: the node in the dictionary that we are searching on
        scope: the scope dictionary
    Returns 1 lists:
        - found aliases is a list of all
            the sources that this article node refers to
    '''
    found_aliases = []

    citation_url_or_text_alias = []
    citation_name = []
    anchor_text = []
    sequence = article['html_content']
    for source, info in scope.items():

        if 'http' in source:
            # find the in-scope citation url in html_content
            ext_node = tldextract.extract(article['domain'])
            ext = tldextract.extract(source)

            # skip recursive citation
            if (ext_node == ext):
                continue

            if ext[0] == '':
                domain = ext[1] + '.' + ext[2]
            else:
                domain = '.'.join(ext)
            pattern = r"<a\s+href=([\"'])(http://www.|http://|https://www.|https://)" + \
                re.escape(domain) + r"/(.*?)([\"'])(.*?)(>)(.*?)(</a>)"
            matches = re.findall(pattern, sequence, re.IGNORECASE)
            if matches:
                for match in matches:
                    citation_url = match[1] + domain + '/' + match[2]
                    # sometime non english article list hyperlink multiple times for a single citation
                    # check duplicate here
                    if citation_url not in citation_url_or_text_alias:
                        citation_url_or_text_alias.append(citation_url)
                        anchor_text.append(match[6])
                        citation_name.append(info["Name"])
                    if source not in found_aliases:
                        found_aliases.append(source)

        # find in-scope aliases in html_content
        if info['aliases']:
            aliases = info['aliases']
            for i in range(0, len(aliases)):
                pattern = r"( |\"|')" + re.escape(aliases[i]) + r"( |\"|'|,)"
                if re.search(pattern, sequence, re.IGNORECASE):
                    citation_url_or_text_alias.append(aliases[i])
                    citation_name.append(info["Name"])
                    if source not in found_aliases:
                        found_aliases.append(source)

        # find twitter_handles in html_content
        if info['twitter_handles']:
            handles = info['twitter_handles']
            for i in range(0, len(handles)):
                pattern = r"@" + re.escape(handles[i])
                if re.search(pattern, sequence, re.IGNORECASE):
                    citation_url_or_text_alias.append(handles[i])
                    citation_name.append(info['Name'])
                    if source not in found_aliases:
                        found_aliases.append(source)

    return str(citation_url_or_text_alias), str(citation_name),  str(anchor_text), str(found_aliases)


def get_domain_info(article, crawl_scope):
    try:
        publisher = crawl_scope[article["domain"]]['Publisher']
    except Exception:
        publisher = ""
    try:
        tags = crawl_scope[article["domain"]]['Tags']
    except Exception:
        tags = ""
    try:
        name = crawl_scope[article["domain"]]['Name']
    except Exception:
        name = ""

    return publisher, tags, name


def domain_helper(article, crawl_scope, citation_scope):
    article = row_parser(article)
    citation_url_or_text_alias, citation_name, anchor_text, found_aliases = find_domain_citation_aliases(
        article, citation_scope)
    publisher, tags, name = get_domain_info(article, crawl_scope)
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


def process_domain(crawl_scope, citation_scope):
    """
    Processes the domain data by finding all the articles that it is
    referring to and articles that are referring to it and mutating
    the output dictionary.
    Parameters:
        data: the domain output dictionary
        scope: the scope dictionary
    Return:
        Returns 2 dicts, one for the mutated data dictionary,
        and another dict of referrals.
    """
    logging.info("Processing Domain")
    try:
        start = timer()
        # load domain_data from Saved
        data_partitions = dd.read_parquet('./Saved/domain_data.parquet')
        data_partitions['citation url or text alias'] = ''
        data_partitions['citation name'] = ''
        data_partitions['anchor text'] = ''

        referrals = {}
        processed_data_pd = pd.DataFrame()
        data = data_partitions.repartition(
            partition_size="100MB")  # data is a dask dataframe
        logging.info('process domain data with {} rows and {} partitions'.format(
            len(data_partitions), data_partitions.npartitions))

        res_arr = data.apply(domain_helper, axis=1, args=(
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
                if link['url'] in referrals:
                    referrals[link['url']].append(data_pd.loc[node]['id'])
                else:
                    referrals[link['url']] = [data_pd.loc[node]['id']]

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

        processed_data = dd.from_pandas(processed_data_pd, npartitions=1)

        end = timer()
        logging.info("Finished Processing Domain - Took " + str(end - start) + " seconds")  # nopep8
        return referrals, processed_data
    except Exception:
        logging.warning('Exception at Processing Domain, data written to Saved/')  # nopep8
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error(exc_value)
        logging.error(exc_type)
        raise
