import pandas as pd
import glob
import os
import dask.dataframe as dd
import logging
import csv
from post_utils.utils import write_to_file
import uuid
import tldextract
from post_utils.utils import row_parser
from timeit import default_timer as timer
import re
import ast

            
def find_twitter_citation_aliases(tweet, scope):
    '''Find and return the twitter citation aliases based on 
    a given tweet that satisfies the scope.'''
    found_aliases = []
    citation_url_or_text_alias = []
    citation_name = []
    node_twitter_handle = tweet['domain']
    

    try:
        for source, info in scope.items():
            
            # Compare the twitter handle in the citation scope with that in the crawled data, if the crawled one is not in the citation scope, then simply skip
            
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
                    
                print(domain)

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

            if 'Mentions' in tweet.keys():
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
        print("Exception")
        return str(citation_url_or_text_alias), str(citation_name), str([]), str(found_aliases)
    
def load_scope(file):
    """
    Loads the scope csv into a dictionary.
    Returns a dict of the scope with Source as key.
    """
    logging.info("Loading scope")
    # parse all the text aliases from each source using the scope file
    scope = {}
    # format: {source: {aliases: [], twitter_handles:[]}}
    with open(file) as csv_file:
        for line in csv.DictReader(csv_file):
            aliases, twitter, tags = [], [], []
            if 'Text Aliases' in line.keys() and line['Text Aliases']:
                aliases = line['Text Aliases'].split('|')
            else:
                aliases = []
            if 'Associated Twitter Handle' in line.keys() and line['Associated Twitter Handle']:  # nopep8
                twitter = line['Associated Twitter Handle'].split('|')
            else:
                twitter = []
            if 'Tags' in line.keys() and line['Tags']:
                tags = line['Tags'].split('|')
            else:
                tags = []
            try:
                publisher = line['Associated Publisher']
            except(Exception):
                publisher = ''
            try:
                source = line['Source']
            except(Exception):
                source = str(uuid.uuid5(uuid.NAMESPACE_DNS, line['Name']))
                
            scope[source] = {'Name': line['Name'] if 'Name' in line.keys() else '',
                                    #  'RSS': line['RSS feed URLs (where available)'],  # nopep8
                                     'Type': line['Type'] if 'type' in line.keys() else '',
                                     'Publisher': publisher,
                                     'Tags': tags,
                                     'aliases': aliases,
                                     'twitter_handles': twitter}
    write_to_file(scope, "processed_" + file.replace('./',
                  '').replace('.csv', '') + ".json")
    return scope

all_files = glob.glob("/home/dsu/notebooks/MEDIACAT-DOWNLOADS/washingtonpost/data_twitter/*.csv")


df_dask = dd.read_csv("/home/dsu/notebooks/MEDIACAT-DOWNLOADS/washingtonpost/data_twitter/*.csv", dtype=str)


crawl_scope = load_scope('./crawl_scope.csv')
citation_scope = load_scope('./citation_scope.csv')

i = 0
for item in df_dask.iterrows():
    i = i + 1
    if i == 5:
        #print(i)
        #print(item[1])
        tweet = item[1]
        tweet['domain'] = tweet["twitter_handle"]
        tweet['found_urls'] = tweet["citation_urls"]
        
        tweet['found_urls'] = ["http://aljarmaq.net"]
        tweet['date'] = tweet["created_at"]
        tweet['article_text'] = tweet["text"]
        tweet['article_text'] = tweet["text"]

        citation_url_or_text_alias, citation_name, anchor_text, found_aliases = find_twitter_citation_aliases(
        tweet, citation_scope)
        print(citation_url_or_text_alias)
        print(citation_name)
        
        break
    
#df_pandas = df_dask.compute()


li = []

#for filename in all_files:
#    df = pd.read_csv(filename, index_col=None, header=0)
#    li.append(df)


#frame = pd.concat(li, axis=0, ignore_index=True)

# dataframe.size
#size = df_pandas.size

# printing size
#print("Size = {}".format(size))
