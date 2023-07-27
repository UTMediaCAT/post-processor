from timeit import default_timer as timer
import dask.dataframe as dd
import pandas as pd
import logging


def init():
    """
    Initialize output script.
    """
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')  # nopep8


def create_output():
    # initialize script
    init()

    # start logging for output
    logging.info("creating output")
    start = timer()

    # read data from saved
    domain_data = dd.read_parquet('./saved/processed_domain_data.parquet').repartition(
        partition_size="100MB")
    twitter_data = dd.read_parquet('./saved/processed_twitter_data.parquet').repartition(
        partition_size="100MB")

    # remove unwanted keys for domain_data
    domain_data = domain_data.drop(
        columns=['domain', 'found_urls', 'html_content', 'completed'])
    domain_data = domain_data.set_index('id')
    domain_data = domain_data.rename(
        columns={'date': 'date of publication', 'article_text': 'plain text', 'url_dup': 'url'})
    # remove unwanted keys for twitter_data
    twitter_data = twitter_data.drop(
        columns=['domain', 'found_urls', 'Mentions', 'completed'])
    twitter_data = twitter_data.set_index('id')
    twitter_data = twitter_data.rename(
        columns={'date': 'date of publication', 'article_text': 'plain text', 'url_dup': 'url'})

    if (len(domain_data) == 0):
        output = twitter_data
    elif (len(twitter_data) == 0):
        output = domain_data
    else:
        output = domain_data.append(twitter_data)
    output = output[(output['citation url or text alias'].str.len() > 2) & ((
        output['associated publisher'].str.len() > 0) | (
        output['name'].str.len() > 0) | (
        output['tags'].str.len() > 0))]
    output = output.repartition(1)
    output.to_parquet('./saved/final_output.parquet')
    output.to_csv('./output/output_*.csv')


if __name__ == '__main__':
    create_output()
