import logging
import os
from timeit import default_timer as timer
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client
from post_utils.utils import LogPlugin

def init():
    '''
    Initialize output script.
    '''
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w') 
    os.makedirs('./logs/output_processing', exist_ok=True)

def create_output(args):
    '''
    Create the CSV outputs of the processor
    Parameter:
      args: the arguments of the program
    '''
    # initialize script
    init()

    # start logging for output
    logging.info('creating output')
    start = timer()
    client = Client(n_workers=args.workers, threads_per_worker=args.threads_per_worker, memory_limit=args.worker_memory)
    plugin = LogPlugin(logging, './logs/processing/')
    client.register_plugin(plugin)
    # read data from saved
    domain_data = dd.read_parquet('./saved/final_processed_domain_data.parquet')
    twitter_data = dd.read_parquet('./saved/final_processed_twitter_data.parquet')

    # remove unwanted keys for domain_data
    domain_data = domain_data.drop(columns=['Domain', 'Found URLs'])
    domain_data = domain_data.rename(columns={'Date': 'Date of Publication', 'Article Text': 'Plain Text'})
    # Reordering Columns
    domain_data = domain_data[['Referring Name',
                               'Referring Associated Publisher',
                               'Referring Tags',
                               'Title',
                               'Author',
                               'Date of Publication',
                               'Plain Text',
                               'Anchor Text',
                               'Cited URLs or Text Aliases',
                               'Cited Names',
                               'Cited Associated Publishers',
                               'Cited Tags',
                               'Cited by URLs',
                               'Number of Cited by URLs',
                               'ID'
                             ]]

    # remove unwanted keys for twitter_data
    twitter_data = twitter_data.drop(columns=['Found URLs', 'Mentions'])
    twitter_data = twitter_data.rename(columns={'Domain': 'Author', 'Date': 'Date of Publication', 'Article Text': 'Plain Text'})
    # Reordering Columns
    twitter_data = twitter_data[['Referring Name',
                                 'Referring Associated Publisher',
                                 'Referring Tags',
                                 'Author',
                                 'Date of Publication',
                                 'Like Count',
                                 'Retweet Count',
                                 'Quote Count',
                                 'Reply Count',
                                 'Plain Text',
                                 'Cited URLs or Text Aliases',
                                 'Cited Names',
                                 'Cited Associated Publishers',
                                 'Cited Tags',
                                 'Cited by URLs',
                                 'Number of Cited by URLs',
                                 'ID'
                               ]]

    if (len(domain_data) == 0):
        logging.info('only including domain data')
    elif (len(twitter_data) == 0):
        logging.info('only including twitter data')
    else:
        logging.info('including both types of data')

    output = twitter_data
    output.to_csv('./output/output_twitter_data.csv', single_file=False)

    output2 = domain_data
    output2.to_csv('./output/output_domain_data.csv', single_file=False)
    client.close()
    end = timer()
    logging.info('processing output took ' + str(end - start) + ' seconds')
    logging.info('complete')    

if __name__ == '__main__':
    create_output()

