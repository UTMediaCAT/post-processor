import logging
import post_input.load_input as load_input
import post_processor.processor as processor
import post_output.create_output as output
logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')  # nopep8


def parse_args():
    """
    Parse the script arguments
    """


if __name__ == '__main__':
    parse_args()

    ## LOAD INPUT DATA ##
    # load scopes
    crawl_scope = load_input.load_scope('./crawl_scope.csv')
    citation_scope = load_input.load_scope('./citation_scope.csv')

    # load domain and twitter data
    load_input.load_twitter('./DataTwitter/*_output_*.csv')
    load_input.load_domain('./DataDomain/')

    # post process data
    processor.process_crawler(crawl_scope, citation_scope)

    # create output
    output.create_output()
