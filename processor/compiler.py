import logging
import os
import post_input.load_input as load_input
import post_processor.processor as processor
import post_output.create_output as output
logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w')  # nopep8


def parse_args():
    """
    Parse the script arguments.
    Currently there are no script arguments.
    """
    pass


def check_dirs():
    """
    Check the directory structure to see if the required
    directories exist.  If they do not, create the directories
    and exit the program.
    """
    REQUIRED = ('data_domain', 'data_twitter', 'output', 'saved', 'logs')
    MISSING = []
    for directory in REQUIRED:
        if not os.path.isdir(f'./{directory}'):
            MISSING.append(directory)
    if MISSING:
        print('[MediaCAT] Directories not found; creating directories...')
        for directory in MISSING:
            os.mkdir(f'./{directory}')
        print('[MediaCAT] Directories created; please add data')
        exit()


def check_files():
    """
    Check the directory structure to see if the required
    files exist.  If they do not, exit the program.
    """
    REQUIRED = ('crawl_scope.csv', 'citation_scope.csv')
    MISSING = []
    for file in REQUIRED:
        if not os.path.isfile(f'./{file}'):
            MISSING.append(file)
    if MISSING:
        print('[MediaCAT] Files not found; please add files')
        exit()


if __name__ == '__main__':
    ## SCRIPT PREPARATION ##
    # parse script arguments
    parse_args()

    # check directories and files
    check_dirs()
    check_files()

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
