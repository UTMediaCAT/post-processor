import logging
import os
import post_input.load_input as load_input
import post_processor.processor as processor
import post_output.create_output as output
from post_utils.utils import eprint


def parse_args():
    '''
    Parse the script arguments.
    Currently there are no script arguments.
    '''
    pass


def check_dirs():
    '''
    Check the directory structure to see if the required
    directories exist.  If they do not, create the directories
    and exit the program.
    '''
    REQUIRED = ('data_domain', 'data_twitter', 'output', 'saved', 'logs')
    OPTIONAL = ('output', 'saved', 'logs')
    missing = []
    for directory in REQUIRED:
        if not os.path.isdir(f'./{directory}'):
            missing.append(directory)
    if missing:
        eprint('[MediaCAT] Directories not found; creating directories...')
        for directory in missing:
            os.mkdir(f'./{directory}')
        elective = map((lambda x : x in OPTIONAL), missing)
        if not all(elective):
            eprint('[MediaCAT] Directories created; please add data')
            exit(1)
        eprint('[MediaCAT] Missing directories created')


def check_files():
    '''
    Check the directory structure to see if the required
    files exist.  If they do not, exit the program.
    '''
    REQUIRED = ('crawl_scope.csv', 'citation_scope.csv')
    missing = []
    for file in REQUIRED:
        if not os.path.isfile(f'./{file}'):
            missing.append(file)
    if missing:
        eprint('[MediaCAT] Files not found; please add files')
        exit(1)


def init():
    '''Initialize the compiler script.'''
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w') 


def terminate():
    '''Terminate the completed program.'''
    eprint('[MediaCAT] Post processing completed successfully')
    logging.info('successful completion')
    exit(0)


if __name__ == '__main__':
    ## SCRIPT PREPARATION ##
    # parse script arguments
    parse_args()

    # check directories and files
    check_dirs()
    check_files()

    # initialize script
    init()

    ## LOAD INPUT DATA ##
    # load scopes
    crawl_scope = load_input.load_scope('./crawl_scope.csv')
    citation_scope = load_input.load_scope('./citation_scope.csv')

    # load domain and twitter data
    load_input.load_twitter('./data_twitter/*_output_*.csv')
    load_input.load_domain('./data_domain/')

    # post process data
    processor.process_crawler(crawl_scope, citation_scope)

    # create output
    output.create_output()

    # terminate the program
    terminate()
