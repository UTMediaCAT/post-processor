import logging
import os
import glob
import argparse
import shutil
import post_input.load_input as load_input
import post_processor.processor as processor
import post_output.create_output as output
from post_utils.utils import eprint

def parse_args():
    '''
    Parse the script arguments.
    '''
    parser = argparse.ArgumentParser(description='MediaCAT compiler script')
    # Disables loading Twitter entirely (Ensure you have ./saved/twitter_data.parquet saved)
    parser.add_argument('-lt', action='store_true', help='Disable Load Twitter data flag')
    # Disables loading Domain entirely (Ensure you have ./saved/domain_data.parquet saved)
    parser.add_argument('-ld', action='store_true', help='Disable Load Domain data flag')
    # Disables processing Twitter data entirely (Ensure you have ./saved/processed_twitter_data.parquet saved and ./saved/twitter_referral_data.parquet)
    parser.add_argument('-pt', action='store_true', help='Disable Total Process Twitter data flag')
    # Disables processing Domain data entirely (Ensure you have ./saved/processed_domain_data.parquet saved and ./saved/domain_referral_data.parquet)
    parser.add_argument('-pd', action='store_true', help='Disable Total Process Domain data flag')
    # Disables the Twitter data conversion to a single CSV (Ensure you have ./data_twitter/data_twitter_csv/output.csv saved)
    parser.add_argument('-ltcsv', action='store_true', help='Disable Twitter CSV file convert')
    # Disables the Domain data conversion to a single CSV (Ensure you have ./data_domain/data_domain_csv/output.csv saved)
    parser.add_argument('-ldcsv', action='store_true', help='Disable Domain CSV file convert')
    # Disables the Process Domain data but allow the referral to generate (Ensure you have ./saved/processed_domain_data.parquet saved)
    parser.add_argument('-ppd', action='store_true', help='Disable Process Domain data flag')
    # Disables the Process Twitter data but allow the referral to generate (Ensure you have ./saved/processed_twitter_data.parquet saved)
    parser.add_argument('-ppt', action='store_true', help='Disable Process Twitter data flag')
    # Flag for controlling number of workers used for processing. Default = 4 workers
    parser.add_argument('-w', '--workers', type=int, default=4, help='Number of workers to use for processing')
    # Flag for controlling number of threads per worker used for processing. Default = 1 thread per worker
    parser.add_argument('-tpw', '--threads_per_worker', type=int, default=1, help='Number of threads per worker to use for processing')
    # Flag for the GB of maximum memory used per worker. Default = 3.5GB per worker
    parser.add_argument('-wm', '--worker_memory', type=float, default=3.5, help='Max memory per worker to use for processing in GB')
    # Flag to cleanup the saved files
    parser.add_argument('-cu', '--cleanup', action='store_true', help='Cleanup data')
    args = parser.parse_args()
    args.worker_memory = f"{args.worker_memory}GB"
    return args

def check_dirs():
    '''
    Check for folders 'data_domain', 'data_twitter', 'output', 'saved', 'logs'.\n
    Create if doesn't exist
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
    files exist.  If they do not, exit the program. \n
    Check for 'citation_scope.csv'
    '''
    if not os.path.isfile('./citation_scope.csv'):
        eprint('[MediaCAT] Files not found; please add files')
        exit(1)

def init():
    '''
    Initialize the compiler script.
    Nothing very important goes on here. It just setup logging to ./logs/processor.log file.
    Also cleans the log folder
    '''
    logging.basicConfig(filename='./logs/processor.log', level=logging.DEBUG, filemode='w') 
    # Get a list of all log files and directory
    log_files = glob.glob('./logs/*')

    # Iterate over the list of files and remove each log
    for file in log_files:
        try:
            if os.path.isdir(file):
                shutil.rmtree(file)
                logging.info(f'Removed {file}')
        except Exception as e:
            logging.info(f"Error deleting {file}: {e}")

def cleanup_parquet():
    '''
    Cleans up every .parquet saves in the ./saved folder
    '''
    # Get a list of saved parquets
    parquet_files = glob.glob('./saved/*')

    # Iterate over the list of files and remove each parquet
    for file in parquet_files:
        try:
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)
            logging.info(f'Removed {file}')
        except Exception as e:
            logging.info(f"Error deleting {file}: {e}")

def cleanup():
    '''
    Clean up any remaining temporary files or directories.
    '''
    CLEANUP = ('domain_input_csv', 'twitter_input_csv')
    for directory in CLEANUP:
        dir_path = f'./{directory}'
        if os.path.isdir(f'./{directory}'):
            shutil.rmtree(dir_path)
            logging.info(f'Removed {dir_path}')

def terminate():
    '''
    Terminate the completed program.
    '''
    # cleanup()
    print('[MediaCAT] Post processing completed successfully')
    logging.info('successful completion')
    exit(0)

if __name__ == '__main__':
    ## SCRIPT PREPARATION ##
    # parse script arguments
    args = parse_args()

    # check directories and files
    check_dirs()
    check_files()

    # initialize script
    init()

    if args.cleanup:
        cleanup_parquet()
        cleanup()
        exit()

    ## LOAD INPUT DATA ##
    # load scopes
    citation_scope, twitter_scope = load_input.load_scope('./citation_scope.csv')
    # load domain and twitter data
    if not args.lt:
        load_input.load_twitter('./data_twitter/', args)
    if not args.ld:
        load_input.load_domain('./data_domain/', args)
    # post process data
    processor.process_crawler(citation_scope, twitter_scope, args)

    # create output
    output.create_output(args)

    # terminate the program
    terminate()
