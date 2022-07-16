import os
import time
import requests
import dateutil
import pandas as pd

''' This script takes a list of months as input within its main() method on line 103,
    as well as the user's API Key for the New York Times Archive API as input in the 
    send_request() method on line 28.

    The output from this script is a directory "/headlines", with each csv 
    within the directory representing a month of articles.

    The Archive API must be enabled on the user's Developer Portal to use this script.
    This script is provided as a way to validate article count from NYT
    on the MediaCAT project, as output can be filtered by 'subsection'
    to obtain the counts for articles on a specific topic.
    This script is adapted from this guide:
    https://towardsdatascience.com/collecting-data-from-the-new-york-times-over-any-period-of-time-3e365504004   # nopep8

'''


def send_request(date):
    '''Sends a request to the NYT Archive API for the months of interest'''
    base_url = 'https://api.nytimes.com/svc/archive/v1/'
    url = base_url + date + '.json?api-key=' + 'YOUR API KEY HERE'
    response = requests.get(url).json()
    time.sleep(6)
    return response


def is_valid(article):
    '''Checks if article has a headline'''
    has_headline = type(article['headline']) == dict and 'main' in article['headline'].keys()  # nopep8
    return has_headline


def parse_response(response):
    '''Parses and returns response as pandas data frame'''
    data = {'headline': [],
            'date': [],
            'doc_type': [],
            'material_type': [],
            'section': [],
            'subsection': [],
            'keywords': [],
            'url': [],
            'byline': []}

    articles = response['response']['docs']
    for article in articles:
        date = dateutil.parser.parse(article['pub_date']).date()
        if is_valid(article):
            data['date'].append(date)
            data['headline'].append(article['headline']['main'])

            if 'section_name' in article:
                data['section'].append(article['section_name'])
            else:
                data['section'].append(None)

            if 'subsection_name' in article:
                data['subsection'].append(article['subsection_name'])
            else:
                data['subsection'].append(None)

            data['doc_type'].append(article['document_type'])
            data['url'].append(article['web_url'])

            if 'type_of_material' in article:
                data['material_type'].append(article['type_of_material'])
            else:
                data['material_type'].append(None)

            if 'original' in article['byline']:
                data['byline'].append(article['byline']['original'])
            else:
                data['byline'].append(None)

            keywords = [keyword['value'] for keyword in article['keywords'] if keyword['name'] == 'subject']  # nopep8
            data['keywords'].append(keywords)
    return pd.DataFrame(data)


def get_data(dates):
    '''Sends and parses request/response to/from NYT
    Archive API for provided months'''
    total = 0
    if not os.path.exists('headlines'):
        os.mkdir('headlines')
    for date in dates:
        response = send_request(date)
        df = parse_response(response)
        total += len(df)
        filename = date.replace('/', '')
        df.to_csv('headlines/' + filename + '.csv', index=False)
        print('Saving headlines/' + filename + '.csv...')
    print('Number of articles collected: ' + str(total))


def main():
    ''' Input list of months of interest in format ['YYYY/M',...]'''
    months = ['2021/1', '2021/2']
    get_data(months)


if __name__ == "__main__":
    main()
