import argparse
import json
import os
import googleapiclient.discovery
import requests
import pandas as pd
import calendar
import time
import quandl

from rake_nltk import Rake
from google.cloud import storage
from io import StringIO


def generate_keywords(search_input):
    """
    Extracts a list of keywords from the input string.

    :param search_input: user input query in natural language
    :type search_input: str
    :return: list containing search keywords
    """
    rake = Rake()
    rake.extract_keywords_from_text(search_input)
    phrases = rake.get_ranked_phrases()
    print('The key tags in the query are: ' + str(phrases))
    return phrases


def create_service():
    """Creates the service object for calling the Cloud Storage API."""
    # Construct the service object for interacting with the Cloud Storage API -
    # the 'storage' service, at version 'v1'.
    return googleapiclient.discovery.build('storage', 'v1')


def list_bucket_metadata(bucket, folder):
    """
    Returns a list of the names of objects within the given bucket folder.

    :param bucket: bucket containing the data
    :type bucket: str
    :param folder: folder name to look for in bucket
    :type folder: str
    :return: list containing data paths
    """
    service = create_service()
    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return)
    all_objects = []
    while req:
        resp = req.execute()
        all_objects.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)
    output = []
    for obj in all_objects:
        if folder in obj.get('name'):
            output.append(obj.get('name'))
    return output


def search_in_metadata(keywords, bucket, folder):
    """
    Search within the metadata layer for values that match the keywords.

    :param keywords: list containing keywords to search for
    :type keywords: list
    :param bucket: bucket containing the data
    :type bucket: str
    :param folder: folder within bucket containing relevant data
    :type folder: str
    :return: list of files containing relevant data
    """
    # get the available data in bucket
    metadata = list_bucket_metadata(bucket, folder)
    positive_columns = []
    for keyword in keywords:
        words = keyword.split(' ')
        if 'data' in words:
            words.remove('data')
        for w in words:
            for column in metadata:
                if w.lower() in column.lower():
                    # add columns that match one of the keywords
                    positive_columns.append(column)
    # Return data files
    positive_columns = [c for c in positive_columns if '.csv' in c]
    return positive_columns[:3]


def generate_chart_types(metadata):
    """
    Generate chart types to show based on relevant metadata.

    :param metadata: list containing data paths
    :type metadata: list
    :return: dict containing the paths to data to plot and chart types
    """
    agg_types = {
        'sum': 'pie_chart',
        'mean': 'bar_chart',
        'count': 'bar_chart',
        'quandl': 'bar_chart',
        'news': 'line_chart'
    }
    plot_types = []
    data_to_plot = []
    news_metadata = []
    for m in metadata:
        # prepare news metadata separately
        if 'news' in m:
            news_metadata.append(m)
    # show a maximum of 7 charts
    if len(metadata) > 7:
        metadata = metadata[0:6]
    # make sure that at least one news chart is shown
    if len(news_metadata) >= 1:
        metadata = metadata + list(news_metadata[0])
    for vertex in metadata:
        for agg_type in agg_types.keys():
            if agg_type in vertex:
                # determine chart type based on input metadata
                plot_types.append(agg_types.get(agg_type))
                data_to_plot.append(vertex)
    return dict(zip(data_to_plot, plot_types))


def get_news(search_data):
    """
    Return path to data containing date aggregation of relevant news data related to input.

    :param search_data: search phrase to look for
    :type search_data: str
    :return: path to the relevant news data
    """
    api_key = 'NEWS API KEY'
    base_url = 'https://newsapi.org/v2/'
    # use AND to search for multiple words at once
    if ' ' in search_data:
        search_data = search_data.replace(' ', ' AND ')
    # prepare the query's url for data in the chosen period
    main_url = f"{base_url}everything?q={search_data}&apiKey={api_key}&from=2020-04-27&to=2020-05-04"
    news_data = requests.get(main_url).json()
    # determine the number of available pages in result
    try:
        nb_pages = int(news_data["totalResults"] / 20)
    except:
        nb_pages = 0
        return None
    # free version allows for 100 results, so we can't query more than 5 pages
    if nb_pages > 5:
        nb_pages = 5
    results = []
    for i in range(1, nb_pages + 1):
        page_url = f"{main_url}&page={i}"
        page_data = requests.get(page_url).json()
        article = page_data["articles"]
        for ar in article:
            # take only publishing date
            results.append(ar["publishedAt"][:10])
    df_news = pd.DataFrame({'article_date': results})
    df_news['count'] = 1
    # get a count per date
    df_news = df_news.groupby('article_date', as_index=False).sum().sort_values(by=['article_date'])
    # generate timestamp
    ts = calendar.timegm(time.gmtime())
    # write df as csv to google cloud storage
    gcs = storage.Client()
    f = StringIO()
    df_news.to_csv(f)
    f.seek(0)
    gcs.get_bucket('notebooks_bucket'
                   ).blob(f'aggregated_data2/news/{search_data}_{ts}.csv'
                          ).upload_from_file(f, content_type='text/csv')
    return f'aggregated_data2/news/{search_data}_{ts}.csv'


def get_datasets_from_quandl(search_keywords):
    """
    Retrieves data from Quandl corresponding to the search keywords.

    :param search_keywords: list of the search keywords
    :type search_keywords: List
    :return: int representing number of dataset found on Quandl
    """
    api_key = 'QUANDL API KEY'
    quandl.ApiConfig.api_key = api_key
    base_url = 'https://www.quandl.com/api/v3/datasets.json?'
    gcs = storage.Client()
    quandl_data = []
    for kw in search_keywords:
        # prepare the Quandl search query
        main_url = base_url + 'query=' + kw.replace(' ', '+') + '&api_key=' + api_key
        query_result = requests.get(main_url).json()
        # get the list of the datasets returned by Quandl
        datasets = query_result["datasets"]
        print('Quandl returned ' + str(len(datasets)) + ' Datasets in total')
        for ds in datasets:
            # Skip premium datasets
            if ds['premium'] == 'false':
                # add the necessary information to query the dataset
                quandl_data.append((ds['database_code'], ds['dataset_code'], ds['name']))
    print('Quandl returned ' + str(len(quandl_data)) + ' free Datasets')
    for qd in quandl_data[:20]:
        # query the dataset
        data = quandl.get(qd[0] + '/' + qd[1])
        f = StringIO()
        data.to_csv(f)
        f.seek(0)
        # write the dataset in csv format to gs bucket
        gcs.get_bucket('notebooks_bucket'
                       ).blob(f'aggregated_data2/quandl/{qd[2]}.csv'
                              ).upload_from_file(f, content_type='text/csv')
    return len(quandl_data)
