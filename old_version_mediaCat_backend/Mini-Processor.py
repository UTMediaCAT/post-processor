#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import csv
import os


# In[2]:


def get_list(file):
    """
    get twitter user from csv and return a list ie["@aaa","@bbb","@ccc"]
    :param df: a csv file
    :rtype: a list
    """
    try:
        # try to read csv file
        df = pd.read_csv(file)
        name_list = df["Source"].tolist()
        tags = df["Tags"].tolist()
        return (name_list, tags)
    except(Exception):
        print("Input file is not csv or doesn't exist such csv")
        exit(1)

# In[42]:


def mini_processor(name, tag):
    """
    reshape all csvs which generate by twitter crawler,
    :param df: a csv file
    :rtype: None
    """
    try:
        tweet = pd.read_csv("./csv/" + name + ".csv", low_memory=False)
        retweet = pd.DataFrame({'Hit Record Unique ID': tweet["id"].tolist(),
                                "URL to article/Tweet": tweet["link"].tolist(),
                                "Source": "@" + name,
                                "Location": tweet["place"].tolist(),
                                "Hit Type": "Twitter Handle",
                                "Passed through tags": tag,
                                "Language": tweet["language"].tolist(),
                                "Associated Publisher": np.nan,
                                "Referring Hit Record Unique ID": np.nan,
                                "Authors": tweet["name"].tolist(),
                                "Plain Text of Article or Tweet": tweet["tweet"].tolist(),  # nopep8
                                "Date": tweet["date"].tolist(),
                                "Mentions": tweet["mentions"].tolist(),
                                "Hashtags": tweet["hashtags"].tolist(),
                                "Found URL": tweet["urls"].tolist()})
        retweet.to_csv("./tester/" + name + '.csv', index=False, encoding='utf-8-sig', quoting=csv.QUOTE_NONNUMERIC)  # nopep8
    except Exception as e:
        print(e)
        pass


def add_headers():
    """
    add correct headers to twitter csv files.
    """
    path = './csv/'
    for file_name in [file for file in os.listdir(path) if file.endswith('.csv')]:  # nopep8
        try:
            df = pd.read_csv(path + file_name, header=None, low_memory=False)
            df.to_csv("csv/" + file_name,
                      header=["id", "conversation_id", "created_at", "date",
                              "time", "timezone", "user_id", "username",
                              "name", "place", "tweet", "language",
                              "mentions", "urls", "photos", "replies_count",
                              "retweets_count", "likes_count", "hashtags",
                              "cashtags", "link", "retweet", "quote_url",
                              "video", "thumbnail", "near", "geo", "source",
                              "user_rt_id", "user_rt", "retweet_id",
                              "reply_to", "retweet_date", "translate",
                              "trans_src", "trans_dest"], index=False)
        except pd.errors.EmptyDataError:
            print("Empty File: " + file_name)
            pass


def pre_processer(file):
    (list_name, tags) = get_list(file)
    # add_headers()
    for i in range(len(list_name)):
        txtname = list_name[i].split('@')[1]
        mini_processor(txtname, tags[i])


if __name__ == '__main__':
    pre_processer("twitter.csv")
    # pre_processer("/voyage_storage/mediacat-twitter-crawler/twitter.csv")
