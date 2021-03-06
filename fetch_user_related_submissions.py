import os
import time
from kafka import KafkaConsumer
import json
import numpy as np
import re
import praw
from pymongo import MongoClient
from multiprocessing import Process

kafka_topic = "reddit"


reddit = praw.Reddit(client_id="Yeoq6MgIIwS9Sg",
                     client_secret="zT6thUYquHqa9b-4U2i7Q1RXCBg",
                     user_agent="personal use script")


myclient = MongoClient()
redditdb = myclient["reddit"]

submissions_col = redditdb["user.submissions"]
comments_col = redditdb["user.comments"]
authors_col = redditdb["user.authors"]
user_col = redditdb["user"]


clone_user_submissions_col = redditdb["clone_user_submissions_col"]
clone_user_comments_col = redditdb["clone_user_comments_col"]
clone_user_authors_col = redditdb["clone_user_authors_col"]
clone_user_col = redditdb["clone_user_col"]

def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        new[key1] = val1

    return new


def standardize_text(txt):
    txt = re.sub(r"http\S+", "",txt)
    txt = re.sub(r"http", "",txt)
    txt = re.sub(r"@\S+", "",txt)
    txt = re.sub(r"[^A-Za-z0-9(),!?@\'\`\"\_\n]", " ",txt)
    txt = re.sub(r"@", "at",txt)
    txt = txt.lower()
    return txt




if __name__ == '__main__':
    print('$$$$$$$$$$$ CONSUMER $$$$$$$$$$$$$$$$')



    success = False
    consumer = None
    while success == False:
        try:
            consumer = KafkaConsumer()
            consumer.subscribe([kafka_topic])
            success = True
        except:
            print("consumer error")

        for msg in consumer:
            user_json = json.loads(msg.value)

            name = user_json['name']
            user = reddit.redditor(name)

            user_submissions = user.submissions.new(limit=None)


            user_obj = {
                'id': user.id,
                'comment_karma': user.comment_karma,
                'created': user.created,
                'name': user.name,
                'fullname': user.fullname,
                'link_karma': user.link_karma
            }



            for submission in user_submissions:
                print('inserting submission.')
                sub_obj = {
                    'title': submission.title,
                    'id': submission.id,
                    'ups': submission.ups,
                    'downs': submission.downs,
                    'name': submission.name,
                    'subreddit_display_name': submission.subreddit.display_name,
                    'subreddit_name': submission.subreddit.name,
                    'subreddit_id': submission.subreddit.id,
                    'user_name': name,
                    'user_id': user_obj['id']
                }

                submissions_col.update({'id': sub_obj['id']}, sub_obj, upsert=True)
                clone_user_submissions_col.update({'id': sub_obj['id']}, sub_obj, upsert=True)

            print('pushed user submissions.')

