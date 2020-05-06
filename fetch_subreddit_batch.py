#https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273
#zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties & kafka-server-start /usr/local/etc/kafka/server.properties
#pip install kafka-python
#pip install msgpack

import praw
import datetime
from pymongo import MongoClient
import time
from kafka import KafkaProducer, KafkaClient
import time
import json

myclient = MongoClient()
redditdb = myclient["reddit"]

submissions_col = redditdb["coronavirus.submissions"]
comments_col = redditdb["coronavirus.comments"]
authors_col = redditdb["coronavirus.authors"]

clone_submissions_col = redditdb["clone_submissions_col"]
clone_comments_col = redditdb["clone_comments_col"]
clone_user_authors_col = redditdb["user_authors_col"]


# isinstance(thing, praw.models.Comment)
# isinstance(thing, praw.models.Submission)

reddit = praw.Reddit(client_id="Yeoq6MgIIwS9Sg",
                     client_secret="zT6thUYquHqa9b-4U2i7Q1RXCBg",
                     user_agent="personal use script")


# assume you have a Reddit instance bound to variable `reddit`
subreddit = reddit.subreddit("coronavirus")

print(subreddit.display_name)  # Output: redditdev
print(subreddit.title)         # Output: reddit Development
print(subreddit.description)
print(subreddit.collections.subreddit)
print(subreddit.active_user_count)
print(subreddit.created)
print(subreddit.over18)
print(subreddit.quarantine)

# kafka = KafkaClient("kafka:9092")
producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:

    submission_list = []
    for submission in subreddit.new(limit=1000):

        submission_obj = {
            'id': submission.id,
            'title': submission.title,
            'created': submission.created,
            'selftext': submission.selftext,
            'is_video': submission.is_video,
            'is_reddit_media_domain': submission.is_reddit_media_domain,
            'media': submission.media,
            'author_id': submission.author.id,
            'is_self': submission.is_self,
            'name': submission.name,
            'ups': submission.ups,
            'downs': submission.downs,
            'url': submission.url

        }

        submissions_col.update({'id': submission_obj['id']}, submission_obj, upsert=True)
        clone_submissions_col.update({'id': submission_obj['id']}, submission_obj, upsert=True)


        submission.comments.replace_more(limit=None)
        comment_queue = submission.comments[:]  # Seed with top-level
        while comment_queue:
            comment = comment_queue.pop(0)
            print(comment.author, comment.created)
            # print(comment.score, comment.subreddit_id, comment.ups, comment.downs)
            comment_queue.extend(comment.replies)

            author_obj = {}

            if comment.author is not None:
                author_obj = {
                    'id': comment.author.id,
                    'name': comment.author.name,
                    'has_subscribed': comment.author.has_subscribed,
                    'has_verified_email': comment.author.has_verified_email,
                    'created': comment.author.created,

                }

                print('sending to producer..')
                producer.send("reddit", author_obj)


                authors_col.update({'id': author_obj['id']}, author_obj, upsert=True)
                clone_user_authors_col.update({'id': author_obj['id']}, author_obj, upsert=True)

            parent_id_trimmed = comment.parent_id.split('_')[1]
            subreddit_id_trimmed = comment.subreddit_id.split('_')[1]

            comment_obj = {
                'id': comment.id,
                'created': comment.created,
                'subreddit_id': comment.subreddit_id,
                'is_root': comment.is_root,
                'parent_id': comment.parent_id,
                'body': comment.body,
                'ups': comment.ups,
                'downs': comment.downs,
                'score': comment.score,
                'author': author_obj,
                'parent_id_trimmed': parent_id_trimmed,
                'subreddit_id_trimmed': subreddit_id_trimmed

            }




            comments_col.update({'id': comment_obj['id']}, comment_obj, upsert=True)
            clone_comments_col.update({'id': comment_obj['id']}, comment_obj, upsert=True)
            # print('while ended')

    print('done.')
    print('sleeping...')
    time.sleep(100)




