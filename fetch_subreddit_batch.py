import praw
import datetime
from pymongo import MongoClient

myclient = MongoClient()
redditdb = myclient["reddit"]
comments_col = redditdb["comments"]


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


submission_list = []
for submission in subreddit.new(limit=100):
    submission.comments_col.replace_more(limit=None)
    comment_queue = submission.comments_col[:]  # Seed with top-level
    while comment_queue:
        comment = comment_queue.pop(0)
        print(comment.author, comment.created)
        # print(comment.score, comment.subreddit_id, comment.ups, comment.downs)
        comment_queue.extend(comment.replies)

        author_obj = {
            'id': comment.author.id,
            'name': comment.author.name,
            'has_subscribed': comment.author.has_subscribed,
            'has_verified_email': comment.author.has_verified_email,
            'created': comment.author.created,

        }

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
            'author': author_obj

        }




        comments_col.update({'id': comment_obj['id']}, comment_obj, {'upsert': True})
        print('while ended')

print('done.')




