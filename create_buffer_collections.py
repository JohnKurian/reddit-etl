from pymongo import MongoClient
import time





while True:
    myclient = MongoClient()
    redditdb = myclient["reddit"]

    user_submissions_col = redditdb["user.submissions"]
    user_comments_col = redditdb["user.comments"]
    authors_col = redditdb["user.authors"]
    user_col = redditdb["user"]

    submissions_col = redditdb["coronavirus.submissions"]
    comments_col = redditdb["coronavirus.comments"]
    user_authors_col = redditdb["coronavirus.authors"]

    print('dropping..')


    try:
        redditdb.clone_user_submissions_col.drop()
    except:
        print('clone_user_submissions_col error')
        pass

    try:
        redditdb.clone_user_comments_col.drop()
    except:
        print('clone_user_comments_col error')
        pass


    try:
        redditdb.clone_user_authors_col.drop()
    except:
        print('clone_user_authors_col error')
        pass


    try:
        redditdb.clone_user_col.drop()
    except:
        print('clone_user_col error')
        pass





    try:
        redditdb.clone_submissions_col.drop()
    except:
        print('clone_submissions_col error')
        pass

    try:
        redditdb.clone_comments_col.drop()
    except:
        print('clone_comments_col error')
        pass

    try:
        redditdb.user_authors_col.drop()
    except:
        print('user_authors_col error')
        pass



    print('cloning..')


    for x in user_submissions_col.find({}):
        redditdb.clone_user_submissions_col.insert(x)

    for x in user_comments_col.find({}):
        redditdb.clone_user_comments_col.insert(x)


    for x in authors_col.find({}):
        redditdb.clone_user_authors_col.insert(x)

    for x in user_col.find({}):
        redditdb.clone_user_col.insert(x)




    for x in submissions_col.find({}):
        redditdb.clone_submissions_col.insert(x)

    for x in comments_col.find({}):
        redditdb.clone_comments_col.insert(x)

    for x in user_authors_col.find({}):
        redditdb.clone_user_authors_col.insert(x)


    print('cloning done.')
    print('sleeping for 5 mins...')
    time.sleep(300)


