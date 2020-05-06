#Download driver from https://dev.mysql.com/downloads/connector/python/

#pip install mysql-connector-python

#start daemon
#sudo /usr/local/mysql/bin/mysqld_safe

#create database twitterdb
#

#to enter into the terminal
#/usr/local/mysql/bin/mysql -u root -p
#/usr/local/mysql/bin/mysqld --user=_mysql --basedir=/usr/local/mysql --datadir=/usr/local/mysql/data --plugin-dir=/usr/local/mysql/lib/plugin --log-error=/usr/local/mysql/data/mysqld.local.err --pid-file=/usr/local/mysql/data/mysqld.local.pid --keyring-file-data=/usr/local/mysql/keyring/keyring --early-plugin-load=keyring_file=keyring_file.so


import mysql.connector
from mysql.connector import Error
from pymongo import MongoClient
import time
import subprocess
from subprocess import PIPE





while True:

    print('dropping tables..')

    p = subprocess.Popen(["python3.7", "create_mysql_schema.py"], stdin=PIPE, stdout=PIPE)
    p.communicate()

    con = mysql.connector.connect(host='127.0.0.1',
                                          database='reddit',
                                          user='root',
                                          password='7dc41992',
                                          charset='utf8',
                                          auth_plugin='mysql_native_password')



    print('starting sql dump loop...')
    myclient = MongoClient()
    redditdb = myclient["reddit"]



    print('creating user table...')
    user = redditdb['user']
    cursor = user.find({})

    for document in cursor:
        if 'id' in document:
            try:
                if con.is_connected():
                    """
                    Insert twitter data
                    """


                    if document['id'] is not "is_suspended":
                        cursor = con.cursor()
                        query = "INSERT INTO user (id, comment_karma, created, name, fullname, link_karma) VALUES (%s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (
                        document['id'], document['comment_karma'], document['created'], document['name'], document['fullname'], document['link_karma']))
                        con.commit()
                        cursor.close()


            except Error as e:
                print(e)

    print('creating user comments...')
    user_comments = redditdb['user.comments']
    cursor = user_comments.find({})

    for document in cursor:
        if 'id' in document:
            try:
                if con.is_connected():
                    """
                    Insert twitter data
                    """

                    if document['id'] is not "is_suspended":
                        cursor = con.cursor()
                        query = "INSERT INTO user_comments (id, body, link_title, name, subreddit_id, subreddit_display_name, ups, downs, user_name, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (
                            document['id'], document['body'], document['link_title'], document['name'],
                            document['subreddit_id'], document['subreddit_display_name'], document['ups'], document['downs'], document['user_name'], document['user_id']))
                        con.commit()
                        cursor.close()


            except Error as e:
                print(e)






    print('adding user submissions..')
    user_submissions = redditdb['user.submissions']
    cursor = user_submissions.find({})

    for document in cursor:
        if 'id' in document:
            try:
                if con.is_connected():
                    """
                    Insert twitter data
                    """

                    if document['subreddit_display_name'] and document['user_name'] and  document['user_id']:
                        cursor = con.cursor()
                        query = "INSERT INTO user_submissions (id, title, ups, downs, name, subreddit_display_name, subreddit_name, subreddit_id, user_name, user_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                        cursor.execute(query, (
                            document['id'], document['title'], document['ups'], document['downs'],
                            document['name'], document['subreddit_display_name'], document['subreddit_name'], document['subreddit_id'], document['user_name'], document['user_id']))
                        con.commit()
                        cursor.close()


            except Error as e:
                print(e)



    print('adding coronavirus submissions..')
    coronavirus_submissions = redditdb['coronavirus.submissions']
    cursor = coronavirus_submissions.find({})

    for document in cursor:
        if 'id' in document:
            try:
                if con.is_connected():
                    """
                    Insert twitter data
                    """

                    if document['id'] is not "is_suspended":

                        media = 'none'

                        if document['media']:
                            media = document['media']['type']
                        else:
                            media = ''

                        cursor = con.cursor()
                        query = "INSERT INTO coronavirus_submissions (id, title, created, selftext, is_video, is_reddit_media_domain, media, author_id, is_self, name, ups, downs, url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s)"
                        cursor.execute(query, (
                            document['id'], document['title'], document['created'], document['selftext'],
                            document['is_video'], document['is_reddit_media_domain'], media, document['author_id'], document['is_self'], document['name'], document['ups'], document['downs'], document['url']))
                        con.commit()
                        cursor.close()


            except Error as e:
                print(e)



    print('adding coronavirus comments..')
    coronavirus_comments = redditdb['coronavirus.comments']
    cursor = coronavirus_comments.find({})

    for document in cursor:
        if 'id' in document:
            try:
                if con.is_connected():
                    """
                    Insert twitter data
                    """

                    if document['id'] is not "is_suspended" and document['author']:
                        cursor = con.cursor()
                        query = "INSERT INTO coronavirus_comments (id, created, subreddit_id, is_root, parent_id, body, ups, downs, score, author_id, parent_id_trimmed, subreddit_id_trimmed) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s)"
                        cursor.execute(query, (
                            document['id'], document['created'], document['subreddit_id'], document['is_root'],
                            document['parent_id'], document['body'], document['ups'], document['downs'], document['score'], document['author']['id'], document['parent_id_trimmed'], document['subreddit_id_trimmed']))
                        con.commit()
                        cursor.close()


            except Error as e:
                print(e)
    con.close()
    print('done.')
    print('sleeping..')
    time.sleep(1000)
