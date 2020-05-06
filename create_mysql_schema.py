import mysql.connector

mydb = mysql.connector.connect(
  host="127.0.0.1",
  user="root",
  passwd="7dc41992"
)

mycursor = mydb.cursor()


try:
    mycursor.execute("drop database reddit;")
except:
    pass


try:
    mycursor.execute("CREATE DATABASE reddit")
    mycursor.execute("USE reddit")
except:
    pass



#Dimension tables
mycursor.execute("""CREATE TABLE user 
(id varchar(255) NOT NULL PRIMARY KEY, 
comment_karma int(11), 
created int(11), 
name VARCHAR(255),
fullname VARCHAR(255),
link_karma int(11))""")


mycursor.execute("""CREATE TABLE user_comments 
(id varchar(255) NOT NULL PRIMARY KEY, 
body VARCHAR(255), 
link_title VARCHAR(255), 
subreddit_display_name VARCHAR(255),
subreddit_id VARCHAR(255),
user_id VARCHAR(255),
user_name VARCHAR(255),
name VARCHAR(255),
ups int(11),
downs int(11),

FOREIGN KEY(user_id) REFERENCES user(id))
""")


mycursor.execute("""CREATE TABLE user_submissions 
(id varchar(255) NOT NULL PRIMARY KEY, 
title VARCHAR(255), 
ups int(11), 
downs int(11), 
name VARCHAR(255),
subreddit_display_name VARCHAR(255),
subreddit_name VARCHAR(255),
subreddit_id VARCHAR(255),
user_name VARCHAR(255),
user_id VARCHAR(255),
FOREIGN KEY(user_id) REFERENCES user(id))
""")



mycursor.execute("""CREATE TABLE coronavirus_submissions
(id varchar(255) NOT NULL PRIMARY KEY,
title VARCHAR(255),
created int(11),
selftext VARCHAR(255),
is_video BOOLEAN,
is_reddit_media_domain BOOLEAN,
media VARCHAR(255),
is_self BOOLEAN,
name VARCHAR(255),
ups int(11),
downs int(11),
url varchar(255),

author_id varchar(255),
FOREIGN KEY(author_id) REFERENCES user(id))
""")

mycursor.execute("""CREATE TABLE coronavirus_comments
(id varchar(255) NOT NULL PRIMARY KEY, 
created int(11), 
subreddit_id VARCHAR(255), 
is_root BOOLEAN,
parent_id varchar(255),
body VARCHAR(255), 
ups int(11),
downs int(11),
score int(11),
author_id VARCHAR(255),
parent_id_trimmed varchar(255),
subreddit_id_trimmed varchar(255),

FOREIGN KEY(author_id) REFERENCES user(id))
""")







print('created fact and dimension tables.')