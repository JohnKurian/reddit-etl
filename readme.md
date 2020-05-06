# Start zookeeper

#start the mongodb server
screen -S mongod -dm mongod --dbpath /usr/local/var/mongodb


# start producer
screen -S producer -dm python3.7 fetch_subreddit_batch.py

# start user submission consumer
screen -S user_submission_consumer -dm python3.7 fetch_user_related_submissions.py

# start user details consumer
screen -S user_details_consumer -dm python3.7 fetch_user_details.py

# start user comment consumer
screen -S user_comment_consumer -dm python3.7 fetch_user_comments.py

# start cloning 
screen -S cloning_db -dm python3.7 create_buffer_collections.py