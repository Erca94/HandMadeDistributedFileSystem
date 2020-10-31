import pymongo
import json
import sys
from bson.objectid import ObjectId
import datetime 

namenode = 'namenode1'

#get the configurations
with open('conf.json') as f: 
    config = json.load(f) 

host = config['namenodes_setting'][namenode]['host_metadata']
port = config['namenodes_setting'][namenode]['port_metadata']
#connect to MongoDB with a client
client = pymongo.MongoClient('mongodb://{}:{}/'.format(host, port))
db = client['metadatafs']
fs = db['fs']
groups = db['groups']
users = db['users']
trash = db['trash']

#clear the metadata and the namespace
fs.delete_many({})
groups.delete_many({})
users.delete_many({})
trash.delete_many({})

#create the "root" user object
root_usr = { "_id" : ObjectId("111111111111111111111111"), "name" : "root", "password" : "root1.", "creation" : "1970-01-01 00:00:00", "groups" : [ "root" ] }
#create the default "user" user object
user_usr = { "_id" : ObjectId("111111111111111111111112"), "name" : "user", "password" : "user1.", "creation" : "1970-01-01 00:00:00", "groups" : [ "user" ] }
#create the "root" group object
root_grp = { "_id" : ObjectId("111111111111111111111111"), "name" : "root", "creation" : "1970-01-01 00:00:00", "users" : [ "root" ] }
#create the "user" group object
user_grp = { "_id" : ObjectId("111111111111111111111112"), "name" : "user", "creation" : "1970-01-01 00:00:00", "users" : [ "user" ] }
#create the root directory
root = { "_id" : ObjectId("111111111111111111111111"), "name" : "/", "parent" : None, "type" : "d", "files" : [ ], "directories" : [ "user" ], "creation" : "1970-01-01 00:00:00", "own" : "root", "grp" : "root", "mod" : { "own" : 7, "grp" : 5, "others" : 5 } }
#create the "user" home directory
user_dir = { "_id" : ObjectId("111111111111111111111112"), "name" : "user", "parent" : ObjectId("111111111111111111111111"), "type" : "d", "files" : [ ], "directories" : [ ], "creation" : "1970-01-01 00:00:00", "own" : "user", "grp" : "user", "mod" : { "own" : 7, "grp" : 5, "others" : 5 } }

#insert into MongoDB
root_usr_id = users.insert_one(root_usr).inserted_id
print("\"root\" user created: MongoDB id {}".format(root_usr_id))
user_usr_id = users.insert_one(user_usr).inserted_id
print("\"user\" user created: MongoDB id {}".format(user_usr_id))
root_grp_id = groups.insert_one(root_grp).inserted_id
print("\"root\" group created: MongoDB id {}".format(root_grp_id))
user_grp_id = groups.insert_one(user_grp).inserted_id
print("\"user\" group created: MongoDB id {}".format(user_grp_id))
root_id = fs.insert_one(root).inserted_id
print("root directory created: MongoDB id {}".format(root_id))
user_id = fs.insert_one(user_dir).inserted_id
print("\"user\" home directory created: MongoDB id {}".format(user_id))
