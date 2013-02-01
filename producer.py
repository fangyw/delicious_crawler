# -*- coding: utf-8 -*-
# Copyright (C) 2012 young001 <iamyoung001@gmail.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

import urllib
from lxml import etree
from deliciousapi import *
from utils import *
import MySQLdb
from meliae import scanner
from twisted.enterprise import adbapi
from twisted.internet.threads import deferToThread
from logger import get_logger
import redis

r = redis.Redis(host='125.221.225.12',db=9)

dbpool = adbapi.ConnectionPool('MySQLdb', db='delicious',user='young001', passwd='young001', charset='utf8', use_unicode=True)

db=MySQLdb.connect(user='root',passwd='young001',charset='utf8',db='delicious',use_unicode=True)  
cur=db.cursor()  

dapi = DeliciousAPI()
user = 'mrdukeofoil'

def insert_bookmarks(dapi,user):
    '''
    insert all the user's bookmarks into database
    @param user: the username
    @type:string
    '''
    user_tags = dapi.get_tags_of_user(user)
    if tags_is_english(user_tags):
        bookmarks = dapi.get_bookmarks(username=user,max_bookmarks=0) 
        for i in bookmarks:
            _bool = r.sadd('urls',i[0])
            if _bool:
                r.lpush('job_queue',i[0])
            if i[1]:
                for tag in i[1]:
                    print 'the bookmark is', i
                    #print 'now is %s,%s,%s'%(user,i[0],tag)
                    cur.execute("insert into bookmark_e(user,url,tag) values (%s,%s,%s)",(user.encode('utf-8'),i[0].encode('utf-8'),tag.encode('utf-8')))
    else:
        bookmarks = dapi.get_bookmarks(username=user,max_bookmarks=0) 
        for i in bookmarks:
            _bool = r.sadd('urls',i[0])
            if _bool:
                r.lpush('job_queue',i[0])
            r.sadd('urls',i[0])
            if i[1]:
                for tag in i[1]:
                    print 'the bookmark is', i
                    cur.execute("insert into bookmark_c(user,url,tag) values (%s,%s,%s)",(user.encode('utf-8'),i[0].encode('utf-8'),tag.encode('utf-8')))
    db.commit()
    r.sadd('users_visited',user)


def get_users(dapi,url):
    '''
    get users from the url
    @param url:the url
    @type:string
    '''
    users = set()
    bookmarks = dapi.get_bookmarks(url=url)
    for i in bookmarks:
        users.add(i[0])
        r.sadd('users',i[0])

    return users


while True:
    url = r.brpop('job_queue')
    scanner.dump_all_objects("dump.txt")

    try:
        users = get_users(dapi,url[1])
        r.sadd('urls_visited',url[1])
    except:
        pass
    #print 'users is', users
    for user in users:
        if not r.sismember('users_visited',user):
            try:
                insert_bookmarks(dapi,user)
            except:
                pass
        else:
            pass

