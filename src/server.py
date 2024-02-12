# Core Functionality
# 1 - with Youtuber:
#   1.1 - consume videos uploaded from Youtuber.py - consumeVideos and print video.
# 2 - with User:
#   2.1 - consume login requests.
#   2.2 - consume subscription/unsubscription requests and print these.
#   2.3 - notify the user of new videos.

# Maintain a list of users, every user needs to have username, last login, 
# last logout, list of subscribed youtubers. 
# Maintain a list of youtubers, for every youtuber store the list of videos it has published.

import pika
import signal
import threading
from user_pb2 import User
from video_pb2 import Video
from datetime import datetime
import time
import sys
import os

class server():
   
    def __init__(self):

        self.HOST = 'localhost'
        self.SERVER_DOWN = False
        self.SERVER_AUTH_ROUTING_KEY = '0'
        self.SERVER_PUBLISH_ROUTING_KEY = '1'
        self.SERVER_SUBEQ_ROUTING_KEY = '2'

        # Stores users:userID
        self.Users = dict()
        # UserSubList[i] gives the sublist of Users[username]
        self.UserSubList = []
        # Stores Youtubers:YoutuberID
        self.Youtubers = dict()
        # YoutuberSubList[i] gives a list of all users that sub to Youtubers[youtuber_name]
        self.YoutuberSubList = []

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(self.HOST))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='', exchange_type='direct')
        self.queue_declare(queue='auth_queue')
        self.queue_declare(queue='pub_queue')
        self.queue_declare(queue='notif_queue')
        