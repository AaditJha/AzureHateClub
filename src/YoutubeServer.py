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
from user_pb2 import User
from video_pb2 import Video
from subrequest_pb2 import SubRequest
import sys

HOST = 'localhost'
SERVER_DOWN = False
SERVER_AUTH_ROUTING_KEY = 'auth_queue'
SERVER_PUBLISH_ROUTING_KEY = 'pub_queue'
SERVER_SUBREQ_ROUTING_KEY = 'subreq_queue'


class Server():

    def __init__(self):
        print('Starting server.')
        signal.signal(signal.SIGINT, self.signalHandler)

        self.Users = dict()
        self.UserSubList = []
        self.Youtubers = dict()
        self.YoutuberSubList = []

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))
        self.channel = self.connection.channel()

        self.channel.queue_declare(queue=SERVER_AUTH_ROUTING_KEY, durable=True)
        self.channel.basic_consume(queue=SERVER_AUTH_ROUTING_KEY, on_message_callback=self.authCallback, auto_ack=True)

        self.channel.queue_declare(queue=SERVER_PUBLISH_ROUTING_KEY, durable=True)
        self.channel.basic_consume(queue=SERVER_PUBLISH_ROUTING_KEY, on_message_callback=self.publishCallback, auto_ack=True)

        self.channel.queue_declare(queue=SERVER_SUBREQ_ROUTING_KEY, durable=True)
        self.channel.basic_consume(queue=SERVER_SUBREQ_ROUTING_KEY, on_message_callback=self.subRequestCallback, auto_ack=True)

        self.channel.start_consuming()


    def authCallback(self, ch, method, properties, body):
        user_message = User()
        user_message.ParseFromString(body)
        
        if user_message.name not in self.Users.keys():
            self.Users[user_message.name] = len(self.Users)
            self.UserSubList.append(set())

        print(f"{user_message.name} logged in.")


    def publishCallback(self, ch, method, properties, body):
        video_message = Video()
        video_message.ParseFromString(body)
        
        if video_message.youtuber_name not in self.Youtubers.keys():
            self.Youtubers[video_message.youtuber_name] = len(self.Youtubers)
            self.YoutuberSubList.append(set())
        
        print(f"{video_message.youtuber_name} posted '{video_message.video_name}'.")

        self.sendNotifications(video_message.youtuber_name, video_message.video_name)

    def subRequestCallback(self, ch, method, properties, body):
        
        request = SubRequest()
        request.ParseFromString(body)
        user = request.name
        user_id = self.Users[user]
        youtuber = request.youtuber_name
        youtuber_id = self.Youtubers[youtuber]

        if request.status == SubRequest.Status.SUBSCRIBE:
            
            if youtuber not in self.UserSubList[user_id]:
                self.UserSubList[user_id].add(youtuber)
                self.YoutuberSubList[youtuber_id].add(user)
                print(f"{user} subscribed to {youtuber}.")
            else:
                print(f"{user} was already subscribed to {youtuber}.")

        else:
            
            if youtuber in self.UserSubList[user_id]:
                self.UserSubList[user_id].remove(youtuber)
                self.YoutuberSubList[youtuber_id].remove(user)
                print(f"{user} unsubscribed to {youtuber}.")
            else:
                print(f"{user} was not subscribed to {youtuber}.")

        # print(self.UserSubList)
        # print(self.YoutuberSubList)

    def sendNotifications(self, youtuber:str, video_name:str):
        youtuber_id = self.Youtubers[youtuber]
        subscribed_users = self.YoutuberSubList[youtuber_id]
        message = self.createNotificationMessage(youtuber, video_name)
        for user in subscribed_users:
            self.channel.basic_publish(exchange='', routing_key=f"{user}_QUEUE", body=message,
                                       properties=pika.BasicProperties(delivery_mode=2))

    def createNotificationMessage(self, youtuber:str, video_name:str) -> str:
        video = Video()
        video.youtuber_name = youtuber
        video.video_name = video_name
        message = video.SerializeToString()
        return message
    
    def signalHandler(self, sig, _):
        print("\nShutting Server.")
        self.connection.close()
        sys.exit(0)

if __name__ == '__main__':
    Server()