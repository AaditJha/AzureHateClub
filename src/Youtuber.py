# Core Functionality
# Publish Video - with 2 args username videoname, if username doesn't exists in YoutubeServer create new Youtuber.

import sys
import time
import pika
from datetime import datetime
import video_pb2

SERVER_AUTH_ROUTING_KEY = 'auth_queue'
SERVER_PUBLISH_ROUTING_KEY = 'pub_queue'
SERVER_SUBREQ_ROUTING_KEY = 'subreq_queue'
HOST = 'localhost'

class YouTuber():
    def __init__(self, youtuber:str, video_name:str):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(HOST))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=SERVER_PUBLISH_ROUTING_KEY, durable=True)

        message = self.createVideoMessage(youtuber, video_name)
        self.publishVideo(message)

    def createVideoMessage(self, youtuber:str, video_name:str) -> str:
        video = video_pb2.Video()
        video.youtuber_name = youtuber
        video.video_name = video_name
        message = video.SerializeToString()
        return message
    
    def publishVideo(self, message:str):
        self.channel.basic_publish(
            exchange='',
            routing_key=SERVER_PUBLISH_ROUTING_KEY,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )

if __name__ == '__main__':
    youtuber = sys.argv[1]
    video_name = ' '.join(sys.argv[2:])
    YouTuber(youtuber, video_name)



