# Core functionality 
# 1 - Create an account or authentication using communication with the server.
# 2 - get a list of videos posted by subbed channels since last logout - getNotifications; 1 command line arg - username
# 3 - Subscribe/unsub to channels - updateSubscription; 3 command line args - username <u/s> channelname.
# 4 - Write a SIGINT handler to logout, updates last logout time in the server then exits User.py.

import sys
import pika
import signal
from datetime import datetime
import user_pb2
import subrequest_pb2
import video_pb2

LOGOUT_FLAG = False 
SERVER_AUTH_ROUTING_KEY = 'auth_queue'
SERVER_PUBLISH_ROUTING_KEY = 'pub_queue'
SERVER_SUBREQ_ROUTING_KEY = 'subreq_queue'
HOST = '34.31.25.112'

def signalHandler(sig, _):
    print("\nShutting Server")
    sys.exit(0)

class User():
    def __init__(self, username:str, sub:str=None, channel_name:str=None):
        signal.signal(signal.SIGINT, self.signalHandler)

        self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=HOST, credentials=pika.PlainCredentials('aadit','1234'), port=5672)
            )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=SERVER_AUTH_ROUTING_KEY, durable=True)

        self.name = username
        message = self.createAuthMessage(username)
        self.sendMessage(message, SERVER_AUTH_ROUTING_KEY)
        print(f"Welcome, {self.name}!")

        if sub:
            self.channel.queue_declare(queue=SERVER_SUBREQ_ROUTING_KEY, durable=True)
            message = self.createSubRequestMessage(username, sub, channel_name)
            self.sendMessage(message, SERVER_SUBREQ_ROUTING_KEY)

        self.getNotifications()
        

    def createAuthMessage(self, username:str) -> str:
        user = user_pb2.User()
        user.name = username
        message = user.SerializeToString()
        return message
    
    def createSubRequestMessage(self, username:str, sub:str, sub_channel:str) -> str:
        subreq = subrequest_pb2.SubRequest()
        subreq.name = username
        subreq.status = subrequest_pb2.SubRequest.Status.SUBSCRIBE if sub == 's' else subrequest_pb2.SubRequest.Status.UNSUBSCRIBE
        subreq.youtuber_name = sub_channel
        message = subreq.SerializeToString()
        return message
    
    def sendMessage(self, message:str, _routing_key:str):
        self.channel.basic_publish(
            exchange='',
            routing_key=_routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2)
        )

    def getNotifications(self):
        QUEUE = f"{self.name}_QUEUE"
        self.channel.queue_declare(queue=QUEUE, durable=True)
        self.channel.basic_consume(queue=QUEUE, on_message_callback=self.notificationCallback, auto_ack=True)
        self.channel.start_consuming()

    def notificationCallback(self, ch, method, properties, body):
        video_message = video_pb2.Video()
        video_message.ParseFromString(body)
        video = video_message.video_name
        youtuber = video_message.youtuber_name
        print(f"{youtuber} posted '{video}'.")

    def receiveNotifCallback(self, ch, method, properties, body):
        video_message = video_pb2.Video()
        video_message.ParseFromString(body)
        print(f"{video_message.youtuber_name} posted '{video_message.video_name}'.")

    def signalHandler(self, sig, _):
        print("\nLogging out.")
        self.connection.close()
        sys.exit(0)


if __name__ == '__main__':
    n_args = len(sys.argv)
    username = sys.argv[1]
    channel_name = None
    sub = None 

    if n_args > 3:
        channel_name = sys.argv[-1]
        sub = sys.argv[2]

    User(username=username, sub=sub, channel_name=channel_name)