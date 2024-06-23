import os
import time
import threading
import event_broker as e
from datetime import datetime
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor

from solace.messaging.config.solace_properties.message_properties import SEQUENCE_NUMBER
from solace.messaging.messaging_service import MessagingService
from solace.messaging.publisher.outbound_message import OutboundMessage
from solace.messaging.publisher.request_reply_message_publisher import RequestReplyMessagePublisher
from solace.messaging.receiver.request_reply_message_receiver import RequestReplyMessageReceiver, \
    RequestMessageHandler, Replier
from solace.messaging.resources.topic import Topic
from solace.messaging.resources.topic_subscription import TopicSubscription

class Company:
    def __init__(self):
        self.event_broker = e.Event_broker()
        self.setting()
    
    def __del__(self):
        pass
    
    def setting(self):
        self.event_broker.set_guranteed_publisher()
        self.event_broker.set_guranteed_receiver('Q.HJ.Company')
        self.read_request_thread_func = threading.Thread(target=self.receive_paymentRequest)
        self.read_request_thread_func.start()
    
    def receive_paymentRequest(self):
        while(1):
            topic, payload = self.event_broker.read_msg_guranteed()
            if topic == None:
                continue
            
            split_topic = topic.split('/')
            if split_topic[2] == 'PaymentRequest':                
                msg = payload.split(' ')
                print('---------------------------------')
                print('Company : PaymentRequest receive')
                print('---------------------------------')
                print(f'Time: {msg[0]}')
                print(f'UserID: {msg[1]}')
                print(f'Cost: {msg[2]}')
                print('---------------------------------')
                userID = msg[1]
                cost = msg[2]
                self.send_paymentRequest(userID, cost)

                
    def send_paymentRequest(self, userID, cost):
        topic_str  ='HJ/User/PaymentRequest/' + userID 
        date = datetime.now()
        time = date.strftime('%Y-%m-%d-%H:%M:%S')
        msg =  time + ' ' + userID + ' ' + cost
        self.event_broker.send_msg_guranteed(topic_str, msg)
        print('---------------------------------')
        print('Company : PaymentRequest send')
        print('---------------------------------')
        print(f'Time: {time}')
        print(f'UserID: {userID}')
        print(f'Cost : {cost}')
        print('---------------------------------')