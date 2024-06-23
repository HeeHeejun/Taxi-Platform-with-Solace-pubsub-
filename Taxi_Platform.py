import os
import time
import threading
import event_broker as e

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

class Taxi_platform:
    def __init__(self):
        self.user = []
        self.driver = []
        self.event_broker = e.Event_broker()
        self.setting()
        
    def __del__(self):
        del self.event_broker
        
    def setting(self):
        queue_name = 'Q.HJ.taxi_platform'
        self.event_broker.set_guranteed_receiver(queue_name)
        self.event_broker.set_guranteed_publisher()
        self.event_broker.set_request_reply_mode()
        self.read_request_thread_func = threading.Thread(target=self.read_request_thread)
        self.read_request_thread_func.start()
        
    def read_request_thread(self):
        while(1):
            topic, payload = self.event_broker.read_msg_guranteed()
            if topic == None:
                continue
            
            #todo 
            split_topic = topic.split('/')

            if split_topic[2] == 'RideRequest':                
                msg = payload.split(' ')
                print('---------------------------------')
                print('Taxi platform : User Ride Request receive')
                print('---------------------------------')
                print(f'Time: {msg[0]}')
                print(f'UserID: {msg[1]}')
                print(f'Current_location: {msg[2]}')
                print(f'Destination: {msg[3]}')
                print('---------------------------------')
                driver_topic = 'HJ/driver/PickupRequest'
                user_ID = msg[1]
                reply_timeout = 10000
                requester: RequestReplyMessagePublisher = self.event_broker.messaging_service.request_reply() \
                .create_request_reply_message_publisher_builder().build().start()

                ping_message = self.event_broker.messaging_service.message_builder().build(payload=payload,
                                                            additional_message_properties={SEQUENCE_NUMBER: 123})

                publish_request_async = requester.publish(request_message=ping_message,
                                                        request_destination=Topic.of(driver_topic),
                                                        reply_timeout=reply_timeout)
                r = publish_request_async.result().get_payload_as_string()
                msg_split = r.split(' ')
                print('---------------------------------')
                print('Taxi platform : Driver PickupRequestResponse receive')
                print('---------------------------------')
                print(f'Time: {msg_split[0]}')
                print(f'DriverID: {msg_split[1]}')
                print(f'Current_location: {msg_split[2]}')
                print(f'Result: {msg_split[3]}')
                print('---------------------------------')
                user_topic = 'HJ/User/RideRequestResponse/' + user_ID
                self.event_broker.guranteed_publisher.send_guaranteed_message(r, user_topic)
                
                #self.event_broker.request_reply.send_msg_and_response_async(driver_topic, msg)
            elif split_topic[2] == 'PickupComplete':
                msg = payload.split(' ')
                print('---------------------------------')
                print('Taxi platform : Driver PickupComplete receive')
                print('---------------------------------')
                print('---------------------------------')
                print(f'Time: {msg[0]}')
                print(f'UserID: {msg[1]}')
                print(f'pick up location : {msg[2]}')
                print(f'Destination: {msg[3]}')
                print('---------------------------------')
                
    
    def add_user(self, user_id):
        self.user.append(user_id)
    
    def add_driver(self, driver_id):
        self.driver.append(driver_id)
        
    #def 