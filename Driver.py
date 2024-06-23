import os
import time
import event_broker as e
import threading
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

class Driver:
    def __init__(self, driverID, location):
        self.ID = driverID
        self.location = location
        self.event_broker = e.Event_broker()
        self.setting()
        
    def __del__(self):
        del self.event_broker
    
    def setting(self):
        self.event_broker.set_guranteed_publisher()
        self.event_broker.set_request_reply_mode()
        self.read_request_reply_thread = threading.Thread(target=self.receive_request_reply, args=("accept",))
        self.read_request_reply_thread.start()
        
    def receive_request_reply(self, result_str):
        while(1):
            topic_str = 'HJ/driver/PickupRequest'
            topic_subscription = TopicSubscription.of(topic_str)
            request_receiver: RequestReplyMessageReceiver = self.event_broker.messaging_service.request_reply() \
                .create_request_reply_message_receiver_builder().build(topic_subscription).start()

            msg, replier = request_receiver.receive_message(timeout=5000)
            if replier == None:
                continue
            msg = msg.get_payload_as_string()
            msg_split = msg.split(' ')
            print('---------------------------------')
            print('Driver : User Ride Request receive')
            print('---------------------------------')
            print(f'Time: {msg_split[0]}')
            print(f'UserID: {msg_split[1]}')
            print(f'Current_location: {msg_split[2]}')
            print(f'Destination: {msg_split[3]}')
            print('---------------------------------')
            self.user_ID = msg_split[1]
            self.user_loc = msg_split[2]
            self.user_Dest = msg_split[3]
            #topic = 'HJ/taxi_platform/PickupRequestResponse'
            date = datetime.now()
            time = date.strftime('%Y-%m-%d-%H:%M:%S')
            msg =  time + ' ' + self.ID + ' ' + self.location + ' ' + result_str
            #todo 6/12 
            if replier is not None:
                outbound_msg = self.event_broker.messaging_service.message_builder().build(msg)
                replier.reply(outbound_msg)
            
            #result = self.event_broker.request_reply.receive_msg_and_response_async(topic_str)
    
    def pickup_complete(self):
        topic_str  ='HJ/taxiPlatform/PickupComplete/' + self.ID
        date = datetime.now()
        time = date.strftime('%Y-%m-%d-%H:%M:%S')
        msg =  time + ' ' + self.ID + ' ' + self.user_loc + ' ' + self.user_ID
        self.event_broker.send_msg_guranteed(topic_str, msg)
        print('---------------------------------')
        print('Driver : Driver PickupComplete send')
        print('---------------------------------')
        print(f'Time: {time}')
        print(f'DriverID: {self.ID}')
        print(f'location : {self.user_loc}')
        print(f'Destination: {self.user_Dest}')
        print('---------------------------------')
        
    def drop_off_complte(self):
        topic_str  ='HJ/payment/DropoffComplete/' + self.user_ID
        date = datetime.now()
        time = date.strftime('%Y-%m-%d-%H:%M:%S')
        msg =  time + ' ' + self.ID + ' ' + self.user_loc + ' ' + self.user_ID
        self.event_broker.send_msg_guranteed(topic_str, msg)
        print('---------------------------------')
        print('Driver : Driver Drop_off_Complte send')
        print('---------------------------------')
        print(f'Time: {time}')
        print(f'DriverID: {self.ID}')
        print(f'location : {self.user_loc}')
        print(f'UserID: {self.user_ID}')
        print('---------------------------------')
    
def main():
    pass

if __name__ == "__main__":    
    main()
