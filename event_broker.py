import json
import argparse, sys
import time

import direct_publisher as dipub
import direct_receiver as direc
import guaranteed_publisher as gpub
import guaranteed_receiver as grec
import request_reply as rr

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.inbound_message import InboundMessage

TOPIC_PREFIX = "HJ"

class MessageHandlerImpl(MessageHandler):
    def on_message(self, message: 'InboundMessage'):
        global SHUTDOWN
        if "quit" in message.get_destination_name():
            print("QUIT message received, shutting down.")
            SHUTDOWN = True 
            
        # Check if the payload is a String or Byte, decode if its the later
        payload = message.get_payload_as_string() if message.get_payload_as_string() != None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            print(f"Received a message of type: {type(payload)}. Decoding to string")
            payload = payload.decode()
        
        print("\n" + f"Message payload: {payload} \n")
        print("\n" + f"Message dump: {message} \n")

class ServiceEventHandler(ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener):
    def on_reconnected(self, e: ServiceEvent):
        print("\non_reconnected")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")
    
    def on_reconnecting(self, e: "ServiceEvent"):
        print("\non_reconnecting")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

    def on_service_interrupted(self, e: "ServiceEvent"):
        print("\non_service_interrupted")
        print(f"Error cause: {e.get_cause()}")
        print(f"Message: {e.get_message()}")

class PublisherErrorHandling(PublishFailureListener):
    def on_failed_publish(self, e: "FailedPublishEvent"):
        print("on_failed_publish")

class Event_broker:
    def __init__(self, json_properties_file_path = './solace_broker_properties.json'):
        self.setting(json_properties_file_path)
        self.connect()
        self.set_event_handler()
        self.direct_publisher=None
        self.direct_receiver=None

    def __del__(self):
        if self.direct_publisher:
            del self.direct_publisher
        if self.direct_receiver:
            del self.direct_receiver
        print('Disconnecting Messaging Service')
        self.messaging_service.disconnect()
    
    def setting(self, properties_file):
        with open(properties_file, 'r') as f:
            json_data = json.load(f)
            self.broker_host = json_data["solbrokerProperties"]["solace.messaging.transport.host"]
            self.broker_vpn = json_data["solbrokerProperties"]["solace.messaging.service.vpn-name"]
            self.user_id = json_data["solbrokerProperties"]["solace.messaging.authentication.basic.username"]
            self.user_password = json_data["solbrokerProperties"]["solace.messaging.authentication.basic.password"]
            self.broker_props = {
                "solace.messaging.transport.host" : self.broker_host,
                "solace.messaging.service.vpn-name" : self.broker_vpn,
                "solace.messaging.authentication.scheme.basic.username" : self.user_id,
                "solace.messaging.authentication.scheme.basic.password" : self.user_password
            }
        
        self.messaging_service = MessagingService.builder().from_properties(self.broker_props)\
                    .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(20,3))\
                    .build()
    
    def connect(self):
        self.messaging_service.connect()
        
    def set_event_handler(self):
        self.service_handler = ServiceEventHandler()
        self.messaging_service.add_reconnection_listener(self.service_handler)
        self.messaging_service.add_reconnection_attempt_listener(self.service_handler)
        self.messaging_service.add_service_interruption_listener(self.service_handler)
    
    def set_direct_publisher(self):
        self.direct_publisher = dipub.DirectPublisher(self.messaging_service)
    
    def set_direct_receiver(self, topic):
        self.direct_receiver = direc.DirectReceiver(self.messaging_service, topic)
    
    def set_request_reply_mode(self):
        self.request_reply = rr.Request_reply(self.messaging_service)
    
    def set_guranteed_publisher(self):
        self.guranteed_publisher = gpub.GuaranteedPublisher(self.messaging_service)
    
    def set_guranteed_receiver(self, queue_str):
        self.guranteed_receiver = grec.GuaranteedReceiver(self.messaging_service, queue_str)
            
    def send_msg(self, guaranteed = False, topic = None, queue = None, message = None, request_reply = False):
        if request_reply:
            pass
        if guaranteed and queue:
            pass
        elif guaranteed and topic:
            self.send_msg_guranteed(topic, message)
        elif guaranteed == False and queue:
            pass
        else :
            self.send_msg_direct(topic, message)
    
    def read_msg_guranteed(self):
        return self.guranteed_receiver.msghandler.get_data()
            
    def send_msg_direct(self, topic, message):
        if topic and message:
            self.direct_publisher.send_Direct_message(message, topic)
            print('send direct message')
            
    def read_msg_direct(self):
        self.direct_receiver.read_Direct_message()
        
    def send_msg_guranteed(self, topic, message):
        if topic and message:
            self.guranteed_publisher.send_guaranteed_message(message, topic)


def main(argv, args):
    unique_name = ""
    while not unique_name:
        unique_name = input("Enter your name: ").replace(" ", "")
    
    topic = [TOPIC_PREFIX + "/*/test/>"]
    topin_sub = []
    queue_name = 'Q.HJ1'
    for t in topic:
        topin_sub.append(TopicSubscription.of(t))
        
    if args.type == 'pub':
        mEvent_broker = Event_broker(mode=args.type)
        message_body = f'Test Event_Broker!'
        if args.mode == 'Direct':
            mEvent_broker.set_direct_publisher()
            while 1:
                mEvent_broker.send_msg(message=message_body, topic=TOPIC_PREFIX + "/*/test/>")
                time.sleep(5)
        elif args.mode == 'Guaranteed':
            mEvent_broker.set_guranteed_publisher()
            while 1:
                mEvent_broker.send_msg(guaranteed=True, 
                                       topic=TOPIC_PREFIX + "/*/test/>",
                                       message=message_body)
                time.sleep(5)
            
    elif args.type == 'sub':
        mEvent_broker = Event_broker(mode=args.type)
        if args.mode == 'Direct':
            mEvent_broker.set_direct_receiver(topin_sub)
            
            mEvent_broker.read_msg()
            while 1:
                continue
            
        elif args.mode == 'Guaranteed':
            mEvent_broker.set_guranteed_receiver(queue_name)
            while 1:
                continue
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Event Broker')
    parser.add_argument('--type', required=True, help='Pub or Sub')
    parser.add_argument('--mode', required=True, help='Direct or Guaranteed')
    args = parser.parse_args()
    print('Event Broker Test')
    print(f'type is {args.type}')
    print(f'mode is {args.mode}')
    argv = sys.argv
    main(argv, args)