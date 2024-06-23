import argparse, sys

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener, FailedPublishEvent
from solace.messaging.resources.topic_subscription import TopicSubscription
from solace.messaging.receiver.message_receiver import MessageHandler
from solace.messaging.config.solace_properties.message_properties import APPLICATION_MESSAGE_ID
from solace.messaging.resources.topic import Topic
from solace.messaging.receiver.inbound_message import InboundMessage


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

class DirectReceiver:
    def __init__(self, messaging_service, topic):
        self.messaging_service = messaging_service
        self.create_direct_receiver(topic)
        
    def __del__(self):
        self.direct_receiver.terminate()

    def create_direct_receiver(self, topic):
        self.direct_receiver = self.messaging_service.create_direct_message_receiver_builder().with_subscriptions(topic).build()
        self.direct_receiver.start()
        
    def read_Direct_message(self):
        if self.direct_receiver :
            self.direct_receiver.receive_async(MessageHandlerImpl())
            if self.direct_receiver.is_running() :
                print("Connected and Subscrived! Ready to publish\n")
        else:
            print('Direct receiver is not created')
    
    def add_topic(self, topic, type = "Direct"):
        if self.direct_receiver:
            self.direct_receiver.add_subscription(topic)
        else:
            print("direct_receiver is not created")

def main(argv, args):
    pass
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Event Broker')
    parser.add_argument('--type', required=True, help='Pub or Sub')
    #parser.add_argument('--mode', required=True, help='Direct or Guaranteed')

    args = parser.parse_args()
    print('Event Broker Test')
    print(f'type is {args.type}')
    #print(f'mode is {args.mode}')
    argv = sys.argv
    main(argv, args)