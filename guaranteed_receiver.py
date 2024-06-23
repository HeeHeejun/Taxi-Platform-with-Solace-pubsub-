import os
import time

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, ServiceEvent
from solace.messaging.resources.queue import Queue
from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.receiver.persistent_message_receiver import PersistentMessageReceiver
from solace.messaging.receiver.message_receiver import MessageHandler, InboundMessage
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.config.missing_resources_creation_configuration import MissingResourcesCreationStrategy

class MessageHandlerImpl(MessageHandler):
    def __init__(self, persistent_receiver: PersistentMessageReceiver):
        self.receiver: PersistentMessageReceiver = persistent_receiver
        self.payload = []
        
    def on_message(self, message: InboundMessage):
        # Check if the payload is a String or Byte, decode if its the later
        payload = message.get_payload_as_string() if message.get_payload_as_string() != None else message.get_payload_as_bytes()
        if isinstance(payload, bytearray):
            print(f"Received a message of type: {type(payload)}. Decoding to string")
            payload = payload.decode()

        topic = message.get_destination_name()
        print("\n" + f"Received message on: {topic}")
        print("\n" + f"Message payload: {payload} \n")
        self.receiver.ack(message)
        msg = {}
        msg['topic'] = topic
        msg['payload'] = payload
        self.payload.append(msg)
        # print("\n" + f"Message dump: {message} \n")
    
    def get_data(self):
        if len(self.payload) != 0:
            item = self.payload.pop(0)
            return item['topic'], item['payload']
        return None, None

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

class GuaranteedReceiver:
    def __init__(self, messaging_service, queue_name):
        self.messaging_service = messaging_service
        self.queue_name = queue_name
        self.durable_exclusive_queue = Queue.durable_exclusive_queue(queue_name)
        self.create_guaranteed_receiver()
    def __del__(self):
        self.persistent_receiver.terminate(grace_period = 0)
    
    def create_guaranteed_receiver(self):
        try:
            self.persistent_receiver : PersistentMessageReceiver = self.messaging_service.create_persistent_message_receiver_builder()\
                .with_message_auto_acknowledgement()\
                .with_missing_resources_creation_strategy(MissingResourcesCreationStrategy.DO_NOT_CREATE)\
                .build(self.durable_exclusive_queue)
            self.persistent_receiver.start()
            self.msghandler = MessageHandlerImpl(self.persistent_receiver)
            self.persistent_receiver.receive_async(self.msghandler)
            print(f'PERSISTENT receiver started... Bound to Queue [{self.durable_exclusive_queue.get_name()}]')
        
        except PubSubPlusClientError as exception:
            print(f'\nMake sure queue {self.queue_name} exists on broker!')
            #HowToConsumeMessageExclusiveVsSharedMode.delete_queue(self.queue_name)
            #self.create_guaranteed_receiver()

