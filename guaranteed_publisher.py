# Guaranteed Publisher publishing persistent messages
import os
import platform
import time
import threading

from solace.messaging.messaging_service import MessagingService, ReconnectionListener, ReconnectionAttemptListener, ServiceInterruptionListener, RetryStrategy, ServiceEvent
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisher
from solace.messaging.publisher.persistent_message_publisher import MessagePublishReceiptListener
from solace.messaging.resources.topic import Topic

if platform.uname().system == 'Windows': os.environ["PYTHONUNBUFFERED"] = "1" # Disable stdout buffer 

lock = threading.Lock() # lock object that is not owned by any thread. Used for synchronization and counting the 

TOPIC_PREFIX = "solace/samples/python"

# Inner classes for error handling
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

class MessageReceiptListener(MessagePublishReceiptListener):
    def __init__(self):
        self._receipt_count = 0

    @property
    def receipt_count(self):
        return self._receipt_count

    def on_publish_receipt(self, publish_receipt: 'PublishReceipt'):
        with lock:
            self._receipt_count += 1
            print(f"\npublish_receipt:\n {self.receipt_count}\n")

class GuaranteedPublisher:
    def __init__(self, messaging_service):
        self.messaging_service = messaging_service
        self.create_guaranteed_pusblisher()
    def __del__(self):
        self.guranteed_publisher.terminate()
    def create_guaranteed_pusblisher(self):
        self.guranteed_publisher : PersistentMessagePublisher = self.messaging_service.create_persistent_message_publisher_builder().build()
        self.guranteed_publisher.start()
        self.receipt_listener = MessageReceiptListener()
        self.guranteed_publisher.set_message_publish_receipt_listener(self.receipt_listener)

    def send_guaranteed_message(self, message_body, topic_str):
        topic = Topic.of(topic_str)

        outbound_msg_builder = self.messaging_service.message_builder()
        
        outbound_msg = outbound_msg_builder .build(message_body)
        
        self.guranteed_publisher.publish(outbound_msg, topic)
        print(f'PERSISTENT publish message is successful... Topic: [{topic.get_name()}]')




