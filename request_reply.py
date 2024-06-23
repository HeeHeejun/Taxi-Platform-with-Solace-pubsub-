"""sampler for request reply message publishing and receiving"""
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


class Request_reply:
    def __init__(self, messaging_service):
        self.messaging_service = messaging_service
        self.reply_timeout = 10000
        
    def __del__(self):
        pass
    
    def send_msg_and_response_async(self, topic_str, msg):
        topic = Topic.of(topic_str)
        topic_subscription = TopicSubscription.of(topic_str)
        return Request_reply.publish_request_and_process_response_message_async(service=self.messaging_service,
                                                request_destination=topic,
                                                for_requests=topic_subscription,
                                                reply_timeout=self.reply_timeout,
                                                msg= msg)
    
    def receive_msg_and_response_async(self, topic_str, msg):
        topic_subscription = TopicSubscription.of(topic_str)
        return Request_reply.receive_request_and_send_response_message(self.messaging_service, topic_subscription, msg) 
        
    @staticmethod
    def publish_request_and_process_response_message_async(service: MessagingService, request_destination: Topic,
                                                           reply_timeout: int, msg = 'ping'):
        """Mimics microservice that performs a async request
        Args:
            service: connected messaging service
            request_destination: where to send a request (it is same for requests and responses)
            reply_timeout: the reply timeout
        """

        requester: RequestReplyMessagePublisher = service.request_reply() \
            .create_request_reply_message_publisher_builder().build().start()

        ping_message = service.message_builder().build(payload=msg,
                                                       additional_message_properties={SEQUENCE_NUMBER: 123})

        publish_request_async = requester.publish(request_message=ping_message,
                                                  request_destination=request_destination,
                                                  reply_timeout=reply_timeout)
        # we can get the reply from the future
        r = publish_request_async.result()
        print(r)
        
        return r

    @staticmethod
    def publish_request_and_process_response_message_blocking(service: MessagingService, request_destination: Topic,
                                                              reply_timeout: int):
        """Mimics microservice that performs a blocking request

        Args:
            service: connected messaging service
            request_destination: where to send a request (it is same for requests and responses)
            reply_timeout: the reply timeout
        """
        requester: RequestReplyMessagePublisher = service.request_reply() \
            .create_request_reply_message_publisher_builder().build().start()

        ping_message: OutboundMessage = service.message_builder().build(payload='Ping')
        try:

            reply = requester.publish_await_response(request_message=ping_message,
                                                     request_destination=request_destination,
                                                     reply_timeout=reply_timeout)
            print(f"reply: {reply}")
        except TimeoutError as e:
            print(e)

    @staticmethod
    def receive_request_and_send_response_message(service: MessagingService, for_requests: TopicSubscription, r_msg = "pong"):
        """Mimics microservice that performs a response

        Args:
            service: connected messaging service
            for_requests: where to expect requests
        """

        request_receiver: RequestReplyMessageReceiver = service.request_reply() \
            .create_request_reply_message_receiver_builder().build(for_requests).start()

        msg, replier = request_receiver.receive_message(timeout=5000)
        if replier is not None:
            outbound_msg = service.message_builder().build(r_msg)
            replier.reply(outbound_msg)
        
        return msg

    @staticmethod
    def async_request_and_response(service: MessagingService, request_destination: Topic,
                                   for_requests: TopicSubscription, reply_timeout: int):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as e:
            r_f = e.submit(Request_reply.receive_request_and_send_response_message,
                           service=service,
                           for_requests=for_requests)
            p_f = e.submit(Request_reply.publish_request_and_process_response_message_async,
                           service=service,
                           request_destination=request_destination,
                           reply_timeout=reply_timeout)
            r_f.result()
            p_f.result()

    @staticmethod
    def blocking_request_and_response(service: MessagingService, request_destination: Topic,
                                      for_requests: TopicSubscription, reply_timeout: int):

        with ThreadPoolExecutor(max_workers=2) as e:
            r_f = e.submit(Request_reply.receive_request_and_send_response_message,
                           service=service,
                           for_requests=for_requests)
            p_f = e.submit(Request_reply.publish_request_and_process_response_message_blocking,
                           service=service,
                           request_destination=request_destination,
                           reply_timeout=reply_timeout)
            r_f.result()
            p_f.result()