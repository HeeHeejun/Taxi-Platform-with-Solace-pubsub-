import os
import time
import threading
from datetime import datetime
import event_broker as e

class User:
    def __init__(self, userID, location):
        self.ID = userID
        self.location = location
        self.event_broker = e.Event_broker()
        self.setting()
        
    def __del__(self):
        del self.event_broker
    
    def setting(self):
        self.event_broker.set_guranteed_publisher()
        self.event_broker.set_guranteed_receiver('Q.HJ.user')
    
    def set_location(self, new_location):
        self.location = new_location
        
    def send_taxi_request_receive_ack(self, Destination):
        topic_str  ='HJ/taxiPlatform/RideRequest/' + self.ID
        date = datetime.now()
        time = date.strftime('%Y-%m-%d-%H:%M:%S')
        msg =  time + ' ' + self.ID + ' ' + self.location + ' ' + Destination
        self.event_broker.send_msg_guranteed(topic_str, msg)
        print('---------------------------------')
        print('User : User Ride Request send')
        print('---------------------------------')
        print(f'Time: {time}')
        print(f'UserID: {self.ID}')
        print(f'Current_location: {self.location}')
        print(f'Destination: {Destination}')
        print('---------------------------------')
        
        read_request_thread_func = threading.Thread(target=self.wait_receive_ack)
        read_request_thread_func.start()
        
    def wait_receive_ack(self):
        while(1):
            topic, payload = self.event_broker.read_msg_guranteed()
            if topic == None:
                continue
            
            split_topic = topic.split('/')
            if split_topic[2] == 'RideRequestResponse':                
                msg = payload.split(' ')
                print('---------------------------------')
                print('User : User Ride Request receive')
                print('---------------------------------')
                print(f'Time: {msg[0]}')
                print(f'UserID: {msg[1]}')
                print(f'Driver_location: {msg[2]}')
                print(f'Destination: {msg[3]}')
                print('---------------------------------')
                
            elif split_topic[2] == 'PaymentRequest':
                msg = payload.split(' ')
                print('---------------------------------')
                print('User : User PaymentRequest receive')
                print('---------------------------------')
                print(f'Time: {msg[0]}')
                print(f'UserID: {msg[1]}')
                print(f'Cost: {msg[2]}')
                print('---------------------------------')
                
                print('\n user pay the fee!!!!!!!!')


def main():
    pass

if __name__ == "__main__":    
    main()
