import Driver as dr
import event_broker as eb
import User as us
import Taxi_Platform as tp
import payment as pm 
import company as cp
def main():
    drvier = dr.Driver('driver1', 'AjouUniversity')
    user = us.User('user1', 'area2')
    taxi_platform = tp.Taxi_platform()
    payment = pm.Payment()
    company = cp.Company()
    
    print('\nUser send taxi request\n')
    user.send_taxi_request_receive_ack('area3')
    
    print('\nDriver moving~~\n')
    input()
    
    print('\npick up!!\n')
    drvier.pickup_complete()
    
    print('\ntaxi is going to Destination~~~~~~~~~\n')
    input()
    
    print('\ndrop off the customer!!!!!\n')
    drvier.drop_off_complte()
    input()
    
    
if __name__ == "__main__":
    main()