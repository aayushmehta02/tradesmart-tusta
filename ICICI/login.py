# login.py
import urllib.parse

import pyotp
from breeze_connect import BreezeConnect  # type:ignore
from NorenRestApiPy.NorenApi import NorenApi


class ICICILogin:
    def __init__(self, tusta_user_id):
        
        self.tusta_user_id = tusta_user_id
       
    def icici_handle_login(self):
        try:
            user_broker_details = UserBrokerDetails.getUserBrokerDetailsByUserIdAndBroker(
                self.tusta_user_id , "ICICI" #here user_id is tusta user_id
            )
            api_key = user_broker_details.get('app_key')
            api_secret = user_broker_details.get('api_secret')
            client_id = user_broker_details.get('client_id')
            if not all([
                api_key, #for icici only get client id
                api_secret, #for icici only get api secret
                client_id #for icici only get api key
            ]): 
                raise BrokerError("Missing required ICICI credentials")

            # Initialize ICICI client and login
            breeze = BreezeConnect(api_key=api_key)
            breeze.generate_session(api_secret=api_secret, session_token=client_id)
            user_name = breeze.get_customer_details(client_id).get('Success').get('idirect_user_name')
            user_id = breeze.get_customer_details(client_id).get('Success').get('idirect_userid')



            UserManager.save_active_user({
            "broker": "ICICI",
            "access_token": self.api_session,
            "api_key": self.api_key,
            "refresh_token": None,
            "secret_key": self.api_secret,
            "clientCode": user_id,
            "name": user_name,
            "uid": tusta_user_id,
            "feed_token": None,
            "password": user_broker_details['password'],
            "yob": user_broker_details['yob']
        })

            return redirect(APP_REDIRECT_URL)
        except Exception as e:
            print(f"Error: {e}")
            return False


           
