# login.py
import urllib.parse

import pyotp
from NorenRestApiPy.NorenApi import NorenApi
from breeze_connect import BreezeConnect #type:ignore

class ICICILogin:
    def __init__(self, api_key, api_secret, api_session, tusta_user_id):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_session = api_session
        self.tusta_user_id = tusta_user_id
       
    def icici_handle_login(self):
        try:
            user_broker_details = UserBrokerDetails.getUserBrokerDetailsByUserIdAndBroker(
                tusta_user_id , "ICICI" #here user_id is tusta user_id
            )
            if not all([
            user_broker_details.get('client_id')#for icici only get client id
            user_broker_details.get('api_secret')#for icici only get api secret
            user_broker_details.get('app_key')#for icici only get api key
            ]): 
                raise BrokerError("Missing required ICICI credentials")

            # Initialize ICICI client and login
            breeze = BreezeConnect(api_key=self.api_key)
            breeze.generate_session(api_secret=self.api_secret, session_token=self.api_session)
            user_name = breeze.get_customer_details(self.api_session).get('Success').get('idirect_user_name')
            user_id = breeze.get_customer_details(self.api_session).get('Success').get('idirect_userid')



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


           
