# login.py
import pyotp  # type: ignore
from NorenRestApiPy.NorenApi import NorenApi  # type: ignore


class TradeSmartLogin(NorenApi):
    def __init__(self):
        super().__init__(
            host='https://v2api.tradesmartonline.in/NorenWClientTP/',
            websocket='wss://v2api.tradesmartonline.in/NorenWSTP/'
        )
       
    def login_to_broker(self, user, pwd, factor2, vc, app_key, imei):
        UserBrokerDetails.getUserBrokerDetailsByUserIdAndBroker(
            user_id, "Trade Smart"
        )

          if not all([
            user_broker_details.get('password'),
            user_broker_details.get('factor2'),
            user_broker_details.get('client_id')
        ]):

        totp = pyotp.TOTP(factor2)
        otp = totp.now()

        login_data = self.login(
            userid=user,
            password=pwd,
            twoFA=totp,
            vendor_code=vc,
            api_secret=app_key,
            imei=imei
        )

        login_data = json.loads(login_response)
        
        session_token = login_data.get('sessionToken')

        if not session_token:
            raise BrokerError("Failed to obtain Samco session token")

        # Set session token
        tradesmart.set_session_token(sessionToken=session_token)

        UserManager.save_active_user({
            "broker": "Trade Smart",
            "access_token": session_token,
            "api_key": None,
            "refresh_token": None,
            "secret_key": None,
            "clientCode": login_data['accountID'],
            "name": login_data['accountName'],
            "uid": user_id,
            "feed_token": None,
            "password": user_broker_details['password'],
            "factor2" : user_broker_details['factor2']
           
        })

        return redirect(APP_REDIRECT_URL)
