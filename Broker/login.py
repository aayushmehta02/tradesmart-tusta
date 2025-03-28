# login.py
import pyotp  # type: ignore
from NorenRestApiPy.NorenApi import NorenApi  # type: ignore


class TradeSmartLogin(NorenApi):
    def __init__(self, user, pwd, factor2, vc, app_key, imei):
        super().__init__(
            host='https://v2api.tradesmartonline.in/NorenWClientTP/',
            websocket='wss://v2api.tradesmartonline.in/NorenWSTP/'
        )
        self.login_to_broker(user, pwd, factor2, vc, app_key, imei)

    def login_to_broker(self, user, pwd, factor2, vc, app_key, imei):
        totp = pyotp.TOTP(factor2)
        otp = totp.now()

        ret = self.login(
            userid=user,
            password=pwd,
            twoFA=otp,
            vendor_code=vc,
            api_secret=app_key,
            imei=imei
        )
        if ret is None or ret.get("stat") != "Ok":
            raise Exception(f"Login failed: {ret}")
        print("Login successful")
