import logging
import os
import time
import uuid

import pandas as pd
import pyotp  # type: ignore
from breeze_connect import BreezeConnect  # type: ignore

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def load_combined_instruments(file_path: str) -> pd.DataFrame:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]
    return df

class ICICI_Broker:
    instrument_df: pd.DataFrame = None

    def __init__(self, api_key: str, api_secret: str, api_session: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_session = api_session
        self.obj = BreezeConnect(api_key=self.api_key)
        self.obj.generate_session(api_secret=self.api_secret, session_token=self.api_session)

    def initialize_data(self):
        ICICI_Broker.instrument_df = load_combined_instruments(
            r"C:\Users\aayus\OneDrive\Desktop\fyers\combined_instrument_data.csv"
        )
        logging.info("Instrument data loaded successfully.")

    def get_broker_obj(self):
        return self.obj

    def get_funds(self):
        try:
            response = self.obj.get_funds()
            bank_balance = response.get('Success', {}).get('total_bank_balance', 0)
            return bank_balance
        except Exception as e:
            print(f"Failed to fetch funds: {e}")
            return 0

    def get_ltp(self, exchange_code, token):
        right, row= self.filter_csv_by_token(token)
        print(right, row.get('Series').lower())
        if row.get('Series').lower() == "option":
            product_type = "options"
        elif row.get('Series').lower() == "future":
            product_type = "futures"
        else:
            product_type = "cash"
    
        print(product_type)
        if exchange_code in ["BSESEN", "BANKEX"]:
          exchange_code = "BFO"
        elif exchange_code == "NSE":
            exchange_code = "NSE"
        elif exchange_code == "BSE":
            exchange_code = "BSE"
        else:
            exchange_code = "NFO"

        print(exchange_code)
        expiry_raw = row.get("ExpiryDate")
        expiry_date = ""

        if pd.notnull(expiry_raw) and str(expiry_raw).strip() != "":
            expiry_dt = pd.to_datetime(expiry_raw, errors='coerce')
            if pd.notnull(expiry_dt):
                expiry_date = expiry_dt.strftime('%Y-%m-%dT06:00:00.000Z')
        print(expiry_date)
        strike_price = row.get("StrikePrice")
        if pd.isna(strike_price):
            strike_price = "0"
        else:
            strike_price = str(int(float(strike_price))) if float(strike_price).is_integer() else str(strike_price)

        print("DEBUG PARAMS -->",
      row.get("ShortName"),
      exchange_code,
      expiry_date,
      product_type,
      right,
      strike_price)

        try:
            response = self.obj.get_quotes(
            stock_code=row.get("ShortName"),
            exchange_code=exchange_code,
            expiry_date=expiry_date,
            product_type=product_type,
            right=right,
            strike_price=strike_price
        )


            print(response)

            
            success_list = response.get("Success", [])
            for item in success_list:
                if item.get("exchange_code") == exchange_code:
                    return item.get("ltp", 0)
        except Exception as e:
            logging.error(f"Error fetching LTP: {e}")
            return 0

    def fetch_instruments(self, exch_seg):
        return self.obj.instruments(exch_seg)

    def cancel_order_on_broker(self, order_id):
        response = self.obj.cancel_order(order_id)
        if response.get('Success'):
            logging.info(f"Order {order_id} cancelled successfully.")
        else:
            logging.warning(f"Failed to cancel order {order_id}: {response.get('message')}")

    @classmethod
    def filter_by_expiry(cls, df, expiry='W'):
        df = df.copy()
        df['ExpiryDate'] = pd.to_datetime(df['ExpiryDate'], errors='coerce')
        df = df.dropna(subset=['ExpiryDate'])

        monthly_df = df.loc[df.groupby(df['ExpiryDate'].dt.to_period('M'))['ExpiryDate'].idxmax()]

        if expiry == 'W':
            return df.iloc[0] if len(df) > 0 else None
        elif expiry == 'NW':
            return df.iloc[1] if len(df) > 1 else None
        elif expiry == 'M':
            return monthly_df.iloc[0] if len(monthly_df) > 0 else None
        elif expiry == 'NM':
            return monthly_df.iloc[1] if len(monthly_df) > 1 else None
        elif expiry == 'NNM':
            return monthly_df.iloc[2] if len(monthly_df) > 2 else None
        return None

    @classmethod
    def filter_fno_instruments(cls, df, exch_seg, symbol, strike_price=None, ce_pe=None, instrumenttype=None):
        if instrumenttype == 'FUTIDX':
            return df[
                (df['ExAllowed'] == exch_seg) &
                (df['ShortName'] == symbol) &
                (df['Series'] == 'FUTURE')
            ]
        else:
            if ce_pe == "put":
                ce_pe = "PE"
            else:
                ce_pe = "CE"
            print(symbol, exch_seg, strike_price, ce_pe)
            if symbol == "SENSEX":
                symbol = "BSESEN"
            return df[
                (df['ShortName'] == symbol) &
                (df['ExAllowed'] == exch_seg) &
                (df['StrikePrice'] == strike_price) &
                (df['Series'] == 'OPTION') &
                (df['OptionType'] == ce_pe)
            ]
            

    @classmethod
    def get_icici_token_details(cls, exch_seg, symbol, strike_price=None, is_pe=None, expiry='W', instrumenttype=None):
        ce_pe = "put" if is_pe else "call"
        symbol = symbol.upper()
        df = cls.instrument_df.copy()
        symbol_map = {'NIFTY': 'NIFTY', 'BANKNIFTY': 'CNXBAN', 'FINNIFTY': "NIFFIN"}

        if exch_seg in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD']:
            print("token inputs:",exch_seg, symbol_map.get(symbol, symbol), strike_price, ce_pe, instrumenttype)
            df_filtered = cls.filter_fno_instruments(
                df, exch_seg, symbol_map.get(symbol, symbol), strike_price, ce_pe, instrumenttype
            )
            print(df_filtered)
            if df_filtered is None or df_filtered.empty:
                logging.warning(f"No token found for {symbol} {strike_price}{ce_pe} in {exch_seg}")
                return None, None, None
            token_info = cls.filter_by_expiry(df_filtered, expiry)
        else:
            df_filtered = df[(df['Series'] == 'EQ') &
                           (df['ShortName'] == symbol)    ]
            if df_filtered.empty:
                logging.warning(f"No token found for {symbol} in {exch_seg}")
                return None, None, None
            token_info = df_filtered.iloc[0]
        
        if token_info is not None:
            return token_info.get('Token'), token_info.get('ShortName'), token_info.get('LotSize')
        else:
            return None, None, None

    def filter_csv_by_token(self, token):
        try:
            # Convert token to string and create a copy of the dataframe
            token = str(token)
            df = self.instrument_df.copy()
            df['Token'] = df['Token'].astype(str)
            
            # Filter the DataFrame for the specific token
            filtered_df = df[df['Token'] == token]
            
            if filtered_df.empty:
                logging.warning(f"No data found for token {token}")
                return 'others'
            
            # Get the first row that matches
            row = filtered_df.iloc[0]
            
            # Check Series value
            series = row.get('Series', '')
            if pd.isna(series):
                return 'cash',  row
                
            
            if series == 'FUTURE':
                return 'others', row
                
            elif series == 'OPTION':
                option_type = row.get('OptionType', '')
                if pd.isna(option_type):
                    return 'others', row
                    
                
                if option_type == 'CE':
                    return 'call', row
                    
                elif option_type == 'PE':
                    return 'put', row
                    
            elif series == 'EQ':
                return 'others', row
                
            
            
        except Exception as e:
            logging.error(f"Error in filter_csv_by_token: {str(e)}")
            return 'others'

    def place_order_on_broker(self, symbol_token, symbol, qty, exchange_code, buy_sell, order_type, price,  is_paper=False,
                            is_overnight=False):
        try:
            product = 'I'  # Intraday default
            right = ''
            if exchange_code in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD'] and is_overnight:
                product = 'M'  # Margin for derivatives
            elif is_overnight:
                product = 'cash'  # Carryforward for cash
                
            order_params = {
                "tradingsymbol": symbol,
                "exch": exchange_code,
                "transaction_type": buy_sell,
                "quantity": qty,
                "order_type": order_type,
                "price": price if order_type == "LIMIT" else 0,
                "product": product,
                "validity": "DAY"
            }

            order_id = None
            average_price = 0
            right, row = self.filter_csv_by_token(symbol_token)
            if right == 'others' and exchange_code in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD']:
                product = "futures"
            elif right == 'call' or right == 'put':
                product = "options"
            else:
                product = "cash"
            # Expiry and strike for derivatives, else empty
            expiry_date = (row.get("ExpiryDate") + "T06:00:00.000Z") if product in ["futures", "options"] else ""
            strike_price_raw = row.get("StrikePrice", 0)
            strike_price = str(int(float(strike_price_raw))) if product == "options" else "0"
            symbol_map = {'NIFTY': 'NIFTY', 'BANKNIFTY': 'CNXBAN', 'FINNIFTY': "NIFFIN"}
            symbol = symbol_map.get(symbol, symbol)
            
            print("DEBUG ORDER PARAMS:",
      f"stock_code={symbol}",
      f"exchange_code={exchange_code}",
      f"product={product}",
      f"expiry_date={expiry_date}",
      f"strike_price={strike_price}({type(strike_price)})",
      f"right={right}")

            

            # Prepare order params
            if not is_paper:
                print("Order Params:", {
    "stock_code": symbol,
    "exchange_code": exchange_code,
    "product": product,
    "action": buy_sell.lower(),
    "order_type": order_type.lower(),
    "stoploss": "0",
    "quantity": str(qty),
    "price": str(price) if order_type.lower() == "limit" else "0",
    "validity": "day",
    "validity_date": "",
    "disclosed_quantity": "0",
    "expiry_date": expiry_date or "",
    "right": right,
    "strike_price": strike_price or "0",
})

                response=self.obj.place_order(
                    stock_code= symbol,
                    exchange_code= exchange_code,
                    product=product,
                    action=buy_sell.lower(),
                    order_type= order_type.lower(),
                    stoploss="0",
                    quantity= str(qty),
                    price=str(price) if order_type.lower() == "limit" else "0",
                    validity= "day",
                    validity_date="",
                    disclosed_quantity ="0",
                    expiry_date=expiry_date or "",
                    right= right,
                    strike_price= strike_price or "0",
                    # "user_remark": f"{symbol} {product}"
                )
                print(response)
                  
                if not response or response.get('Success') == 'None':
                            error = response.get('emsg', 'Order placement failed')
                            print(f"Order placement failed: {error}")
                            return None, None, f"Order placement failed: {response.get('emsg', 'Unknown error')}"

                order_id = response.get('order_id')
                if not order_id and response.get('Error') == 'Insufficient limit  :Allocate funds to increase your limit. Available Limits :0.00':
                            return None, None, "Order placement failed: Insufficient balance"
                else:
                    

                    # Poll status
                    average_price, status, error_message = self.handle_order_status(order_id)
                
                    if not average_price:
                        return None, None, "Order placement failed: No order number received"
 

            else:
                # Paper trading logic
                order_id = 'Paper' + str(uuid.uuid4())
                print(f"Paper trade created with ID: {order_id}")

            # Fallback to LTP if no avg price available
            if average_price == 0 or 'average_price' not in locals():
                average_price = self.get_ltp( exchange_code,symbol_token)
            order_params['ltp'] = str(average_price)
            order_params['transaction_type'] = buy_sell
            order_params['tradingsymbol'] = str(symbol)
            return order_id, order_params, None

        except Exception as e:
            print(f"Order placement failed: {str(e)}")
            return None, 0

    def fetch_order_status(self, order_id, retries=3, delay=0.5):
        """Attempts to fetch the order status with retries for ICICI."""
        try:
            for _ in range(retries):
                time.sleep(delay)
                # Get order history from ICICI API
                orders = self.obj.get_order_list()
                if not orders or 'Success' not in orders:
                    continue
                    
                # Find the specific order
                for order in orders['Success']:
                    if order.get('order_id') == order_id:
                        status = order.get('order_status', '')
                        print(f"Order Status: {status}")
                        # ICICI status mapping: COMPLETE -> 'Completed', REJECTED -> 'Rejected'
                        if status in ('Completed', 'Rejected'):
                            return order
            return None
        except Exception as e:
            logging.error(f"Error fetching order status: {e}")
            return None

    def handle_order_status(self, order_id):
        """Handles order status checking and response for ICICI."""
        try:
            latest_order = self.fetch_order_status(order_id)
            if not latest_order:
                self.obj.cancel_order(order_id)
                return None, None, "Failed to fetch order status during polling"

            status = latest_order.get('order_status', '')
            if status == 'Completed':
                return latest_order.get('average_price', 0), status, None
            if status == 'Rejected':
                return self.handle_rejection(latest_order)

            # Cancel order if still open
            self.obj.cancel_order(order_id)
            return None, None, "Order was canceled due to timeout."
        except Exception as e:
            logging.error(f"Error handling order status: {e}")
            return None, None, f"Error handling order status: {str(e)}"

    def handle_rejection(self, order):
        """Handles rejected orders for ICICI by returning appropriate error messages."""
        try:
            # ICICI specific rejection reason field
            rejection_reason = order.get('rejection_reason', 'Unknown reason')
            print(f"Order rejected: {rejection_reason}")
            
            error_message = ("Order placement failed due to insufficient funds."
                            if "insufficient" in rejection_reason.lower()
                            else f"Order placement failed: {rejection_reason}")
            return None, None, error_message
        except Exception as e:
            logging.error(f"Error handling rejection: {e}")
            return None, None, "Error handling order rejection"

# Optional test usage
if __name__ == "__main__":
    creds = {
        "api_key": "677(02S7Re9a3&67k7N5#dI94!O494^0",
        "api_secret": "060C002y9Q3p2Y37243860734k2X2H32",
        "api_session": "51233345"
    }
    broker = ICICI_Broker(**creds)
    broker.initialize_data()
    #print(broker.get_funds())
    #print(broker.filter_csv_by_token(broker.instrument_df,'1660'))
    #print(broker.place_order_on_broker('1660', "ITC", 1, 'NSE', 'buy', "limit", 450, is_paper=False))
    #print(3, broker.get_icici_token_details('NFO', 'NIFTY', '24000', '1', 'NW', 'FUTIDX'))
    #print(broker.place_order_on_broker('', 'BANKNIFTY', 100, 'NFO', 'buy', 'LIMIT', 50000, is_paper=False))
    #print(19, broker.get_icici_token_details('BFO', 'BANKEX', '26000', '1', 'NM', 'FUTIDX'))
    #print(broker.place_order_on_broker(861616, 'BSESEN', 20, 'BFO', 'BUY', 'LIMIT', 0, True, False))
   


    #print(broker.get_ltp('BANKNIFTY', 55980))
    # print(broker.place_order_on_broker(54452, 'NIFTY', 1, 'NFO', 'BUY', 'MARKET', 0, False, False))
    # print(broker.place_order_on_broker('55980', 'BANKNIFTY', 1, 'NFO', 'BUY', 'LIMIT', 50000, is_paper=False))
    print("\nCash Order Test (NSE, TCS):")
    token, symbol, lot_size = broker.get_icici_token_details('NSE', 'TCS')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(broker.place_order_on_broker(token, symbol, 1, 'NSE', "BUY", "MARKET", 0, is_paper=False))

    print("\nFNO Order Test (NFO, NIFTY, 28500 PE, Monthly):")
    token, symbol, lot_size = broker.get_icici_token_details('NFO', 'NIFTY', 28500, 1, 'M', 'OPTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(broker.place_order_on_broker(token, symbol, 75, 'NFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nFutures Order Test (NFO, NIFTY, Monthly FUTIDX):")
    token, symbol, lot_size = broker.get_icici_token_details('NFO', 'NIFTY', expiry='M', instrumenttype='FUTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(broker.place_order_on_broker(token, symbol, 75, 'NFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nSensex Option (BFO, SENSEX, 77400 CE, Weekly):")
    token, symbol, lot_size = broker.get_icici_token_details('BFO', 'SENSEX', 77400, 0, 'W', 'OPTIDX')
    print("Token:", token, "| Symbol:", symbol, "| Lot size:", lot_size)
    print(broker.place_order_on_broker(token, symbol, 20, 'BFO', "BUY", "MARKET", 0, is_paper=False))

    print("\nLTP Examples:")
    print("NSE:", broker.get_ltp('NSE', 54452))  # Last token from above
    print("NFO:", broker.get_ltp('NFO', 130741))  # Same
    print(broker.get_funds())
