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
            r"C:\Users\aayus\OneDrive\Desktop\finance-browser\combined_instrument_data_icici.csv"
        )
        logging.info("Instrument data loaded successfully.")

    def get_broker_obj(self):
        return self.obj

    def get_funds(self):
        try:
            response = self.obj.get_funds()
            print(response)
            return response.get("Success", {})[0].get("total_bank_balance", 0)
        except Exception as e:
            logging.error(f"Failed to fetch funds: {e}")
            return 0

    def get_ltp(self, stock_code, exchange_code, product_type, right=None, strike_price=None, expiry_date=None):
        try:
            response = self.obj.get_quotes(
                stock_code=stock_code,
                exchange_code=exchange_code,
                expiry_date=expiry_date,
                product_type=product_type,
                right=right,
                strike_price=strike_price
            )
            return response.get("Success", {}).get("ltp", 0)
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
            df_filtered = cls.filter_fno_instruments(
                df, exch_seg, symbol_map.get(symbol, symbol), strike_price, ce_pe, instrumenttype
            )
            if df_filtered is None or df_filtered.empty:
                logging.warning(f"No token found for {symbol} {strike_price}{ce_pe} in {exch_seg}")
                return None, None, None
            token_info = cls.filter_by_expiry(df_filtered, expiry)
        else:
            df_filtered = df[(df['Series'] == 'EQ') &
                           (df['ShortName'] == symbol) & 
                           (df['ExAllowed'] == exch_seg)]
            if df_filtered.empty:
                logging.warning(f"No token found for {symbol} in {exch_seg}")
                return None, None, None
            token_info = df_filtered.iloc[0]
        
        if token_info is not None:
            return token_info.get('Token'), token_info.get('ShortName'), token_info.get('LotSize')
        else:
            return None, None, None

    def filter_csv_by_token(self, df, token):
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
                return 'others'
            
            if series == 'FUTURE':
                return 'others'
            elif series == 'OPTION':
                option_type = row.get('OptionType', '')
                if pd.isna(option_type):
                    return 'others'
                
                if option_type == 'CE':
                    return 'call'
                elif option_type == 'PE':
                    return 'put'
            elif series == 'EQ':
                return 'cash'
            
            
        except Exception as e:
            logging.error(f"Error in filter_csv_by_token: {str(e)}")
            return 'others'

    def place_order_on_broker(self, symbol_token, symbol, qty, exchange_code, buy_sell, order_type, price, expiry, is_paper=False,
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
            right = self.filter_csv_by_token(self.instrument_df,symbol_token)
            if right == 'others':
                product = "futures"
            elif right == 'call' or right == 'put':
                product = "options"
            else:
                product = "cash"
            if not is_paper:
                response = self.obj.place_order(
                    stock_code=symbol,
                    action=buy_sell,
                    product=product,
                    exchange_code=exchange_code,
                    quantity=qty,
                    discloseqty=0,
                    order_type=order_type,
                    price=price,
                    right=right,
                   
                    validity='day',
                    remarks='TUSTA'
                )
                print(response)
                if not response or response.get('stat') != 'Ok':
                    error = response.get('emsg', 'Order placement failed')
                    print(f"Order placement failed: {error}")
                    return None, None, f"Order placement failed: {response.get('emsg', 'Unknown error')}"

                order_id = response.get('order_id')
                if not order_id:
                    print("Order placement failed: No order number received")
                    return None, None, "Order placement failed: No order number received"

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
                average_price = self.get_ltp(symbol_token, exchange_code, 'options', right, price, expiry)
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
        "api_session": "51050135"
    }

    broker = ICICI_Broker(**creds)
    broker.initialize_data()
    # print(broker.get_funds())
    print(broker.filter_csv_by_token(broker.instrument_df,'2398'))
    print(broker.place_order_on_broker('131604', 'TCS', 100, 'NFO', 'BUY', 'LIMIT', 50000, is_paper=False))
    # print(broker.place_order_on_broker('NIFTY', 'BANKNIFTY', 100, 'NFO', 'buy', 'LIMIT', 50000, is_paper=False))