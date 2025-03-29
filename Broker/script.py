# script.py
import io
import logging
import os
import time
import uuid
import zipfile

import pandas as pd
import requests
from login import TradeSmartLogin

DATA_FOLDER = r"C:\Users\aayus\OneDrive\Desktop\finance-browser\Broker\files"
COMBINED_FILE = os.path.join(DATA_FOLDER, "combined_instruments_2.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Ensure data folder exists
os.makedirs(DATA_FOLDER, exist_ok=True)

def load_combined_instruments(file_path: str) -> pd.DataFrame:
    """
    Load the combined_instruments.csv file into a DataFrame.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    df = pd.read_csv(file_path)
    
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]
    return df

class TradeSmart(TradeSmartLogin):
    exchange_data: pd.DataFrame = None

    def initialize_data(self):
        # exchange_data: pd.DataFrame = None
        TradeSmart.exchange_data = load_combined_instruments(r"C:\Users\aayus\OneDrive\Desktop\finance-browser\combined_instruments.csv")
        
        print("Instrument data loaded successfully")

    def get_funds_available(self):
        funds = self.get_limits()
        return funds if funds and funds.get("stat") == "Ok" else "Failed to fetch funds"
    def get_ltp(self, exchange, searchtext):
        try:
            df = self.exchange_data[self.exchange_data['Exchange'] == exchange]

            if df is None or df.empty:
                print(f"No data available for exchange {exchange}")
                return 0

            searchtext = searchtext.upper()
            print(f"Searching for {searchtext} in {exchange}")

            # First try exact match
            result = df[df['TradingSymbol'] == searchtext]
            if result.empty:
                # If no exact match, try partial match
                result = df[df['TradingSymbol'].str.contains(searchtext, case=False, na=False)]

            print("Found matches:", result['TradingSymbol'].tolist() if not result.empty else "None")

            if not result.empty:
                # Get token directly from the DataFrame
                token = str(result['Token'].iloc[0])
                trading_symbol = result['TradingSymbol'].iloc[0]
                print(f"Using token {token} for {trading_symbol}")

                get_ltp = self.get_quotes(exchange, token)
                if get_ltp and 'lp' in get_ltp:
                    ltp = float(get_ltp.get('lp', 0))
                    print(f"LTP found: {ltp}")
                    return ltp
                else:
                    print("No LTP found in quotes response")
                    return 0

            print(f"No matches found for '{searchtext}' in {exchange}")
            return 0

        except Exception as e:
            print(f"Error in get_ltp: {str(e)}")
            return 0

    def cancel_order_on_broker(self, order_id):
        response = self.cancel_order(orderno=order_id)
        return f"Order {order_id} cancelled successfully" if response and response.get("stat") == "Ok" else f"Failed to cancel order {order_id}"

    @classmethod
    def filter_by_expiry(cls, df, expiry='W'):
        df['expiry'] = pd.to_datetime(df['Expiry'], format='%d-%b-%Y', errors='coerce')
        df = df.dropna(subset=['expiry']).sort_values('expiry')
        monthly = df.groupby(df['expiry'].dt.to_period('M'))['expiry'].idxmax()
        monthly_df = df.loc[monthly].sort_values('expiry')

        if expiry == 'W': return df.iloc[0] if len(df) > 0 else None
        elif expiry == 'NW': return df.iloc[1] if len(df) > 1 else None
        elif expiry == 'M': return monthly_df.iloc[0] if len(monthly_df) > 0 else None
        elif expiry == 'NM': return monthly_df.iloc[1] if len(monthly_df) > 1 else None
        elif expiry == 'NNM': return monthly_df.iloc[2] if len(monthly_df) > 2 else None

    @classmethod
    def filter_fno_instruments(cls, df, exchange, symbol, strike_price=None, ce_pe=None, instrumenttype=None):
        if 'FUT' in (instrumenttype or ''):
            return df[
                (df['Exchange'] == exchange) &
                (df['TradingSymbol'].str.contains(symbol, case=False, na=False)) &
                (df['Instrument'] == 'FUTIDX')
            ]
        else:
            return df[
                (df['Exchange'] == exchange) &
                (df['TradingSymbol'].str.contains(symbol, case=False, na=False)) &
                (
                    (pd.to_numeric(df['StrikePrice'], errors='coerce') == float(strike_price)) |
                    (pd.to_numeric(df['Strike'], errors='coerce') == float(strike_price))
                ) &
                (df['OptionType'] == ce_pe)
            ]

    @classmethod
    def get_token_details(cls, exch_seg, symbol, strike_price=None, is_pe=None, expiry='W', instrumenttype=None):
        symbol = symbol.upper()
        ce_pe = "PE" if is_pe == "1" else "CE"
        df = cls.exchange_data[cls.exchange_data['Exchange'] == exch_seg]

        if exch_seg in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD']:
            df_filtered = cls.filter_fno_instruments(df, exch_seg, symbol, strike_price, ce_pe, instrumenttype)
            if df_filtered is None or df_filtered.empty: return None, None, None
            token_info = cls.filter_by_expiry(df_filtered, expiry)
            if token_info is not None:
                return token_info['Token'], token_info['TradingSymbol'], token_info['LotSize']
        else:
            df_filtered = df[df['TradingSymbol'].str.contains(symbol, case=False, na=False)]
            if df_filtered.empty: return None, None, None
            token_info = df_filtered.iloc[0]
            return token_info['Token'], token_info['TradingSymbol'], token_info['LotSize']

    def place_order_on_broker(self, symbol, qty, exchange, buy_sell, order_type, price, is_paper=False, is_overnight=False):
        try:
            product = 'I'
            if exchange in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD'] and is_overnight:
                product = 'M'
            elif is_overnight:
                product = "C"

            # Define order parameters
            order_params = {
                "tradingsymbol": symbol,
                "exch": exchange,
                "transaction_type": buy_sell,
                "quantity": qty,
                "order_type": order_type,
                "price": price if order_type == "LIMIT" else 0,
                "product": product,
                "validity": "DAY"
            }
            average_price = 0
            order_id = None

            if not is_paper:
                # Place order using the correct API method
                ret = self.place_order(
                    buy_or_sell=buy_sell,
                    product_type=product,
                    exchange=exchange,
                    tradingsymbol=symbol,
                    quantity=qty,
                    discloseqty=0,
                    price_type='MKT',
                    price=price,
                    trigger_price=0,
                    retention='DAY',
                    remarks='TUSTA'
                )

                if ret is None:
                    print("Order placement failed: API returned None")
                    return None, None, "Order placement failed: API returned None"

                if ret.get('stat') != 'Ok':
                    print(f"Order placement failed: {ret.get('emsg', 'Unknown error')}")
                    return None, None, f"Order placement failed: {ret.get('emsg', 'Unknown error')}"

                orderno = ret.get('norenordno')
                if not orderno:
                    print("Order placement failed: No order number received")
                    return None, None, "Order placement failed: No order number received"

                

                t=0
                while t <3:
                    time.sleep(0.5)
                    order_status = self.single_order_history(orderno)
                    if order_status:
                        break
                    else:
                        t+=1

                if order_status is None:
                    return None, None, "Failed to fetch order status during polling"
                latest_status = order_status[-1]
                

               

                
                print("Latest Order Status:", latest_status.get("status"))

                status = latest_status.get("status")
                print("Order Status:", status)
                

                if status == "COMPLETE":
                    holdings = self.get_holdings()
                    if holdings:
                        average_price = holdings[-1].get("upldprc", 0)
                        print(f"Average price: {average_price}")
                elif status == "REJECTED":
                    rejection_reason = order_status[-1].get('rejreason', 'Unknown reason')
                    print(f"Order rejected: {rejection_reason}")
                    if "Insufficient balance" in rejection_reason:
                        return None, None, "Order placement failed due to insufficient funds."
                    else:
                        return None, None, f"Order placement failed: {rejection_reason}"
                else:
                 
                    # Poll for order status
                    t=0
                while t <3:
                    time.sleep(0.5)
                    order_status = self.single_order_history(orderno)
                    if order_status:
                        break
                    else:
                        t+=1
                   
                    if status == 'COMPLETE':
                        average_price = order_status[-1].get('upldprc', 0)
                    else:
                       
                        
                        # Cancel the order if it is still open
                        cancel_result = self.cancel_order(orderno)
                        
                        return None, None, "Order was canceled due to timeout."

            else:
                order_id = 'Paper' + str(uuid.uuid4())
                print(f"Paper trade created with ID: {order_id}")
            # Get the last traded price if needed
            if 'average_price' not in locals() or average_price == 0:
                ltp_data = self.get_ltp(exchange, symbol)
                average_price = ltp_data
            print("average_price", ltp_data)
            order_params['ltp'] = str(average_price)
            # todo: format order_params
            order_params['transaction_type'] = buy_sell
            order_params['tradingsymbol'] = str(symbol)


            return order_id, order_params, None

        except Exception as e:
            print(f"Order placement failed: {str(e)}")
            return None, None, str(e)

# ---- Test usage ---- #
if __name__ == "__main__":
    creds = {
        "user": "",
        "pwd": "",
        "factor2": "",
        "vc": "",
        "app_key": "",
        "imei": ""
    }

    ts = TradeSmart(**creds)
    ts.initialize_data()
    # print("\nTesting BANKNIFTY weekly CE:")
    # print(ts.get_token_details('NFO', 'BANKNIFTY', '46000', is_pe=0, expiry='W', instrumenttype='OPTIDX'))
    print("get_ltp", ts.get_ltp('NSE', 'BANKNIFTY1-EQ'))
    print("\nTesting SENSEX monthly CE:")
    print(ts.get_token_details('BFO', 'SENSEX', '72000', is_pe=0, expiry='M', instrumenttype='OPTIDX'))
    print(ts.place_order_on_broker('BANKNIFTY24APR25C46000', 30, 'NFO', 'B', 'MARKET', 0, is_paper=True, is_overnight=False))