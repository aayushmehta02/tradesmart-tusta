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

    def __init__(self):
        # exchange_data: pd.DataFrame = None
        TradeSmart.exchange_data = load_combined_instruments(r"C:\Users\aayus\OneDrive\Desktop\finance-browser\combined_instruments.csv")
        self.nfo_df = self.exchange_data[self.exchange_data['Exchange'] == 'NFO']
        self.nse_df = self.exchange_data[self.exchange_data['Exchange'] == 'NSE']
        self.bse_df = self.exchange_data[self.exchange_data['Exchange'] == 'BSE']
        self.mcx_df = self.exchange_data[self.exchange_data['Exchange'] == 'MCX']
        self.bcd_df = self.exchange_data[self.exchange_data['Exchange'] == 'BCD']
        self.cds_df = self.exchange_data[self.exchange_data['Exchange'] == 'CDS']
        self.bfo_df = self.exchange_data[self.exchange_data['Exchange'] == 'BFO']
        print("Instrument data loaded successfully")

    def get_funds_available(self):
        funds = self.get_limits()
        return funds if funds and funds.get("stat") == "Ok" else "Failed to fetch funds"
    def get_ltp(self, exchange, searchtext):
        try:
            df = self.exchange_data[self.exchange_data['Exchange'] == exchange]

            if df is None or df.empty:
                print(f"No data available for exchange {exchange}")
                return {
                    'status': 'Not Found',
                    'results_count': 0,
                    'values': []
                }

            searchtext = searchtext.upper()

            mask = df.apply(lambda row: any(
                searchtext in str(val).upper()
                for val in [
                    row.get('TradingSymbol', ''), 
                    row.get('Symbol', ''),
                    row.get('instrument_type', '')
                ]
            ), axis=1)

            result = df[mask]

            if not result.empty:
                if 'instrument_type' in result.columns:
                    result = result.sort_values(['instrument_type', 'TradingSymbol'])
                else:
                    result = result.sort_values('TradingSymbol')

                display_columns = ['TradingSymbol', 'Symbol', 'Exchange', 'Token']
                optional_columns = ['instrument_type', 'expiry', 'strike', 'lot_size']
                display_columns.extend([col for col in optional_columns if col in result.columns])

                if 'expiry' in result.columns:
                    result['expiry'] = pd.to_datetime(result['expiry'], errors='coerce').dt.strftime('%d-%b-%Y')

                get_ltp = self.get_ltp_token(exchange, result['Token'].iloc[0])
                return get_ltp

            print(f"No matches found for '{searchtext}' in {exchange}")
            return {
                'status': 'Not Found',
                'results_count': 0,
                'values': []
            }

        except Exception as e:
            print(f"Error searching script: {e}")
            return {
                'status': 'Error',
                'error_message': str(e),
                'values': []
            }


    def get_ltp_token(self, exchange, token):
        ltp_data = self.get_quotes(exchange=exchange, token=token)
        return ltp_data.get('lp') if ltp_data and ltp_data.get("stat") == "Ok" else "Failed to fetch LTP"

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
            if exchange in ['NFO', 'CDS', 'MCX', 'BFO', 'BCD']:
                product = 'N'
            elif is_overnight:
                product = "C"

            # Define order parameters
            order_params = {
                "tsym": symbol,
                "exch": exchange,
                "trantype": buy_sell,
                "qty": qty,
                "prctyp": order_type,
                "price": price if order_type == "LIMIT" else 0,
                "prd": product,
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
                    price_type='SL-LMT',
                    price=price,
                    trigger_price=199.50,
                    retention='DAY',
                    remarks='my_order_001'
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
                    rejection_reason = order_status.get[-1]('rejreason', 'Unknown reason')
                    print(f"Order rejected: {rejection_reason}")
                    if "Insufficient balance" in rejection_reason:
                        return None, None, "Order placement failed due to insufficient funds."
                    else:
                        return None, None, f"Order placement failed: {rejection_reason}"
                else:
                 
                    # Poll for order status
                    time.sleep(0.5)
                    order_status = self.single_order_history(orderno)
                    latest_status = order_status[-1]
                    status = latest_status.get("status")
                   
                    if status == 'COMPLETE':
                        average_price = order_status[-1].get('upldprc', 0)
                    else:
                       
                        
                        # Cancel the order if it is still open
                        cancel_result = self.cancel_order(orderno)
                        
                        return None, None, "Order was canceled due to timeout."

            else:
                order_id = 'Paper' + str(uuid.uuid4())
                print(f"Paper trade created with ID: {order_id}")

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

    print("\nTesting BANKNIFTY weekly CE:")
    print(ts.get_token_details('NFO', 'BANKNIFTY', '46000', is_pe=0, expiry='W', instrumenttype='OPTIDX'))
    print(ts.search_script('NSE', 'RELIANCE'))
    print("\nTesting SENSEX monthly CE:")
    print(ts.get_token_details('BFO', 'SENSEX', '72000', is_pe=0, expiry='M', instrumenttype='OPTIDX'))
