import pandas as pd


def create_normalized_symbols():
    # Load the data
    df = pd.read_csv(r"C:\Users\aayus\OneDrive\Desktop\finance-browser\combined_instruments.csv")
    
    # Convert expiry to datetime
    df['expiry_date'] = pd.to_datetime(df['Expiry'], format='%d-%b-%Y', errors='coerce')
    
    def generate_normalized_symbol(row):
        try:
            symbol = str(row['Symbol'])
            
            # For equity instruments
            if row['Instrument'] == 'EQ':
                return symbol

            # Handle expiry formatting
            if pd.notna(row['expiry_date']):
                # Check if weekly by adding 7 days and seeing if month changes
                next_week = row['expiry_date'] + pd.Timedelta(days=7)
                is_weekly = next_week.month == row['expiry_date'].month

                if 'FUT' in str(row['Instrument']):
                    # For futures: DDMMMYY format
                    expiry = row['expiry_date'].strftime('%d%b%y').upper()
                elif is_weekly:  # Weekly options
                    # For weekly options: YYMMDD format
                    expiry = (row['expiry_date'].strftime('%y') + 
                            str(int(row['expiry_date'].strftime('%m'))) + 
                            row['expiry_date'].strftime('%d'))
                else:  # Monthly options
                    # For monthly options: YYMM format
                    expiry = row['expiry_date'].strftime('%y%b').upper()
            else:
                expiry = ''
            
            # Handle strike price: skip for FUT instruments
            if pd.notna(row['StrikePrice']) and 'FUT' not in str(row['Instrument']):
                strike_price = str(int(float(row['StrikePrice'])))
            else:
                strike_price = ''

            # Handle option type
            if 'OPT' in str(row['Instrument']):
                option_type = str(row['OptionType'])
            else:
                option_type = 'FUT'

            return symbol + expiry + strike_price + option_type
            
        except Exception as e:
            print(f"Error processing row: {row}")
            return row['TradingSymbol']

    # Create new column
    df['NormalizedSymbol'] = df.apply(generate_normalized_symbol, axis=1)
    
    # Save to new file
    output_path = r"C:\Users\aayus\OneDrive\Desktop\finance-browser\normalized_instruments.csv"
    df.to_csv(output_path, index=False)
    
    # Print some examples to verify
    print("\nSample of normalized symbols with expiry type:")
    sample = df[['TradingSymbol', 'NormalizedSymbol', 'Expiry']].head(10)
    for _, row in sample.iterrows():
        expiry_date = pd.to_datetime(row['Expiry'], format='%d-%b-%Y')
        next_week = expiry_date + pd.Timedelta(days=7)
        is_weekly = next_week.month == expiry_date.month
        print(f"Trading: {row['TradingSymbol']}")
        print(f"Normalized: {row['NormalizedSymbol']}")
        print(f"Type: {'Weekly' if is_weekly else 'Monthly'}")
        print("---")

if __name__ == "__main__":
    create_normalized_symbols()