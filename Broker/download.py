import io
import logging
import zipfile

import pandas as pd
import requests


def download_and_combine_data():
    # URLs containing the data
    urls = [
        "https://v2api.tradesmartonline.in/NFO_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/BCD_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/CDS_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/NSE_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/BSE_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/MCX_symbols.txt.zip",
        "https://v2api.tradesmartonline.in/BFO_symbols.txt.zip"
    ]

    all_data = []  # List to store DataFrames from each URL

    for url in urls:
        try:
            logging.info(f"Downloading from {url}")
            # Download the zip file
            response = requests.get(url)
            response.raise_for_status()  # Raise exception for bad status codes

            # Extract the zip file in memory
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Get the txt file (assuming one txt file per zip)
                txt_file = [f for f in z.namelist() if f.endswith('.txt')][0]
                
                # Read the txt file directly into a DataFrame
                with z.open(txt_file) as f:
                    df = pd.read_csv(f)
                    # Add exchange information
                    exchange = txt_file.split('_')[0]
                    df['Exchange'] = exchange
                    all_data.append(df)
                    logging.info(f"Successfully processed {exchange} data with {len(df)} rows")

        except Exception as e:
            logging.error(f"Error processing {url}: {str(e)}")

    if all_data:
        # Combine all DataFrames
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Save the combined data
        output_file = "combined_instruments.csv"
        combined_df.to_csv(output_file, index=False)
        logging.info(f"Combined data saved to {output_file}")
        logging.info(f"Total rows in combined data: {len(combined_df)}")
        
        return combined_df
    else:
        logging.error("No data was downloaded successfully")
        return None