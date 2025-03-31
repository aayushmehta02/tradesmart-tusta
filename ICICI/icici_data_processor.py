import io
import logging
import os
import zipfile

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class ICICIDataProcessor:
    def __init__(self):
        self.zip_url = "https://directlink.icicidirect.com/NewSecurityMaster/SecurityMaster.zip"
        self.extract_dir = "icici_instrument_data"
        self.combined_csv_file = "combined_instrument_data.csv"
        self.columns_to_keep = [
            'Token', 'ShortName', 'Series',
            'ExchangeCode', 'ExpiryDate', 'StrikePrice',
            'OptionType', 'ExAllowed', 'LotSize', 'Name',
        ]

    def download_and_extract_zip(self):
        """Download and extract the ZIP file containing instrument data."""
        try:
            logging.info("Downloading instrument data...")
            response = requests.get(self.zip_url)
            
            if response.status_code != 200:
                raise Exception(f"Failed to download. Status code: {response.status_code}")

            # Create BytesIO object and extract ZIP contents
            zip_data = io.BytesIO(response.content)
            os.makedirs(self.extract_dir, exist_ok=True)
            
            with zipfile.ZipFile(zip_data, 'r') as zip_ref:
                zip_ref.extractall(self.extract_dir)
                
            logging.info(f"Data extracted to '{self.extract_dir}'")
            return True
            
        except Exception as e:
            logging.error(f"Error in download_and_extract_zip: {str(e)}")
            return False

    def process_txt_files(self):
        """Process extracted TXT files and combine into single CSV."""
        try:
            # List all TXT files in the directory
            txt_files = [f for f in os.listdir(self.extract_dir) if f.endswith('.txt')]
            if not txt_files:
                raise Exception("No TXT files found in extract directory")

            dfs = []
            for txt_file in txt_files:
                file_path = os.path.join(self.extract_dir, txt_file)
                
                # Read and process each TXT file
                df = pd.read_csv(file_path, delimiter=',')
                df.columns = [column_name.strip(' "') for column_name in df.columns]
                
                # Keep only required columns that exist in the file
                available_columns = [col for col in self.columns_to_keep if col in df.columns]
                df = df[available_columns]
                dfs.append(df)

            logging.info(f"Processed {len(dfs)} files")

            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            
            # Process dates and sort
            combined_df['ExpiryDate'] = pd.to_datetime(combined_df['ExpiryDate'], errors='coerce')
            combined_df = combined_df.sort_values(by='ExpiryDate', ascending=True)
            combined_df = combined_df.reset_index(drop=True)
            
            # Save to CSV
            combined_df.to_csv(self.combined_csv_file, index=False)
            logging.info(f"Data saved to '{self.combined_csv_file}'")
            
            return combined_df
            
        except Exception as e:
            logging.error(f"Error in process_txt_files: {str(e)}")
            return None

    def run(self):
        """Execute the complete data processing pipeline."""
        if self.download_and_extract_zip():
            return self.process_txt_files()
        return None

def main():
    processor = ICICIDataProcessor()
    result_df = processor.run()
    
    if result_df is not None:
        logging.info(f"Successfully processed {len(result_df)} rows of data")
    else:
        logging.error("Failed to process instrument data")

if __name__ == "__main__":
    main() 