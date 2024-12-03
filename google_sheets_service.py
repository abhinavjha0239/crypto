import os  
import logging  
import pandas as pd  
from google_auth_oauthlib.flow import InstalledAppFlow  
from google.auth.transport.requests import Request  
from google.oauth2.credentials import Credentials  
from googleapiclient.discovery import build  

class GoogleSheetsService:  
    def __init__(self):  
        # Scopes for Google Sheets  
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']  

        # Spreadsheet ID  
        self.SPREADSHEET_ID = '1bnq2a1ulUm3QYSeBu8Gc9WjcUCbw1gKpGyBPGDcG3kM'  

        # Path to client secrets  
        self.CLIENT_SECRETS_FILE = 'client_secrets.json'  

        # Token storage path  
        self.TOKEN_FILE = 'token.json'  

        # Initialize credentials  
        self.credentials = self.get_credentials()  

        # Build Google Sheets service  
        self.service = build('sheets', 'v4', credentials=self.credentials)  

    def get_credentials(self):  
        credentials = None  

        # Check for existing token  
        if os.path.exists(self.TOKEN_FILE):  
            credentials = Credentials.from_authorized_user_file(  
                self.TOKEN_FILE,  
                self.SCOPES  
            )  

        # Refresh or recreate credentials if needed  
        if not credentials or not credentials.valid:  
            if credentials and credentials.expired and credentials.refresh_token:  
                credentials.refresh(Request())  
            else:  
                flow = InstalledAppFlow.from_client_secrets_file(  
                    self.CLIENT_SECRETS_FILE,  
                    self.SCOPES  
                )  
                credentials = flow.run_local_server(port=8080)  

            # Save the credentials for next run  
            with open(self.TOKEN_FILE, 'w') as token:  
                token.write(credentials.to_json())  

        return credentials  

    def update_sheet(self, crypto_data):  
        try:  
            # Convert to DataFrame if needed  
            if not isinstance(crypto_data, pd.DataFrame):  
                crypto_data = pd.DataFrame(crypto_data)  

            # Updated columns list including timestamp  
            columns_to_write = [  
                'name',  
                'symbol',  
                'current_price',  
                'market_cap',  
                'total_volume',  
                'price_change_percentage_24h',  
                'market_cap_rank',  
                'timestamp'  
            ]  

            # Check for missing columns and handle them  
            for column in columns_to_write:  
                if column not in crypto_data.columns:  
                    logging.warning(f"Missing column: {column}")  
                    crypto_data[column] = None  # Add column with NULL values  

            # Add timestamp column  
            crypto_data['timestamp'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')  

            # Prepare sheet data  
            sheet_data = crypto_data[columns_to_write].values.tolist()  
            sheet_data.insert(0, columns_to_write)  # Add header  

            # Determine range dynamically  
            end_row = len(sheet_data)  
            range_name = f'Sheet1!A1:H{end_row}'  # Updated to H to accommodate timestamp column  

            logging.info(f"Updating sheet: {self.SPREADSHEET_ID}")  
            logging.info(f"Range: {range_name}")  
            logging.info(f"Rows to be added: {end_row}")  

            # Clear existing data  
            self.service.spreadsheets().values().clear(  
                spreadsheetId=self.SPREADSHEET_ID,  
                range='Sheet1!A1:H51'  # Updated to H to accommodate timestamp column  
            ).execute()  

            # Update with new data  
            body = {'values': sheet_data}  
            response = self.service.spreadsheets().values().update(  
                spreadsheetId=self.SPREADSHEET_ID,  
                range=range_name,  
                valueInputOption='RAW',  
                body=body  
            ).execute()  

            logging.info(f"Sheet update successful: {response}")  
            return response  

        except Exception as e:  
            logging.error(f"Error updating sheet: {e}")  
            raise  

def main():  
    sheets_service = GoogleSheetsService()  
    crypto_data = [  
        {'name': 'Bitcoin', 'symbol': 'BTC', 'current_price': 50000, 'market_cap': 1000000000, 'total_volume': 100000, 'price_change_percentage_24h': 2.5, 'market_cap_rank': 1},  
        {'name': 'Ethereum', 'symbol': 'ETH', 'current_price': 2000, 'market_cap': 500000000, 'total_volume': 50000, 'price_change_percentage_24h': -1.2, 'market_cap_rank': 2},  
        {'name': 'Litecoin', 'symbol': 'LTC', 'current_price': 200, 'market_cap': 100000000, 'total_volume': 20000, 'price_change_percentage_24h': 0.8, 'market_cap_rank': 5}  
    ]  
    sheets_service.update_sheet(crypto_data)  

if __name__ == '__main__':  
    main()