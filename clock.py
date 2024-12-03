from apscheduler.schedulers.blocking import BlockingScheduler  
from google_sheets_service import GoogleSheetsService  
from crypto_service import fetch_crypto_data  

sched = BlockingScheduler()  

@sched.scheduled_job('interval', minutes=5)  
def update_sheets():  
    sheets_service = GoogleSheetsService()  
    crypto_data = fetch_crypto_data()  
    sheets_service.update_sheet(crypto_data)  

sched.start()