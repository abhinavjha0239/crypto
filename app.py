import os  
import time  
import threading  
import logging  
from urllib.parse import quote as url_quote  

from flask import Flask, render_template, jsonify, request  
from flask_socketio import SocketIO  
from dotenv import load_dotenv  

# Import custom services  
from google_sheets_service import GoogleSheetsService  
from crypto_service import fetch_crypto_data, analyze_crypto_data  

# Configure logging  
logging.basicConfig(  
    level=logging.INFO,  
    format='%(asctime)s - %(levelname)s: %(message)s',  
    handlers=[  
        logging.FileHandler('crypto_tracker.log'),  
        logging.StreamHandler()  
    ]  
)  
logger = logging.getLogger(__name__)  

# Load environment variables  
load_dotenv()  

# Flask and SocketIO initialization  
app = Flask(__name__)  
app.config['SECRET_KEY'] = os.getenv('FLASK_SECRET_KEY', 'default_secret_key')  
socketio = SocketIO(app, cors_allowed_origins="*")  

# Initialize Google Sheets service  
try:  
    sheets_service = GoogleSheetsService()  
except Exception as e:  
    logger.error(f"Failed to initialize Google Sheets Service: {e}")  
    sheets_service = None  

def background_update():  
    """  
    Background thread for periodic crypto data updates  
    """  
    error_count = 0  
    max_errors = 5  

    while error_count < max_errors:  
        try:  
            # Fetch latest crypto data  
            crypto_data = fetch_crypto_data()  
            
            if not crypto_data:  
                logger.warning("No crypto data fetched")  
                error_count += 1  
                time.sleep(60)  # Wait a minute before retrying  
                continue  

            # Perform data analysis  
            analysis_results = analyze_crypto_data(crypto_data)  
            
            # Emit data via WebSocket  
            socketio.emit('crypto_update', {  
                'data': crypto_data,  
                'analysis': analysis_results  
            })  

            # Update Google Sheets if service is initialized  
            if sheets_service:  
                sheets_service.update_sheet(crypto_data)  

            # Reset error count on successful update  
            error_count = 0  
            
            # Wait for 5 minutes between updates  
            time.sleep(300)  

        except Exception as e:  
            logger.error(f"Background update error: {e}")  
            error_count += 1  
            time.sleep(60)  # Wait a minute before retrying  

    logger.critical("Background update stopped due to repeated errors")  

@app.route('/')  
def index():  
    """  
    Main landing page with Google Sheets link  
    """  
    try:  
        spreadsheet_id = os.getenv('GOOGLE_SPREADSHEET_ID')  
        sheets_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit?gid=0"  
        return render_template('index.html', sheets_url=sheets_url)  
    except Exception as e:  
        logger.error(f"Error rendering index page: {e}")  
        return "An error occurred", 500  

@app.route('/crypto-data')  
def get_crypto_data():  
    """  
    API endpoint to fetch latest crypto data and analysis  
    """  
    try:  
        data = fetch_crypto_data()  
        analysis = analyze_crypto_data(data)  
        return jsonify({  
            'data': data,   
            'analysis': analysis,  
            'timestamp': time.time()  
        })  
    except Exception as e:  
        logger.error(f"Error fetching crypto data: {e}")  
        return jsonify({  
            'error': 'Failed to fetch crypto data',  
            'details': str(e)  
        }), 500  

@socketio.on('connect')  
def handle_connect():  
    """  
    WebSocket connection handler  
    """  
    logger.info(f'Client connected: {request.sid}')  

@socketio.on('disconnect')  
def handle_disconnect():  
    """  
    WebSocket disconnection handler  
    """  
    logger.info(f'Client disconnected: {request.sid}')  

@app.errorhandler(500)  
def handle_500(error):  
    """  
    Custom 500 error handler  
    """  
    logger.error(f"Server Error: {error}")  
    return jsonify({  
        'error': 'Internal Server Error',  
        'message': str(error)  
    }), 500  

@app.errorhandler(404)  
def handle_404(error):  
    """  
    Custom 404 error handler  
    """  
    logger.warning(f"Resource Not Found: {error}")  
    return jsonify({  
        'error': 'Resource Not Found',  
        'message': 'The requested endpoint does not exist'  
    }), 404  

def run_app():  
    """  
    Main application runner with thread management  
    """  
    try:  
        # Start the background update thread  
        update_thread = threading.Thread(target=background_update, daemon=True)  
        update_thread.start()  

        # Run the Flask-SocketIO application  
        socketio.run(  
            app,   
            host='0.0.0.0',   
            port=int(os.getenv('PORT', 5000)),   
            debug=os.getenv('FLASK_DEBUG', 'False') == 'True'  
        )  

    except Exception as e:  
        logger.critical(f"Application startup failed: {e}")  

if __name__ == '__main__':  
    run_app()