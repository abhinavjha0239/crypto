import requests
import numpy as np
import pandas as pd
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def fetch_crypto_data() -> List[Dict[Any, Any]]:
    """
    Enhanced cryptocurrency data fetching with advanced processing and error handling
    """
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 50,  # Increased to 100 for more comprehensive analysis
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h,7d,30d'  # Additional time frames
        }

        # Add timeout and error handling
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        # Basic data validation
        if not data:
            logger.warning("No cryptocurrency data received")
            return []

        logger.info(f"Successfully fetched {len(data)} cryptocurrencies")
        return data

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during data fetch: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error during data processing: {e}")
        return []

def analyze_crypto_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:  
    """  
    Perform comprehensive cryptocurrency market analysis  

    Includes:  
    - Advanced price statistics  
    - Market capitalization insights  
    """  
    if not data:  
        return {  
            'error': 'No data available for analysis',  
            'timestamp': str(pd.Timestamp.now())  
        }  

    # Convert to DataFrame with error handling for missing columns  
    try:  
        df = pd.DataFrame(data)  
    except Exception as e:  
        logger.error(f"Error creating DataFrame: {e}")  
        return {  
            'error': 'Failed to process data',  
            'details': str(e),  
            'timestamp': str(pd.Timestamp.now())  
        }  

    # Add column existence checks  
    required_columns = [  
        'market_cap', 'current_price', 'price_change_percentage_24h',  
        'price_change_percentage_7d', 'name', 'symbol'  
    ]  

    missing_columns = [col for col in required_columns if col not in df.columns]  
    if missing_columns:  
        logger.warning(f"Missing columns: {missing_columns}")  

        # Fill missing columns with default values  
        for col in missing_columns:  
            if 'price_change_percentage' in col:  
                df[col] = 0.0  
            elif col == 'market_cap':  
                df[col] = 0  
            else:  
                df[col] = 'N/A'  

    try:  
        # Comprehensive price statistics  
        price_stats = {  
            'mean_price': float(df['current_price'].mean()),  
            'median_price': float(df['current_price'].median()),  
            'price_std_dev': float(df['current_price'].std()),  
            'min_price': float(df['current_price'].min()),  
            'max_price': float(df['current_price'].max())  
        }  

        # Market capitalization insights  
        market_cap_insights = {  
            'total_market_cap': float(df['market_cap'].sum()),  
            'mean_market_cap': float(df['market_cap'].mean()),  
            'median_market_cap': float(df['market_cap'].median())  
        }  

        analysis = {  
            'price_statistics': price_stats,  
            'market_cap_insights': market_cap_insights,  
            'timestamp': str(pd.Timestamp.now())  
        }  

        return analysis  

    except Exception as e:  
        logger.error(f"Error during cryptocurrency data analysis: {e}")  
        return {  
            'error': 'Analysis failed',  
            'details': str(e),  
            'timestamp': str(pd.Timestamp.now())  
        }

    except Exception as e:
        logger.error(f"Error during cryptocurrency data analysis: {e}")
        return {
            'error': 'Analysis failed',
            'details': str(e),
            'timestamp': str(pd.Timestamp.now())
        }

# Explicitly export the functions
__all__ = ['fetch_crypto_data', 'analyze_crypto_data']