# scripts/data_collector.py
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json

class DataCollector:
    def __init__(self):
        self.symbols = ['EURUSD=X', 'GBPUSD=X', 'USDJPY=X', 'AUDUSD=X']
        self.features = []
        
    def collect_ohlcv_data(self):
        """Recolecta datos OHLCV de múltiples pares"""
        all_data = []
        
        for symbol in self.symbols:
            ticker = yf.Ticker(symbol)
            # Obtener datos de los últimos 30 días con intervalos de 15min
            data = ticker.history(period="30d", interval="15m")
            
            if not data.empty:
                data['symbol'] = symbol
                data['timestamp'] = data.index
                all_data.append(data)
                
        return pd.concat(all_data, ignore_index=True)
    
    def add_technical_indicators(self, df):
        """Añade indicadores técnicos como features"""
        df = df.copy()
        
        # RSI
        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        # Moving Averages
        df['MA_10'] = df['Close'].rolling(window=10).mean()
        df['MA_20'] = df['Close'].rolling(window=20).mean()
        df['MA_50'] = df['Close'].rolling(window=50).mean()
        
        # MACD
        exp1 = df['Close'].ewm(span=12).mean()
        exp2 = df['Close'].ewm(span=26).mean()
        df['MACD'] = exp1 - exp2
        df['MACD_signal'] = df['MACD'].ewm(span=9).mean()
        
        # Bollinger Bands
        df['BB_middle'] = df['Close'].rolling(window=20).mean()
        bb_std = df['Close'].rolling(window=20).std()
        df['BB_upper'] = df['BB_middle'] + (bb_std * 2)
        df['BB_lower'] = df['BB_middle'] - (bb_std * 2)
        
        # Volatilidad
        df['Volatility'] = df['Close'].rolling(window=20).std()
        
        # Price changes
        df['Price_change_1'] = df['Close'].pct_change(1)
        df['Price_change_5'] = df['Close'].pct_change(5)
        
        return df
    
    def create_features_target(self, df):
        """Crea features y target para ML"""
        df = self.add_technical_indicators(df)
        
        # Target: predicción del precio en 4 períodos (1 hora)
        df['target'] = df['Close'].shift(-4)
        df['target_direction'] = (df['target'] > df['Close']).astype(int)
        
        # Features seleccionadas
        feature_columns = [
            'Open', 'High', 'Low', 'Close', 'Volume',
            'RSI', 'MA_10', 'MA_20', 'MA_50',
            'MACD', 'MACD_signal', 'BB_upper', 'BB_lower',
            'Volatility', 'Price_change_1', 'Price_change_5'
        ]
        
        return df[feature_columns + ['target', 'target_direction', 'timestamp', 'symbol']]

if __name__ == "__main__":
    collector = DataCollector()
    raw_data = collector.collect_ohlcv_data()
    processed_data = collector.create_features_target(raw_data)
    
    # Guardar datos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    processed_data.to_csv(f'data/forex_data_{timestamp}.csv', index=False)
    print(f"Datos recolectados: {len(processed_data)} filas")
