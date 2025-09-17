# scripts/data_collector.py - VERSIÓN CORREGIDA
import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests
import json
import os
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
import io

class ForexDataCollector:
    def __init__(self):
        self.symbols = ['EURUSD=X', 'GBPUSD=X', 'USDJPY=X', 'AUDUSD=X', 'USDCAD=X']
        
        # Configurar Google Drive
        credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if credentials_json:
            creds_info = json.loads(credentials_json)
            credentials = Credentials.from_service_account_info(creds_info)
            self.drive_service = build('drive', 'v3', credentials=credentials)
            self.folder_id = '1RHFvpR2Pt_la_PdcTqZgCMYzgfEFldgI'  # Tu folder ID
        else:
            self.drive_service = None
            
    def collect_forex_data(self):
        """Recolecta datos OHLCV de múltiples pares"""
        print("📊 Recolectando datos de forex...")
        
        all_data = []
        
        for symbol in self.symbols:
            try:
                print(f"Descargando {symbol}...")
                ticker = yf.Ticker(symbol)
                
                # Obtener datos de las últimas 24 horas con intervalo de 15min
                data = ticker.history(period="1d", interval="15m")
                
                if not data.empty:
                    data['Symbol'] = symbol.replace('=X', '')
                    data['Timestamp'] = data.index
                    data = data.reset_index(drop=True)
                    all_data.append(data)
                    print(f"✅ {symbol}: {len(data)} registros")
                else:
                    print(f"⚠️ {symbol}: Sin datos")
                    
            except Exception as e:
                print(f"❌ Error con {symbol}: {e}")
                continue
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            print(f"📈 Total de registros recolectados: {len(combined_df)}")
            return combined_df
        else:
            print("❌ No se pudieron recolectar datos")
            return pd.DataFrame()
    
    def add_technical_indicators(self, df):
        """Añade indicadores técnicos"""
        print("🔧 Calculando indicadores técnicos...")
        
        df = df.copy()
        
        # Procesar por símbolo
        processed_data = []
        
        for symbol in df['Symbol'].unique():
            symbol_data = df[df['Symbol'] == symbol].copy().sort_values('Timestamp')
            
            if len(symbol_data) < 50:  # Mínimo datos requeridos
                continue
                
            # RSI
            delta = symbol_data['Close'].diff()
            gain = delta.where(delta > 0, 0).rolling(window=14, min_periods=1).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14, min_periods=1).mean()
            rs = gain / loss
            symbol_data['RSI'] = 100 - (100 / (1 + rs))
            
            # Moving Averages
            symbol_data['MA_10'] = symbol_data['Close'].rolling(window=10, min_periods=1).mean()
            symbol_data['MA_20'] = symbol_data['Close'].rolling(window=20, min_periods=1).mean()
            
            # MACD
            ema12 = symbol_data['Close'].ewm(span=12).mean()
            ema26 = symbol_data['Close'].ewm(span=26).mean()
            symbol_data['MACD'] = ema12 - ema26
            symbol_data['MACD_Signal'] = symbol_data['MACD'].ewm(span=9).mean()
            
            # Bollinger Bands
            symbol_data['BB_Middle'] = symbol_data['Close'].rolling(window=20, min_periods=1).mean()
            bb_std = symbol_data['Close'].rolling(window=20, min_periods=1).std()
            symbol_data['BB_Upper'] = symbol_data['BB_Middle'] + (bb_std * 2)
            symbol_data['BB_Lower'] = symbol_data['BB_Middle'] - (bb_std * 2)
            
            # Volatilidad
            symbol_data['Volatility'] = symbol_data['Close'].rolling(window=20, min_periods=1).std()
            
            # Cambios de precio
            symbol_data['Price_Change_1'] = symbol_data['Close'].pct_change(1)
            symbol_data['Price_Change_5'] = symbol_data['Close'].pct_change(5)
            
            processed_data.append(symbol_data)
        
        if processed_data:
            result_df = pd.concat(processed_data, ignore_index=True)
            print(f"✅ Indicadores calculados para {len(result_df)} registros")
            return result_df
        else:
            return df
    
    def create_prediction_target(self, df):
        """Crea target para predicción"""
        print("🎯 Creando targets de predicción...")
        
        df = df.copy()
        
        # Procesar por símbolo
        for symbol in df['Symbol'].unique():
            mask = df['Symbol'] == symbol
            symbol_data = df[mask].copy().sort_values('Timestamp')
            
            # Target: precio en 4 períodos (1 hora)
            future_price = symbol_data['Close'].shift(-4)
            current_price = symbol_data['Close']
            
            # Dirección (1 = sube, 0 = baja)
            direction = (future_price > current_price).astype(int)
            
            # Actualizar dataframe principal
            df.loc[mask, 'Target_Price'] = future_price
            df.loc[mask, 'Target_Direction'] = direction
        
        # Eliminar filas sin target válido
        df = df.dropna(subset=['Target_Price', 'Target_Direction'])
        
        print(f"🎯 Targets creados para {len(df)} registros")
        return df
    
    def save_to_csv(self, df):
        """Guarda datos en CSV"""
        if df.empty:
            print("⚠️ No hay datos para guardar")
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"forex_data_{timestamp}.csv"
        
        # Seleccionar columnas importantes
        important_columns = [
            'Timestamp', 'Symbol', 'Open', 'High', 'Low', 'Close', 'Volume',
            'RSI', 'MA_10', 'MA_20', 'MACD', 'MACD_Signal',
            'BB_Upper', 'BB_Lower', 'Volatility', 
            'Price_Change_1', 'Price_Change_5',
            'Target_Price', 'Target_Direction'
        ]
        
        # Filtrar solo columnas que existen
        available_columns = [col for col in important_columns if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        # Guardar localmente
        df_filtered.to_csv(filename, index=False)
        print(f"💾 Datos guardados localmente: {filename}")
        
        return filename
    
    def upload_to_drive(self, filename):
        """Sube archivo a Google Drive"""
        if not self.drive_service or not filename:
            print("⚠️ Google Drive no configurado o archivo no válido")
            return
        
        try:
            file_metadata = {
                'name': filename,
                'parents': [self.folder_id]
            }
            
            media = MediaFileUpload(filename, resumable=True)
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,size'
            ).execute()
            
            print(f"☁️ Archivo subido a Drive: {file['name']} ({file.get('size', 0)} bytes)")
            
            # Limpiar archivo local
            if os.path.exists(filename):
                os.remove(filename)
                print(f"🗑️ Archivo local eliminado: {filename}")
            
            return file['id']
            
        except Exception as e:
            print(f"❌ Error subiendo a Drive: {e}")
            return None
    
    def update_google_sheets(self, df):
        """Actualiza Google Sheets con datos más recientes"""
        if df.empty or not self.drive_service:
            return
            
        try:
            from googleapiclient.discovery import build
            
            # Configurar Sheets API
            sheets_service = build('sheets', 'v4', credentials=self.drive_service._http.credentials)
            
            # ID de la hoja "Latest Data"
            sheet_id = '1JNOcONmYy9EBbJYQYfHqFb_xSU7TWxL0tx97WeYNLSI'
            
            # Tomar últimas 10 filas
            latest_data = df.tail(10)
            
            # Preparar datos para Sheets
            values = []
            for _, row in latest_data.iterrows():
                values.append([
                    str(row.get('Timestamp', '')),
                    str(row.get('Symbol', '')),
                    float(row.get('Open', 0)),
                    float(row.get('High', 0)),
                    float(row.get('Low', 0)),
                    float(row.get('Close', 0)),
                    int(row.get('Volume', 0)),
                    float(row.get('RSI', 0)),
                    float(row.get('MACD', 0)),
                    str(row.get('Target_Direction', 0)),
                    str(row.get('Target_Direction', 0)),  # Prediction placeholder
                    float(row.get('Volatility', 0))
                ])
            
            # Actualizar hoja
            body = {
                'values': values
            }
            
            sheets_service.spreadsheets().values().append(
                spreadsheetId=sheet_id,
                range='Sheet1!A:L',
                valueInputOption='RAW',
                body=body
            ).execute()
            
            print(f"📊 Google Sheets actualizado con {len(values)} filas")
            
        except Exception as e:
            print(f"⚠️ Error actualizando Sheets: {e}")
    
    def run_collection(self):
        """Ejecuta el proceso completo de recolección"""
        print(f"🚀 Iniciando recolección de datos - {datetime.now()}")
        
        try:
            # 1. Recolectar datos
            raw_data = self.collect_forex_data()
            
            if raw_data.empty:
                print("❌ No se pudieron recolectar datos")
                return False
            
            # 2. Añadir indicadores técnicos
            processed_data = self.add_technical_indicators(raw_data)
            
            # 3. Crear targets de predicción
            final_data = self.create_prediction_target(processed_data)
            
            # 4. Guardar en CSV
            filename = self.save_to_csv(final_data)
            
            # 5. Subir a Google Drive
            if filename:
                self.upload_to_drive(filename)
            
            # 6. Actualizar Google Sheets
            self.update_google_sheets(final_data)
            
            print("✅ Recolección completada exitosamente")
            return True
            
        except Exception as e:
            print(f"❌ Error en recolección: {e}")
            return False

def main():
    """Función principal para GitHub Actions"""
    collector = ForexDataCollector()
    success = collector.run_collection()
    
    if success:
        print("🎉 Proceso completado exitosamente")
        return 0
    else:
        print("💥 Proceso falló")
        return 1

if __name__ == "__main__":
    exit(main())
