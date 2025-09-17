# webhook_handler.py (Para ejecutar en servidor o Actions)

import requests
import json
from datetime import datetime

class MyFXBookIntegration:
    def __init__(self, webhook_url, account_credentials):
        self.webhook_url = webhook_url
        self.credentials = account_credentials
        
    def send_signal(self, symbol, action, lots, stop_loss=None, take_profit=None):
        """Envía señal de trading a MyFXBook"""
        signal_data = {
            'symbol': symbol,
            'action': action,  # 'BUY' o 'SELL'
            'volume': lots,
            'timestamp': datetime.now().isoformat(),
            'source': 'neural_network_bot'
        }
        
        if stop_loss:
            signal_data['stop_loss'] = stop_loss
        if take_profit:
            signal_data['take_profit'] = take_profit
            
        try:
            response = requests.post(
                self.webhook_url,
                json=signal_data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                print(f"Señal enviada exitosamente: {symbol} {action}")
                return True
            else:
                print(f"Error enviando señal: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"Error en webhook: {e}")
            return False
    
    def get_account_status(self):
        """Obtiene estado de la cuenta"""
        # Implementar llamada a API de MyFXBook para obtener estado
        pass
