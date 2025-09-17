# scripts/storage_manager.py - VERSIÓN CORREGIDA
import os
import json
import gzip
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import io
import pickle

class IntelligentStorageManager:
    def __init__(self, credentials_json):
        """Inicializa el gestor de almacenamiento"""
        creds_info = json.loads(credentials_json)
        self.credentials = Credentials.from_service_account_info(creds_info)
        self.service = build('drive', 'v3', credentials=self.credentials)
        
        # ID de la carpeta principal (usando el que me diste)
        self.main_folder_id = '1RHFvpR2Pt_la_PdcTqZgCMYzgfEFldgI'
        
        # Configuración de retención
        self.retention_config = {
            'raw_data_days': 30,
            'processed_data_days': 90,
            'old_models_keep': 3,
            'compressed_data_months': 12,
            'max_storage_gb': 12,
            'backup_best_models': 5
        }
    
    def get_storage_usage(self):
        """Obtiene el uso actual de almacenamiento"""
        about = self.service.about().get(fields='storageQuota').execute()
        quota = about.get('storageQuota', {})
        
        used_gb = int(quota.get('usage', 0)) / (1024**3)
        limit_gb = int(quota.get('limit', 15 * 1024**3)) / (1024**3)
        
        print(f"Almacenamiento usado: {used_gb:.2f}GB / {limit_gb:.2f}GB")
        return used_gb, limit_gb
    
    def list_files_in_folder(self, folder_id=None):
        """Lista archivos en la carpeta principal"""
        if folder_id is None:
            folder_id = self.main_folder_id
            
        query = f"'{folder_id}' in parents and trashed=false"
        results = self.service.files().list(
            q=query,
            orderBy='createdTime desc',
            fields='files(id, name, size, createdTime, modifiedTime, mimeType)'
        ).execute()
        return results.get('files', [])
    
    def delete_old_files(self):
        """Elimina archivos antiguos"""
        print("🗑️ Eliminando archivos antiguos...")
        
        files = self.list_files_in_folder()
        cutoff_date = datetime.now() - timedelta(days=self.retention_config['raw_data_days'])
        
        deleted_count = 0
        freed_space = 0
        
        for file in files:
            file_date = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00'))
            
            # Eliminar archivos antiguos (excepto notebooks y scripts importantes)
            if (file_date < cutoff_date and 
                not file['name'].endswith('.ipynb') and
                not file['name'].endswith('.gs') and
                'model' not in file['name'].lower()):
                
                try:
                    self.service.files().delete(fileId=file['id']).execute()
                    file_size = int(file.get('size', 0))
                    freed_space += file_size
                    deleted_count += 1
                    print(f"Eliminado: {file['name']} ({file_size/1024**2:.1f}MB)")
                except Exception as e:
                    print(f"Error eliminando {file['name']}: {e}")
        
        print(f"✅ Archivos eliminados: {deleted_count}")
        print(f"✅ Espacio liberado: {freed_space/1024**2:.1f}MB")
        
        return deleted_count, freed_space
    
    def compress_data_files(self):
        """Comprime archivos de datos"""
        print("🗜️ Comprimiendo archivos de datos...")
        
        files = self.list_files_in_folder()
        
        # Buscar archivos CSV para comprimir
        csv_files = [f for f in files if f['name'].endswith('.csv') and 'compressed' not in f['name']]
        
        compressed_count = 0
        
        for file in csv_files[:5]:  # Comprimir máximo 5 archivos por vez
            try:
                # Descargar archivo
                request = self.service.files().get_media(fileId=file['id'])
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
                # Comprimir
                file_content.seek(0)
                compressed_data = gzip.compress(file_content.read())
                
                # Subir archivo comprimido
                compressed_name = file['name'].replace('.csv', '_compressed.csv.gz')
                
                compressed_buffer = io.BytesIO(compressed_data)
                media = MediaFileUpload(compressed_buffer, mimetype='application/gzip')
                
                file_metadata = {
                    'name': compressed_name,
                    'parents': [self.main_folder_id]
                }
                
                self.service.files().create(body=file_metadata, media_body=media).execute()
                
                # Eliminar archivo original
                self.service.files().delete(fileId=file['id']).execute()
                
                compressed_count += 1
                print(f"Comprimido: {file['name']} → {compressed_name}")
                
            except Exception as e:
                print(f"Error comprimiendo {file['name']}: {e}")
        
        print(f"✅ Archivos comprimidos: {compressed_count}")
        return compressed_count
    
    def run_cleanup(self):
        """Ejecuta limpieza completa"""
        print("🚀 Iniciando limpieza de almacenamiento...")
        
        # Estado inicial
        used_initial, limit = self.get_storage_usage()
        
        # 1. Eliminar archivos antiguos
        deleted_count, freed_space_delete = self.delete_old_files()
        
        # 2. Comprimir archivos de datos
        compressed_count = self.compress_data_files()
        
        # Estado final
        used_final, _ = self.get_storage_usage()
        total_freed = used_initial - used_final
        
        print(f"\n✅ Limpieza completada!")
        print(f"Archivos eliminados: {deleted_count}")
        print(f"Archivos comprimidos: {compressed_count}")
        print(f"Espacio liberado: {total_freed:.2f}GB")
        print(f"Uso final: {used_final:.2f}GB / {limit:.2f}GB ({(used_final/limit)*100:.1f}%)")
        
        return {
            'deleted_files': deleted_count,
            'compressed_files': compressed_count,
            'space_freed_gb': total_freed,
            'final_usage_percent': (used_final/limit)*100
        }

def main():
    """Función principal para GitHub Actions"""
    try:
        credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not credentials_json:
            print("❌ Error: GOOGLE_DRIVE_CREDENTIALS no encontrado")
            return 1
        
        manager = IntelligentStorageManager(credentials_json)
        result = manager.run_cleanup()
        
        print("\n📊 Resumen de limpieza:")
        print(json.dumps(result, indent=2))
        
        # Si el uso sigue alto, alertar
        if result['final_usage_percent'] > 90:
            print("🚨 ALERTA: Uso de almacenamiento aún crítico!")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Error en limpieza: {e}")
        return 1

if __name__ == "__main__":
    exit(main())
