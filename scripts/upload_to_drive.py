# scripts/upload_to_drive.py
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
import os
import json
import glob

class DriveUploader:
    def __init__(self, credentials_json):
        creds_info = json.loads(credentials_json)
        self.credentials = Credentials.from_service_account_info(creds_info)
        self.service = build('drive', 'v3', credentials=self.credentials)
        self.folder_id = "YOUR_FOLDER_ID"  # ID de tu carpeta en Drive
        
    def upload_file(self, file_path, file_name):
        """Sube archivo a Google Drive"""
        file_metadata = {
            'name': file_name,
            'parents': [self.folder_id]
        }
        media = MediaFileUpload(file_path, resumable=True)
        
        file = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id'
        ).execute()
        
        return file.get('id')
    
    def upload_latest_data(self):
        """Sube los archivos de datos más recientes"""
        data_files = glob.glob('data/forex_data_*.csv')
        
        for file_path in data_files:
            file_name = os.path.basename(file_path)
            file_id = self.upload_file(file_path, file_name)
            print(f"Archivo subido: {file_name} - ID: {file_id}")

if __name__ == "__main__":
    credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    uploader = DriveUploader(credentials_json)
    uploader.upload_latest_data()
