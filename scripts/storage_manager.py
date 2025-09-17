# scripts/storage_manager.py
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
        
        # Configuración de retención
        self.retention_config = {
            'raw_data_days': 30,        # Datos crudos: 30 días
            'processed_data_days': 90,   # Datos procesados: 3 meses
            'old_models_keep': 3,        # Mantener 3 mejores modelos
            'compressed_data_months': 12, # Datos comprimidos: 1 año
            'max_storage_gb': 12,        # Usar máximo 12GB de 15GB
            'backup_best_models': 5      # Mantener 5 mejores modelos como backup
        }
        
        self.folder_structure = {
            'raw_data': 'forex_data_raw',
            'processed_data': 'forex_data_processed', 
            'models': 'forex_models',
            'compressed': 'forex_compressed',
            'backups': 'forex_backups',
            'logs': 'forex_logs'
        }
        
    def get_folder_id(self, folder_name):
        """Obtiene o crea el ID de una carpeta"""
        query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
        results = self.service.files().list(q=query).execute()
        items = results.get('files', [])
        
        if items:
            return items[0]['id']
        else:
            # Crear carpeta si no existe
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            folder = self.service.files().create(body=file_metadata).execute()
            return folder.get('id')
    
    def get_storage_usage(self):
        """Obtiene el uso actual de almacenamiento"""
        about = self.service.about().get(fields='storageQuota').execute()
        quota = about.get('storageQuota', {})
        
        used_gb = int(quota.get('usage', 0)) / (1024**3)
        limit_gb = int(quota.get('limit', 15 * 1024**3)) / (1024**3)
        
        print(f"Almacenamiento usado: {used_gb:.2f}GB / {limit_gb:.2f}GB")
        return used_gb, limit_gb
    
    def list_files_in_folder(self, folder_id, order_by='createdTime'):
        """Lista archivos en una carpeta ordenados por fecha"""
        query = f"'{folder_id}' in parents and trashed=false"
        results = self.service.files().list(
            q=query,
            orderBy=order_by,
            fields='files(id, name, size, createdTime, modifiedTime)'
        ).execute()
        return results.get('files', [])
    
    def delete_file(self, file_id):
        """Elimina un archivo permanentemente"""
        try:
            self.service.files().delete(fileId=file_id).execute()
            return True
        except Exception as e:
            print(f"Error eliminando archivo {file_id}: {e}")
            return False
    
    def compress_and_archive_data(self):
        """Comprime datos antiguos para ahorrar espacio"""
        print("🗜️ Comprimiendo datos antiguos...")
        
        raw_folder_id = self.get_folder_id(self.folder_structure['raw_data'])
        compressed_folder_id = self.get_folder_id(self.folder_structure['compressed'])
        
        # Obtener archivos de datos crudos
        files = self.list_files_in_folder(raw_folder_id)
        
        # Agrupar archivos por mes
        monthly_groups = {}
        cutoff_date = datetime.now() - timedelta(days=self.retention_config['raw_data_days'])
        
        for file in files:
            file_date = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00'))
            
            if file_date < cutoff_date:
                month_key = f"{file_date.year}-{file_date.month:02d}"
                if month_key not in monthly_groups:
                    monthly_groups[month_key] = []
                monthly_groups[month_key].append(file)
        
        # Comprimir cada grupo mensual
        for month, month_files in monthly_groups.items():
            print(f"Comprimiendo datos de {month}...")
            
            # Descargar y combinar datos del mes
            monthly_data = []
            for file in month_files:
                try:
                    # Descargar archivo
                    request = self.service.files().get_media(fileId=file['id'])
                    file_content = io.BytesIO()
                    downloader = MediaIoBaseDownload(file_content, request)
                    done = False
                    while done is False:
                        status, done = downloader.next_chunk()
                    
                    # Leer CSV
                    file_content.seek(0)
                    df = pd.read_csv(file_content)
                    monthly_data.append(df)
                    
                except Exception as e:
                    print(f"Error procesando {file['name']}: {e}")
                    continue
            
            if monthly_data:
                # Combinar todos los datos del mes
                combined_df = pd.concat(monthly_data, ignore_index=True)
                combined_df = combined_df.drop_duplicates()
                
                # Comprimir y subir
                compressed_filename = f"forex_data_{month}_compressed.csv.gz"
                self.upload_compressed_data(combined_df, compressed_filename, compressed_folder_id)
                
                # Eliminar archivos originales después de comprimir
                for file in month_files:
                    self.delete_file(file['id'])
                    print(f"Eliminado: {file['name']}")
    
    def upload_compressed_data(self, df, filename, folder_id):
        """Sube datos comprimidos a Drive"""
        # Crear archivo comprimido en memoria
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_content = csv_buffer.getvalue()
        
        # Comprimir
        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz_file:
            gz_file.write(csv_content.encode('utf-8'))
        
        compressed_buffer.seek(0)
        
        # Subir a Drive
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        media = MediaFileUpload(
            io.BytesIO(compressed_buffer.getvalue()),
            mimetype='application/gzip',
            resumable=True
        )
        
        self.service.files().create(
            body=file_metadata,
            media_body=media
        ).execute()
        
        print(f"Datos comprimidos subidos: {filename}")
    
    def manage_model_versions(self):
        """Gestiona versiones de modelos manteniendo solo los mejores"""
        print("🧠 Gestionando modelos...")
        
        models_folder_id = self.get_folder_id(self.folder_structure['models'])
        backup_folder_id = self.get_folder_id(self.folder_structure['backups'])
        
        # Obtener todos los modelos
        model_files = self.list_files_in_folder(models_folder_id, 'modifiedTime desc')
        
        # Evaluar modelos (esto requeriría métricas guardadas)
        model_performance = self.load_model_performance_metrics()
        
        # Ordenar por rendimiento
        sorted_models = []
        for file in model_files:
            if file['name'].endswith('.h5'):
                performance = model_performance.get(file['name'], 0)
                sorted_models.append({
                    'file': file,
                    'performance': performance
                })
        
        sorted_models.sort(key=lambda x: x['performance'], reverse=True)
        
        # Mantener solo los mejores modelos
        models_to_keep = self.retention_config['old_models_keep']
        backups_to_keep = self.retention_config['backup_best_models']
        
        for i, model_info in enumerate(sorted_models):
            if i < models_to_keep:
                # Mantener en carpeta principal
                print(f"Manteniendo modelo: {model_info['file']['name']}")
            elif i < backups_to_keep:
                # Mover a backup
                self.move_file_to_backup(model_info['file'], backup_folder_id)
            else:
                # Eliminar modelo
                self.delete_file(model_info['file']['id'])
                print(f"Eliminado modelo: {model_info['file']['name']}")
    
    def load_model_performance_metrics(self):
        """Carga métricas de rendimiento de modelos desde logs"""
        try:
            logs_folder_id = self.get_folder_id(self.folder_structure['logs'])
            log_files = self.list_files_in_folder(logs_folder_id)
            
            # Buscar archivo de métricas
            metrics_file = None
            for file in log_files:
                if 'model_metrics' in file['name']:
                    metrics_file = file
                    break
            
            if metrics_file:
                # Descargar y leer métricas
                request = self.service.files().get_media(fileId=metrics_file['id'])
                file_content = io.BytesIO()
                downloader = MediaIoBaseDownload(file_content, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
                file_content.seek(0)
                metrics = json.load(file_content)
                return metrics
                
        except Exception as e:
            print(f"Error cargando métricas: {e}")
        
        return {}
    
    def move_file_to_backup(self, file_info, backup_folder_id):
        """Mueve archivo a carpeta de backup"""
        try:
            # Actualizar parents del archivo
            file = self.service.files().get(fileId=file_info['id'], fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            
            self.service.files().update(
                fileId=file_info['id'],
                addParents=backup_folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            print(f"Movido a backup: {file_info['name']}")
            
        except Exception as e:
            print(f"Error moviendo archivo: {e}")
    
    def clean_old_compressed_data(self):
        """Elimina datos comprimidos muy antiguos"""
        print("🧹 Limpiando datos comprimidos antiguos...")
        
        compressed_folder_id = self.get_folder_id(self.folder_structure['compressed'])
        files = self.list_files_in_folder(compressed_folder_id)
        
        cutoff_date = datetime.now() - timedelta(days=self.retention_config['compressed_data_months'] * 30)
        
        for file in files:
            file_date = datetime.fromisoformat(file['createdTime'].replace('Z', '+00:00'))
            
            if file_date < cutoff_date:
                self.delete_file(file['id'])
                print(f"Eliminado archivo comprimido antiguo: {file['name']}")
    
    def emergency_cleanup(self):
        """Limpieza de emergencia cuando el espacio está al límite"""
        print("🚨 Ejecutando limpieza de emergencia...")
        
        used_gb, limit_gb = self.get_storage_usage()
        
        if used_gb > self.retention_config['max_storage_gb']:
            # Eliminar datos procesados más antiguos
            processed_folder_id = self.get_folder_id(self.folder_structure['processed_data'])
            files = self.list_files_in_folder(processed_folder_id, 'createdTime')
            
            # Eliminar archivos hasta liberar suficiente espacio
            target_reduction = (used_gb - self.retention_config['max_storage_gb'] + 1) * 1024**3  # +1GB buffer
            freed_space = 0
            
            for file in files:
                if freed_space >= target_reduction:
                    break
                    
                file_size = int(file.get('size', 0))
                if self.delete_file(file['id']):
                    freed_space += file_size
                    print(f"Emergencia - Eliminado: {file['name']} ({file_size/1024**2:.1f}MB)")
    
    def optimize_storage_structure(self):
        """Optimiza la estructura de carpetas y archivos"""
        print("⚡ Optimizando estructura de almacenamiento...")
        
        # Crear todas las carpetas necesarias
        for folder_name in self.folder_structure.values():
            self.get_folder_id(folder_name)
        
        # Mover archivos mal ubicados a sus carpetas correctas
        self.organize_misplaced_files()
    
    def organize_misplaced_files(self):
        """Organiza archivos que están en ubicaciones incorrectas"""
        # Buscar archivos en la raíz que deberían estar en carpetas
        query = "parents in 'root' and name contains 'forex'"
        results = self.service.files().list(q=query).execute()
        root_files = results.get('files', [])
        
        for file in root_files:
            # Determinar carpeta correcta basándose en el nombre
            target_folder = None
            
            if 'forex_data_' in file['name'] and not 'compressed' in file['name']:
                target_folder = self.folder_structure['raw_data']
            elif 'model' in file['name'].lower():
                target_folder = self.folder_structure['models']
            elif 'compressed' in file['name']:
                target_folder = self.folder_structure['compressed']
            
            if target_folder:
                target_folder_id = self.get_folder_id(target_folder)
                self.move_file_to_folder(file, target_folder_id)
    
    def move_file_to_folder(self, file_info, target_folder_id):
        """Mueve archivo a carpeta específica"""
        try:
            # Obtener parents actuales
            file = self.service.files().get(fileId=file_info['id'], fields='parents').execute()
            previous_parents = ",".join(file.get('parents'))
            
            # Mover archivo
            self.service.files().update(
                fileId=file_info['id'],
                addParents=target_folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()
            
            print(f"Organizado: {file_info['name']}")
            
        except Exception as e:
            print(f"Error organizando archivo: {e}")
    
    def generate_storage_report(self):
        """Genera reporte detallado del uso de almacenamiento"""
        print("📊 Generando reporte de almacenamiento...")
        
        used_gb, limit_gb = self.get_storage_usage()
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'storage_usage': {
                'used_gb': used_gb,
                'limit_gb': limit_gb,
                'usage_percentage': (used_gb / limit_gb) * 100
            },
            'folder_sizes': {},
            'file_counts': {},
            'recommendations': []
        }
        
        # Analizar cada carpeta
        for folder_type, folder_name in self.folder_structure.items():
            folder_id = self.get_folder_id(folder_name)
            files = self.list_files_in_folder(folder_id)
            
            total_size = sum(int(f.get('size', 0)) for f in files)
            report['folder_sizes'][folder_type] = total_size / (1024**2)  # MB
            report['file_counts'][folder_type] = len(files)
        
        # Generar recomendaciones
        if used_gb > limit_gb * 0.8:
            report['recommendations'].append("⚠️ Almacenamiento al 80%+ - Ejecutar limpieza")
        
        if report['file_counts']['raw_data'] > 100:
            report['recommendations'].append("📦 Muchos archivos crudos - Comprimir datos antiguos")
        
        if report['file_counts']['models'] > 10:
            report['recommendations'].append("🧠 Muchos modelos - Eliminar versiones antiguas")
        
        # Guardar reporte
        self.save_storage_report(report)
        
        return report
    
    def save_storage_report(self, report):
        """Guarda reporte de almacenamiento"""
        logs_folder_id = self.get_folder_id(self.folder_structure['logs'])
        
        report_content = json.dumps(report, indent=2)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"storage_report_{timestamp}.json"
        
        # Subir reporte
        file_metadata = {
            'name': filename,
            'parents': [logs_folder_id]
        }
        
        media = MediaFileUpload(
            io.StringIO(report_content),
            mimetype='application/json'
        )
        
        self.service.files().create(
            body=file_metadata,
            media_body=media
        ).execute()
        
        print(f"Reporte guardado: {filename}")
    
    def run_full_cleanup(self):
        """Ejecuta limpieza completa del almacenamiento"""
        print("🚀 Iniciando limpieza completa de almacenamiento...")
        
        # 1. Verificar uso actual
        used_gb, limit_gb = self.get_storage_usage()
        print(f"Uso inicial: {used_gb:.2f}GB / {limit_gb:.2f}GB")
        
        # 2. Optimizar estructura
        self.optimize_storage_structure()
        
        # 3. Comprimir datos antiguos
        self.compress_and_archive_data()
        
        # 4. Gestionar modelos
        self.manage_model_versions()
        
        # 5. Limpiar datos comprimidos muy antiguos
        self.clean_old_compressed_data()
        
        # 6. Verificar si necesita limpieza de emergencia
        used_gb_after, _ = self.get_storage_usage()
        if used_gb_after > self.retention_config['max_storage_gb']:
            self.emergency_cleanup()
        
        # 7. Generar reporte final
        final_report = self.generate_storage_report()
        
        # 8. Mostrar resumen
        used_gb_final, _ = self.get_storage_usage()
        space_freed = used_gb - used_gb_final
        
        print(f"\n✅ Limpieza completada!")
        print(f"Espacio liberado: {space_freed:.2f}GB")
        print(f"Uso final: {used_gb_final:.2f}GB / {limit_gb:.2f}GB")
        print(f"Porcentaje usado: {(used_gb_final/limit_gb)*100:.1f}%")
        
        return final_report

# Función principal para ejecutar desde GitHub Actions
def main():
    credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    if not credentials_json:
        print("❌ Error: GOOGLE_DRIVE_CREDENTIALS no encontrado")
        return
    
    manager = IntelligentStorageManager(credentials_json)
    
    # Ejecutar limpieza completa
    report = manager.run_full_cleanup()
    
    # Si el uso sigue siendo alto, enviar alerta
    if report['storage_usage']['usage_percentage'] > 90:
        print("🚨 ALERTA: Uso de almacenamiento crítico!")
        # Aquí podrías enviar una notificación por email o Slack

if __name__ == "__main__":
    main()
