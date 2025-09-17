# scripts/setup_storage_structure.py
import os
import json
from storage_manager import IntelligentStorageManager

def setup_initial_structure():
    """Configura la estructura inicial de carpetas"""
    credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
    manager = IntelligentStorageManager(credentials_json)
    
    print("🏗️ Configurando estructura inicial...")
    
    # Crear todas las carpetas
    folder_ids = {}
    for folder_type, folder_name in manager.folder_structure.items():
        folder_id = manager.get_folder_id(folder_name)
        folder_ids[folder_type] = folder_id
        print(f"✅ Carpeta creada/verificada: {folder_name}")
    
    # Crear archivo de configuración
    config = {
        'folder_ids': folder_ids,
        'retention_config': manager.retention_config,
        'setup_date': datetime.now().isoformat()
    }
    
    # Guardar configuración
    with open('storage_config.json', 'w') as f:
        json.dump(config, f, indent=2)
    
    print("✅ Estructura inicial configurada correctamente!")
    
    # Generar reporte inicial
    manager.generate_storage_report()

if __name__ == "__main__":
    setup_initial_structure()
