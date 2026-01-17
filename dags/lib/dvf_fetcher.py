import os
import requests
from datetime import datetime

def fetch_dvf_data(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    current_day = datetime.now().strftime("%Y%m%d")
    
    target_folder = os.path.join(DATALAKE_ROOT_FOLDER, "raw", "gov")
    
    if not os.path.exists(target_folder):
        os.makedirs(target_folder, exist_ok=True)
        
    url = 'https://files.data.gouv.fr/geo-dvf/latest/csv/2025/full.csv.gz'
    filename = 'dvf_2025_full.csv.gz' 
    target_path = os.path.join(target_folder, filename)
    
    if os.path.exists(target_path) and os.path.getsize(target_path) > 0:
        print(f"Fichier déjà présent : {target_path}")
        return target_path

    print(f"Téléchargement de {url} vers {target_path} ...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(target_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Téléchargement réussi.")
    except Exception as e:
        if os.path.exists(target_path):
            os.remove(target_path)
        raise e

    return target_path