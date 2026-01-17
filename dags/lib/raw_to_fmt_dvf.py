import os
import pandas as pd
from datetime import datetime

def convert_dvf_to_parquet(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    current_day = datetime.now().strftime("%Y%m%d")
    
    raw_path = os.path.join(DATALAKE_ROOT_FOLDER, "raw", "gov", "dvf_2025_full.csv.gz")
    
    formatted_folder = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "gov")
    target_file = os.path.join(formatted_folder, "dvf_2025_cleaned.parquet")

    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder, exist_ok=True)

    if os.path.exists(target_file):
        print(f"✅ Fichier Parquet déjà existant ({target_file}). Skipping transformation.")
        return

    print(f"Lecture du fichier RAW : {raw_path} ...")
    
    try:
        df = pd.read_csv(raw_path, compression='gzip', sep=',', low_memory=False)
        
        if 'date_mutation' in df.columns:
            df['date_mutation'] = pd.to_datetime(df['date_mutation'], errors='coerce').astype('datetime64[us]')

        print(f"Écriture du Parquet : {target_file} ...")
        df.to_parquet(target_file, compression='snappy')
        print("Transformation terminée !")
        
    except Exception as e:
        print(f"Erreur : {e}")
        raise e