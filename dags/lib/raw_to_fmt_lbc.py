import os
import pandas as pd
import json
from datetime import datetime

def convert_lbc_to_parquet(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    current_day = datetime.now().strftime("%Y%m%d")
    
    raw_folder = os.path.join(DATALAKE_ROOT_FOLDER, "raw", "leboncoin", "annonces", current_day)
    
    if not os.path.exists(raw_folder):
        print(f"⚠️ Dossier source introuvable : {raw_folder}")
        return

    formatted_folder = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "leboncoin", "annonces", current_day)
    target_file = os.path.join(formatted_folder, "annonces_cleaned.parquet")

    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder, exist_ok=True)

    json_files = [os.path.join(raw_folder, f) for f in os.listdir(raw_folder) if f.endswith('.json')]
    
    if not json_files:
        print("⚠️ Aucun fichier JSON trouvé pour aujourd'hui.")
        return

    print(f"Lecture des {len(json_files)} fichiers RAW ...")
    
    try:
        all_data = []
        for jf in json_files:
            try:
                with open(jf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data:
                        all_data.extend(data)
            except Exception as e:
                print(f"⚠️ Erreur lecture {jf}: {e}")

        if not all_data:
            print("Aucune donnée trouvée dans les fichiers JSON.")
            return

        df = pd.DataFrame(all_data)
        
        if 'id' in df.columns:
            df.drop_duplicates(subset=['id'], keep='last', inplace=True)
            print(f"Deduplication: {len(df)} annonces uniques.")
        
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        
        df['date'] = pd.to_datetime(df['date'], errors='coerce').astype('datetime64[us]')

        print(f"Écriture du Parquet : {target_file} ...")
        df.to_parquet(target_file, compression='snappy')
        
        print(f"✅ Transformation terminée ! {len(df)} lignes écrites.")
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation : {e}")
        raise e
