import os
import pandas as pd
import json
from datetime import datetime

def convert_lbc_to_parquet(**kwargs):
    # --- CHEMINS ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    current_day = datetime.now().strftime("%Y%m%d")
    
    # Entrée : Fichier RAW JSON -> On scanne TOUT le dossier
    raw_folder = os.path.join(DATALAKE_ROOT_FOLDER, "raw", "leboncoin", "annonces", current_day)
    
    if not os.path.exists(raw_folder):
        print(f"⚠️ Dossier source introuvable : {raw_folder}")
        return

    # Sortie : Fichier Formatted Parquet
    formatted_folder = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "leboncoin", "annonces", current_day)
    target_file = os.path.join(formatted_folder, "annonces_cleaned.parquet")

    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder, exist_ok=True)

    # Glob tous les .json
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

        # Conversion en DataFrame
        df = pd.DataFrame(all_data)
        
        # Deduplication massive sur ID
        if 'id' in df.columns:
            df.drop_duplicates(subset=['id'], keep='last', inplace=True)
            print(f"Deduplication: {len(df)} annonces uniques.")
        
        # Nettoyage basique / Typage
        # 'price' devrait être float/int
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        
        # 'date' en datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Pour les colonnes complexes 'location' et 'attributes' (dicts),
        # Parquet gère les structs, mais parfois cela complique la lecture sans schéma strict.
        # Une option simple est de les laisser telles quelles (Pandas les gère comme 'object' -> Parquet les gère souvent mais attention à la compatibilité)
        # Ou de les 'aplatir' (normalize). 
        # Pour l'instant, on laisse Pandas gérer la sérialisation Parquet par défaut.
        # Si erreur, on peut les convertir en string JSON :
        # df['location'] = df['location'].apply(json.dumps)
        
        print(f"Écriture du Parquet : {target_file} ...")
        # compression='snappy' est standard et performant
        df.to_parquet(target_file, compression='snappy')
        
        print(f"✅ Transformation terminée ! {len(df)} lignes écrites.")
        
    except Exception as e:
        print(f"❌ Erreur lors de la transformation : {e}")
        raise e
