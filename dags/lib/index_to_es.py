import os
import sys
import pandas as pd
from datetime import datetime
import json

# --- CONFIGURATION ES ---
# On tente d'utiliser le nom du service Docker "elasticsearch" qui est résolu dans le réseau Docker
ES_HOST = "http://elasticsearch:9200"

# --- AUTO-INSTALL DEPENDENCY ---
try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("⚠️ 'elasticsearch' manquant. Installation automatique...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "elasticsearch==8.11.0"])
        from elasticsearch import Elasticsearch, helpers
        print("✅ elasticsearch installé.")
    except Exception as e:
        print(f"❌ Echec installation elasticsearch: {e}")
        raise e

def connect_es():
    try:
        # On désactive verify_certs car on a désactivé la sécurité SSL dans le docker-compose pour le dev
        es = Elasticsearch(ES_HOST, verify_certs=False)
        if es.ping():
            print(f"✅ Connecté à Elasticsearch: {ES_HOST}")
            return es
        else:
            print(f"❌ Impossible de pinger Elasticsearch à {ES_HOST}")
            # Fallback pour le dev local hors conteneur
            es_local = Elasticsearch("http://localhost:9200", verify_certs=False)
            if es_local.ping():
                print("✅ Connecté via localhost (Fallback)")
                return es_local
            else:
                return None
    except Exception as e:
        print(f"❌ Erreur connexion ES: {e}")
        return None

import numpy as np

def clean_doc(doc):
    """Nettoie le document pour Elasticsearch (compatibilité types NumPy, NaNs)."""
    cleaned = {}
    for k, v in doc.items():
        # Gestion NaNs / None
        if pd.isna(v):
            continue
            
        # Conversion types NumPy
        if isinstance(v, (np.int64, np.int32, np.int16, np.int8)):
            cleaned[k] = int(v)
        elif isinstance(v, (np.float64, np.float32)):
            cleaned[k] = float(v)
        elif isinstance(v, np.ndarray):
            cleaned[k] = v.tolist()
        elif isinstance(v, datetime):
            cleaned[k] = v.isoformat()
        else:
            cleaned[k] = v
    return cleaned

def index_lbc_to_es(**kwargs):
    # Renamed: index_opportunities_to_es effectively
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    # NEW SOURCE: USAGE / OPPORTUNITIES
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "opportunities", current_day)
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de dossier Usage Opportunités : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    print(f"Lecture Usage LBC: {parquet_file}")
    
    # Lecture Dossier Parquet
    try:
        df = pd.read_parquet(parquet_file, engine='pyarrow')
    except:
        print("Erreur lecture dossier parquet standard. Tentative glob.")
        import glob
        files = glob.glob(os.path.join(parquet_file, "*.parquet"))
        if not files:
             print("Aucun fichier .parquet trouvé.")
             return
        df = pd.concat([pd.read_parquet(f) for f in files])
    
    # Préparation des documents avec Batching
    documents = []
    index_name = "usage-opportunities"
    
    print(f"Début indexation dans {index_name}...")
    
    count_ok = 0
    count_err = 0
    
    for i, row in df.iterrows():
        try:
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            # Optional: Add ID if available
            # _id = str(doc['id']) if 'id' in doc else None
            
            action = {
                "_index": index_name,
                "_source": doc
            }
            # if _id: action["_id"] = _id
            
            documents.append(action)
            
            # Batch size 1000
            if len(documents) >= 1000:
                success, failed = helpers.bulk(es, documents, stats_only=True)
                count_ok += success
                input_len = len(documents)
                documents = []  # Reset batch
                print(f"Batch processed: +{success} (failed: {input_len - success})")
                
        except Exception as e:
            print(f"❌ Erreur sur ligne {i}: {e}")
            count_err += 1

    # Final batch
    if documents:
        success, failed = helpers.bulk(es, documents, stats_only=True)
        count_ok += success
        print(f"Final batch: +{success}")

    print(f"✅ Fin indexation Opportunities. Total OK: {count_ok}, Erreurs locales: {count_err}")

def index_dvf_to_es(**kwargs):
    # Renamed: index_market_stats_to_es
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    # NEW SOURCE: USAGE / MARKET ANALYSIS
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "market_analysis")
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de dossier Usage Market : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    print(f"Lecture Usage Market: {parquet_file}")
    
    try:
        df = pd.read_parquet(parquet_file, engine='pyarrow')
    except:
        import glob
        files = glob.glob(os.path.join(parquet_file, "*.parquet"))
        if not files: return
        df = pd.concat([pd.read_parquet(f) for f in files])

    documents = []
    print(f"Début indexation Market Stats...")
    
    for i, row in df.iterrows():
        try:
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            action = {
                "_index": "usage-market-stats",
                "_source": doc
            }
            documents.append(action)

            if len(documents) >= 5000:
                helpers.bulk(es, documents)
                print(f"Market Stats Batch: {len(documents)}")
                documents = []
        except Exception as e:
            print(f"Skipping market stat row {i}: {e}")

    if documents:
        helpers.bulk(es, documents)
        print("✅ Fin indexation Market Stats.")

def index_formatted_dvf_to_es(**kwargs):
    # Restore: Index raw/formatted DVF for granular comparison
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "gov", "dvf_2025_cleaned.parquet")
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de fichier Parquet DVF : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    print(f"Lecture Formatted DVF: {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    documents = []
    print(f"Début indexation Raw DVF (avec filtre Paris)...")
    
    for i, row in df.iterrows():
        try:
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            doc_id = doc.get('id_mutation', f"dvf_raw_{i}")

            if 'latitude' in doc and 'longitude' in doc:
                 doc['pin'] = {
                     "location": {
                         "lat": float(doc['latitude']),
                         "lon": float(doc['longitude'])
                     }
                 }

            # 1. Index dans global gov-dvf
            action_global = {
                "_index": "gov-dvf",
                "_id": str(doc_id),
                "_source": doc
            }
            documents.append(action_global)

            # 2. Si Paris (75...), index dans gov-dvf-paris
            code = str(doc.get('code_commune', ''))
            if code.startswith('75'):
                action_paris = {
                    "_index": "gov-dvf-paris",
                    "_id": str(doc_id),
                    "_source": doc
                }
                documents.append(action_paris)

            if len(documents) >= 5000:
                helpers.bulk(es, documents)
                print(f"Raw DVF Batch: {len(documents)}")
                documents = []
                
        except Exception as e:
            print(f"Skipping raw dvf row {i}: {e}")

    if documents:
        helpers.bulk(es, documents)
        print("✅ Fin indexation Raw DVF.")

def index_lbc_raw_to_es(**kwargs):
    # Index raw Leboncoin ads for comparison
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    # Path to LBC Raw/Formatted data
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "leboncoin", "annonces", current_day, "annonces_cleaned.parquet")
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de dossier LBC Formatted : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    print(f"Lecture LBC Raw: {parquet_file}")
    
    try:
        df = pd.read_parquet(parquet_file, engine='pyarrow')
    except:
        import glob
        files = glob.glob(os.path.join(parquet_file, "*.parquet"))
        if not files:
             print("Aucun fichier .parquet trouvé.")
             return
        df = pd.concat([pd.read_parquet(f) for f in files])

    documents = []
    print(f"Début indexation LBC Raw...")
    
    for i, row in df.iterrows():
        try:
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            # Use LBC ID if available
            lbc_id = doc.get('id')
            
            action = {
                "_index": "lbc-annonces",
                "_source": doc
            }
            if lbc_id:
                action["_id"] = str(lbc_id)
            
            documents.append(action)

            if len(documents) >= 1000:
                helpers.bulk(es, documents)
                print(f"LBC Raw Batch: {len(documents)}")
                documents = []
        
        except Exception as e:
            print(f"Skipping LBC Raw row {i}: {e}")

    if documents:
        helpers.bulk(es, documents)
        print("✅ Fin indexation LBC Raw.")
