import os
import sys
import pandas as pd
from datetime import datetime
import json

ES_HOST = "http://elasticsearch:9200"

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
        es = Elasticsearch(ES_HOST, verify_certs=False)
        if es.ping():
            print(f"✅ Connecté à Elasticsearch: {ES_HOST}")
            return es
        else:
            print(f"❌ Impossible de pinger Elasticsearch à {ES_HOST}")
            es_local = Elasticsearch("http://localhost:9200", verify_certs=False)
            if es_local.ping():
                print("✅ Connecté via localhost (Fallback)")
                return es_local
            else:
                return None
    except Exception as e:
        print(f"❌ Erreur connexion ES: {e}")
        return None

def ensure_dvf_mapping(es, index_name):
    if not es.indices.exists(index=index_name):
        mapping = {
            "mappings": {
                "properties": {
                    "pin": {
                        "properties": {
                            "location": {"type": "geo_point"}
                        }
                    },
                    "valeur_fonciere": {"type": "float"},
                    "surface_reelle_bati": {"type": "integer"},
                    "date_mutation": {"type": "date"}
                }
            }
        }
        es.indices.create(index=index_name, body=mapping)
        print(f"✅ Index {index_name} créé avec mapping GeoPoint.")
    else:
        pass

import numpy as np

def clean_doc(doc):
    cleaned = {}
    for k, v in doc.items():
        if pd.isna(v):
            continue
            
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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "opportunities", current_day)
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de dossier Usage Opportunités : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    print(f"Lecture Usage LBC: {parquet_file}")
    
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
    
    documents = []
    index_name = "usage-opportunities"
    
    print(f"Début indexation dans {index_name}...")
    
    count_ok = 0
    count_err = 0
    
    for i, row in df.iterrows():
        try:
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            action = {
                "_index": index_name,
                "_source": doc
            }
            documents.append(action)
            
            if len(documents) >= 1000:
                success, failed = helpers.bulk(es, documents, stats_only=True)
                count_ok += success
                input_len = len(documents)
                documents = []
                print(f"Batch processed: +{success} (failed: {input_len - success})")
                
        except Exception as e:
            print(f"❌ Erreur sur ligne {i}: {e}")
            count_err += 1

    if documents:
        success, failed = helpers.bulk(es, documents, stats_only=True)
        count_ok += success
        print(f"Final batch: +{success}")

    print(f"✅ Fin indexation Opportunities. Total OK: {count_ok}, Erreurs locales: {count_err}")

def index_dvf_to_es(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "gov", "dvf_2025_cleaned.parquet")
    
    if not os.path.exists(parquet_file):
        print(f"⚠️ Pas de fichier Parquet DVF : {parquet_file}")
        return

    es = connect_es()
    if not es:
        raise Exception("Elasticsearch non joignable")

    ensure_dvf_mapping(es, "gov-dvf")
    ensure_dvf_mapping(es, "gov-dvf-paris")

    print(f"Lecture Formatted DVF: {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    documents = []
    print(f"Début indexation Raw DVF (avec filtres ML)...")
    
    count_filtered = 0
    total_processed = 0

    for i, row in df.iterrows():
        try:
            total_processed += 1
            raw_doc = row.to_dict()
            doc = clean_doc(raw_doc)
            
            type_local = doc.get('type_local', '')
            if type_local not in ['Appartement', 'Maison']:
                count_filtered += 1
                continue
            
            valeur = doc.get('valeur_fonciere')
            if valeur is None or valeur < 5000 or valeur > 50000000:
                count_filtered += 1
                continue
                
            surface = doc.get('surface_reelle_bati')
            if surface is None or surface < 9 or surface > 10000:
                count_filtered += 1
                continue

            if 'latitude' not in doc or 'longitude' not in doc or pd.isna(doc['latitude']) or pd.isna(doc['longitude']):
                count_filtered += 1
                continue

            doc_id = doc.get('id_mutation', f"dvf_raw_{i}")

            doc['pin'] = {
                "location": {
                    "lat": float(doc['latitude']),
                    "lon": float(doc['longitude'])
                }
            }

            action_global = {
                "_index": "gov-dvf",
                "_id": str(doc_id),
                "_source": doc
            }
            documents.append(action_global)

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
    
    print(f"✅ Fin indexation Raw DVF. Total: {total_processed}, Filtrés: {count_filtered}, Indexés: {total_processed - count_filtered}")


def index_lbc_raw_to_es(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
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
