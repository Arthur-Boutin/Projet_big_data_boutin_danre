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

def index_lbc_to_es(**kwargs):
    # Renamed: index_opportunities_to_es effectively
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    # NEW SOURCE: USAGE / OPPORTUNITIES
    parquet_file = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "opportunities", current_day)
    # Spark écrit un dossier Parquet (avec des fichiers part-*.parquet). Pandas ne lit pas un dossier directement sauf avec engine='pyarrow' récent ou glob.
    # Hack : On cherche le fichier parquet dans le dossier
    
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
    
    # Préparation des documents
    documents = []
    for _, row in df.iterrows():
        doc = row.to_dict()
        
        # Index Name Changed
        index_name = "usage-opportunities"
        
        # Cleanup types NumPy pour ES (int64 -> int, etc)
        # ... (Simplifié ici)
        
        action = {
            "_index": index_name,
            # "_id": ... # Si dispo
            "_source": doc
        }
        documents.append(action)

    if documents:
        print(f"Indexation de {len(documents)} docs dans {index_name}...")
        success, failed = helpers.bulk(es, documents, stats_only=True)
        print(f"✅ Indexés: {success}")

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
    for i, row in df.iterrows():
        doc = row.to_dict()
        
        action = {
            "_index": "usage-market-stats",
            "_source": doc
        }
        documents.append(action)

        if len(documents) >= 5000:
            helpers.bulk(es, documents)
            documents = []

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

    print(f"Lecture Formatted DVF (Paris Filter on indexing or index all? Index All for now): {parquet_file}")
    df = pd.read_parquet(parquet_file)
    
    # Optional: Filter Paris here too if we only want Paris in dashboard comparison
    # df = df[df['code_commune'].astype(str).str.startswith('75')]

    documents = []
    for i, row in df.iterrows():
        doc = row.to_dict()
        doc = {k: v for k, v in doc.items() if pd.notnull(v)}
        
        doc_id = doc.get('id_mutation', f"dvf_raw_{i}")

        if 'latitude' in doc and 'longitude' in doc:
             doc['pin'] = {
                 "location": {
                     "lat": float(doc['latitude']),
                     "lon": float(doc['longitude'])
                 }
             }

        action = {
            "_index": "gov-dvf",
            "_id": str(doc_id),
            "_source": doc
        }
        documents.append(action)

        if len(documents) >= 5000:
            helpers.bulk(es, documents)
            documents = []

    if documents:
        helpers.bulk(es, documents)
        print("✅ Fin indexation Raw DVF.")
