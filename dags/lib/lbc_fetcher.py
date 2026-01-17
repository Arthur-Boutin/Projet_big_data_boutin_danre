import os
import sys
import json
import logging
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
dags_root = os.path.dirname(current_dir)
lbc_repo_path = os.path.join(dags_root, 'lbc')

if lbc_repo_path not in sys.path:
    sys.path.insert(0, lbc_repo_path)

try:
    import curl_cffi
except ImportError:
    print("⚠️ curl_cffi manquant. Installation automatique...")
    import subprocess
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "curl_cffi==0.11.3"])
        print("✅ curl_cffi installé.")
    except Exception as e:
        print(f"❌ Echec de l'installation automatique de curl_cffi: {e}")

try:
    import lbc
except ImportError as e:
    print(f"❌ Erreur import lbc: {e}")
    lbc = None

def fetch_lbc_data(**kwargs):
    if lbc is None:
        raise ImportError("Le module 'lbc' n'est pas installé ou introuvable.")
    
    LEBONCOIN_URL = "https://www.leboncoin.fr/recherche?category=9&locations=Paris__48.86023250788424_2.339006433295173_9256&real_estate_type=1,2,3,4&sort=time&order=desc" 

    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    
    current_day = datetime.now().strftime("%Y%m%d")
    current_time = datetime.now().strftime("%H%M%S")
    
    state_folder = os.path.join(DATALAKE_ROOT_FOLDER, "state")
    if not os.path.exists(state_folder):
        os.makedirs(state_folder, exist_ok=True)
    state_file = os.path.join(state_folder, "lbc_state.json")
    
    last_fetched_date = None
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                state_data = json.load(f)
                if 'last_fetched' in state_data:
                    last_fetched_date = datetime.fromisoformat(state_data['last_fetched'])
                    print(f"ℹ️ Dernière récupération : {last_fetched_date}")
        except Exception as e:
            print(f"⚠️ Erreur lecture state : {e}")

    target_folder = os.path.join(DATALAKE_ROOT_FOLDER, "raw", "leboncoin", "annonces", current_day)
    
    if not os.path.exists(target_folder):
        os.makedirs(target_folder, exist_ok=True)
    
    target_file = os.path.join(target_folder, f"annonces_lbc_{current_time}.json")
    
    print(f"🚀 Démarrage de l'extraction LBC via URL : {LEBONCOIN_URL}")
    
    try:
        client = lbc.Client()

        result = client.search(
            url=LEBONCOIN_URL,
            page=1,
            limit=35 
        )

        ads_data = []
        new_max_date = last_fetched_date
        
        for i, ad in enumerate(result.ads):
            if i == 0:
                print(f"✅ Première annonce trouvée : {getattr(ad, 'subject', 'Sans titre')} - {getattr(ad, 'price', 'N/A')}€")
            
            ad_date_str = getattr(ad, 'first_publication_date', None)
            ad_date = None
            if ad_date_str:
                try:
                    ad_date = datetime.fromisoformat(ad_date_str)
                except ValueError:
                    ad_date = datetime.now() 
            
            if last_fetched_date and ad_date and ad_date <= last_fetched_date:
                print(f"🛑 Annonce du {ad_date} déjà récupérée (Last: {last_fetched_date}). Arrêt du traitement.")
                break
            
            if ad_date:
                if new_max_date is None or ad_date > new_max_date:
                    new_max_date = ad_date

            loc_data = "N/A"
            if hasattr(ad, 'location'):
                loc_obj = ad.location
                loc_data = {
                    "city": getattr(loc_obj, 'city', 'N/A'),
                    "zipcode": getattr(loc_obj, 'zipcode', 'N/A'),
                    "lat": getattr(loc_obj, 'lat', 0),
                    "lng": getattr(loc_obj, 'lng', 0)
                }

            attributes_dict = {}
            if hasattr(ad, 'attributes') and isinstance(ad.attributes, list):
                for attr in ad.attributes:
                    if hasattr(attr, 'key') and hasattr(attr, 'value'):
                        attributes_dict[attr.key] = attr.value

            ad_info = {
                "id": getattr(ad, 'list_id', getattr(ad, 'id', 'N/A')),
                "title": getattr(ad, 'subject', getattr(ad, 'title', 'Titre Inconnu')),
                "price": getattr(ad, 'price', [0])[0] if isinstance(getattr(ad, 'price', 0), list) else getattr(ad, 'price', 0),
                "url": getattr(ad, 'url', 'N/A'),
                "date": ad_date_str or datetime.now().isoformat(),
                "location": loc_data, 
                "attributes": attributes_dict 
            }
            ads_data.append(ad_info)

        if ads_data:
            with open(target_file, "w", encoding="utf-8") as f:
                json.dump(ads_data, f, indent=4, ensure_ascii=False)
            
            print(f"\n🎉 SUCCÈS ! {len(ads_data)} nouvelles annonces sauvegardées.")
            print(f"📁 Fichier : {target_file}")
            
            if new_max_date:
                with open(state_file, 'w') as f:
                    json.dump({'last_fetched': new_max_date.isoformat()}, f)
                print(f"💾 État mis à jour : {new_max_date}")

            return target_file
        else:
            print("⚠️ Aucune nouvelle annonce trouvée.")
            return None

    except Exception as e:
        print(f"\n❌ ERREUR LBC FETCH: {e}")
        raise e
