import os
import sys
import shutil
from datetime import datetime

# --- IMPORTS ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, when, year

def compute_usage_layer(**kwargs):
    # --- CHEMINS ---
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    # Inputs
    dvf_path = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "gov", "dvf_2025_cleaned.parquet")
    lbc_path = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "leboncoin", "annonces", current_day, "annonces_cleaned.parquet")
    
    # Check Inputs
    if not os.path.exists(dvf_path):
        print("Skipping Usage: DVF data not found.")
        return
    if not os.path.exists(lbc_path):
        print("Skipping Usage: LBC data not found.")
        return

    # Outputs
    usage_market_path = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "market_analysis")
    usage_opp_path = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "opportunities", current_day)
    
    # Init Spark
    spark = SparkSession.builder \
        .appName("Immo_Usage_Layer") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        print("📊 Chargement DVF...")
        df_dvf = spark.read.parquet(dvf_path)
        
        # --- 1. MARKET ANALYSIS (DVF) ---
        # Calcul du prix moyen au m² par commune
        # Filtre basique : Maison/Appart, Vente
        # On suppose les colonnes : 'nature_mutation', 'type_local', 'valeur_fonciere', 'surface_reelle_bati', 'code_commune'
        
        # Adaptation schéma (à vérifier selon raw_to_fmt_dvf)
        # Si colonnes manquantes, on fait au mieux
        
        has_cols = lambda df, cols: all([c in df.columns for c in cols])
        required_dvf = ['valeur_fonciere', 'surface_reelle_bati', 'code_commune']
        
        if has_cols(df_dvf, required_dvf):
            print("📈 Calcul Market Analysis (Paris Only)...")
            
            # Filter Paris (75) + Basic filters
            df_cal = df_dvf.filter(
                (col("code_commune").startswith("75")) &
                (col("valeur_fonciere").isNotNull()) & 
                (col("surface_reelle_bati") > 9) & 
                (col("valeur_fonciere") > 1000)
            )
            
            df_cal = df_cal.withColumn(
                "prix_m2", 
                col("valeur_fonciere") / col("surface_reelle_bati")
            )
            
            # Aggrégation par commune
            market_stats = df_cal.groupby("code_commune").agg(
                avg("prix_m2").alias("avg_price_m2_commune"),
                count("*").alias("nb_ventes")
            )
            
            # Sauvegarde Market Analysis (Overwrite mode "complete")
            # En local Spark crée des dossier part-xxx. On veut peut-être un fichier unique ou un dossier propre.
            # Pour Airflow local, save direct.
            market_stats.write.mode("overwrite").parquet(usage_market_path)
            print(f"✅ Market Analysis sauvegardé : {usage_market_path}")
            
        else:
            print(f"⚠️ Colonnes manquantes dans DVF pour analyse m2: {df_dvf.columns}")
            market_stats = None

        # --- 2. OPPORTUNITIES (LBC) ---
        print("💎 Chargement LBC & Recherche Opportunités...")
        df_lbc = spark.read.parquet(lbc_path)
        
        # On a besoin du code_commune (zipcode) dans LBC pour joindre
        # LBC location est un struct ou dict. PySpark lit Parquet struct normalement.
        # location.zipcode
        
        # Aplatir location si struct
        if "location" in df_lbc.columns:
            # Si c'est un StructType
            # df_lbc = df_lbc.select("*", "location.zipcode", "location.city")
            # Mais si c'est stocké en Map ou String (JSON), c'est plus dur.
            # Supposons que raw_to_fmt a laissé Pandas écrire. Parquet Pandas -> Spark struct souvent ok.
            pass

        # Pour simplifier le mapping Zip -> Code Commune DVF (souvent 1-1 ou proche)
        # On extrait le code postal.
        # df_lbc schema check
        
        # Calcul prix m2 LBC (si surface dispo)
        # Souvent surface est dans 'attributes' (liste ou map).
        # C'est complexe à extraire en SQL pur si format variable.
        # Pour ce POC, on simule ou on tente d'extraire si colonne dédiée.
        
        # Si pas de colonne surface explicite, on ne peut pas calculer le m2.
        # On va joindre juste pour avoir le prix moyen de la ville à côté du prix de l'annonce.
        
        # Extraction Zip
        # On suppose que 'location' a un champ 'zipcode'
        if market_stats:
            # Extraction zip from location column (struct)
            df_lbc_aug = df_lbc.withColumn("zip", col("location.zipcode"))
            
            # Join
            df_opp = df_lbc_aug.join(market_stats, df_lbc_aug.zip == market_stats.code_commune, "left")
            
            # Detect good deals?
            # Si on a la surface, on compare prix_m2 LBC < avg_price_m2_commune
            # Sinon, on marque juste l'info.
            
            # Sauvegarde
            df_opp.write.mode("overwrite").parquet(usage_opp_path)
            print(f"✅ Opportunités sauvegardées : {usage_opp_path}")
            
    except Exception as e:
        print(f"❌ Erreur Spark: {e}")
        raise e
    finally:
        spark.stop()
