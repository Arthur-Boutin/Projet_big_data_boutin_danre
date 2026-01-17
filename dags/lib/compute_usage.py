import os
import sys
import shutil
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, when, year

def compute_usage_layer(**kwargs):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    DATALAKE_ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, '..', '..', 'Datalake'))
    current_day = datetime.now().strftime("%Y%m%d")
    
    dvf_path = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "gov", "dvf_2025_cleaned.parquet")
    lbc_path = os.path.join(DATALAKE_ROOT_FOLDER, "formatted", "leboncoin", "annonces", current_day, "annonces_cleaned.parquet")
    
    if not os.path.exists(dvf_path):
        print("Traitement annulé : données DVF introuvables.")
        return
    if not os.path.exists(lbc_path):
        print("Traitement annulé : données Leboncoin introuvables.")
        return

    usage_market_path = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "market_analysis")
    usage_opp_path = os.path.join(DATALAKE_ROOT_FOLDER, "usage", "opportunities", current_day)
    
    spark = SparkSession.builder \
        .appName("Immo_Usage_Layer") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        print("Chargement des données DVF en cours...")
        df_dvf = spark.read.parquet(dvf_path)
        
        has_cols = lambda df, cols: all([c in df.columns for c in cols])
        required_dvf = ['valeur_fonciere', 'surface_reelle_bati', 'code_commune']
        
        if has_cols(df_dvf, required_dvf):
            print("Calcul de l'analyse de marché (Paris uniquement) en cours...")
            
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
            
            market_stats = df_cal.groupby("code_commune").agg(
                avg("prix_m2").alias("avg_price_m2_commune"),
                count("*").alias("nb_ventes")
            )
            
            market_stats.write.mode("overwrite").parquet(usage_market_path)
            print(f"Analyse de marché sauvegardée : {usage_market_path}")
            
        else:
            print(f"Colonnes manquantes dans DVF pour l'analyse au m2 : {df_dvf.columns}")
            market_stats = None

        print("Chargement des données Leboncoin et recherche d'opportunités en cours...")
        df_lbc = spark.read.parquet(lbc_path)
        
        if "location" in df_lbc.columns:
            pass
        
        if market_stats:
            df_lbc_aug = df_lbc.withColumn("zip", col("location.zipcode"))
            
            df_opp = df_lbc_aug.join(market_stats, df_lbc_aug.zip == market_stats.code_commune, "left")
            
            df_opp.write.mode("overwrite").parquet(usage_opp_path)
            print(f"Opportunités sauvegardées : {usage_opp_path}")
            
    except Exception as e:
        print(f"Erreur Spark : {e}")
        raise e
    finally:
        spark.stop()
