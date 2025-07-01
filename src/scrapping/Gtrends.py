from pyspark.sql.functions import col, year, month, dayofmonth
from datetime import datetime, timedelta
from pytrends.request import TrendReq
from pyspark.sql import SparkSession
from calendar import monthrange
import random
import time
import json

output_path = "Tp_final_BD/datas/bronze/gtrends"

keywords_techs = [
    # Langages de programmation populaires
    'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
    
    # Frameworks et bibliothèques web
    'react', 'angular', 'vue.js', 'django', 'flask', 'express.js', 'spring boot', 
    'laravel', 'rails', 'asp.net', 'nextjs', 'nuxt', 'svelte', 'fastapi',
]

country_codes = [
   # Très développés (IDH > 0.9)
   "FR","NO", "CH", "IE", "DE", "HK", "AU", "IS", "SE", "DK", "NL",
   "FI", "SG", "BE", "NZ", "CA", "LI", "LU", "GB", "JP", "KR",
   "US", "IL", "MT", "SI", "AT", "ES", "FR", "CY", "IT", "EE",

   # Développés (IDH 0.8-0.9)
   "CZ", "GR", "PL", "LT", "PT", "LV", "AD", "SK", "HR", "CL",
   "QA", "BH", "AE", "BN", "MY", "UY", "PA", "AR", "RO", "KW",
   "KZ", "RU", "BY", "BG", "MN", "ME", "MX", "CR", "CU", "TR"
]

def chunk_keywords(keywords, chunk_size=5):
    """Divise la liste de mots-clés en chunks de taille maximale chunk_size"""
    for i in range(0, len(keywords), chunk_size):
        yield keywords[i:i + chunk_size]

def create_spark_session():
    spark = SparkSession.builder \
        .appName("US_Accidents_US_Accidents") \
        .config("spark.driver.memory", "8g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
        .config("spark.hadoop.io.native.lib.available", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    return spark

def get_trends(keywords_techs, country_codes, start_date, end_date):
    pytrend = TrendReq(hl='fr', tz=360)
    all_data = []
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    keyword_chunks = list(chunk_keywords(keywords_techs, 5))
    total_chunks = len(keyword_chunks)
    
    print(f"Total de mots-clés: {len(keywords_techs)}")
    print(f"Nombre de chunks: {total_chunks}")
    print(f"Chunks: {[len(chunk) for chunk in keyword_chunks]}")
    current_date = start_date.replace(day=1)  # Premier jour du mois
    total_months = 0
    temp_date = current_date
    while temp_date <= end_date:
        total_months += 1
        if temp_date.month == 12:
            temp_date = temp_date.replace(year=temp_date.year + 1, month=1)
        else:
            temp_date = temp_date.replace(month=temp_date.month + 1)
    month_counter = 0
    current_date = start_date.replace(day=1)  # Reset
    
    while current_date <= end_date:
        month_counter += 1
        last_day = monthrange(current_date.year, current_date.month)[1]
        month_end = current_date.replace(day=last_day)
        if month_end > end_date:
            month_end = end_date
            
        start_str = current_date.strftime('%Y-%m-%d')
        end_str = month_end.strftime('%Y-%m-%d')
        plage_date = f"{start_str} {end_str}"
        
        print(f"\n=== MOIS {month_counter}/{total_months}: {current_date.strftime('%B %Y')} ===")
        print(f"Période: {plage_date}")
        for chunk_idx, keyword_chunk in enumerate(keyword_chunks, 1):
            print(f"\n--- Chunk {chunk_idx}/{total_chunks}: {keyword_chunk} ---")
            
            for country_idx, country_code in enumerate(country_codes, 1):
                try:
                    print(f"Recherche chunk {chunk_idx} pour {country_code} ({country_idx}/{len(country_codes)})")
                    pytrend.build_payload(keyword_chunk, cat=0, timeframe=plage_date, geo=country_code, gprop='')
                    result = pytrend.interest_over_time()
                    
                    if not result.empty:
                        print(f"Succès ! {len(result)} lignes de données")
                        for date, row in result.iterrows():
                            for keyword in keyword_chunk:
                                if keyword in row:
                                    data_point = {
                                        "keyword": keyword,
                                        "country": country_code,
                                        "date": date.strftime('%Y-%m-%d'),
                                        "search_frequency": int(row[keyword]),
                                        "isPartial": row['isPartial'] if 'isPartial' in row else False
                                    }
                                    all_data.append(data_point)
                        
                        points_added = len(result) * len(keyword_chunk)
                        print(f"Ajouté {points_added} points de données") 
                    else:
                        print("Aucune donnée disponible")
                        
                except Exception as e:
                    print(f"Erreur pour chunk {keyword_chunk} en {country_code}: {e}")
                    pause2 = random.uniform(60, 90)
                    print(f"Pause erreur: {pause2:.2f}s")
                    time.sleep(pause2)
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    return json.dumps(all_data, indent=2)

def json_to_parquet(json_data, output_path):
    spark = create_spark_session()
    df = spark.read.json(spark.sparkContext.parallelize([json_data]))
    df_with_dates = df \
            .withColumn("annee_insert", year(col("date"))) \
            .withColumn("mois_insert", month(col("date"))) \
            .withColumn("jour_insert", dayofmonth(col("date")))
    df_with_dates.write.mode("overwrite").partitionBy("annee_insert", "mois_insert", "jour_insert").parquet(output_path)
    print(f"Les données ont été écrites dans {output_path}")

# Exécution
if __name__ == "__main__":
    print("Démarrage de la collecte des données Google Trends...")
    
    resulta = get_trends(keywords_techs, country_codes, start_date='2024-06-01', end_date='2025-06-30')
    print("Collecte terminée, conversion en Parquet...")
    
    json_to_parquet(resulta, output_path)
    print("Traitement terminé avec succès !")