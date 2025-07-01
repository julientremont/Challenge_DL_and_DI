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
    'laravel', 'rails', 'asp.net', 'nextjs', 'nuxt', 'svelte', 'fastapi',]

country_codes = [ "FR"
]


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

    # Configuration optimale
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    return spark

def get_trends(keywords_techs, country_codes, start_date, end_date):
    pytrend = TrendReq(hl='fr', tz=360)
    all_data = []
    
    # Convertir les dates string en objets datetime si nécessaire
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Générer les plages mensuelles
    current_date = start_date.replace(day=1)  # Premier jour du mois
    
    while current_date <= end_date:
        # Calculer le dernier jour du mois
        last_day = monthrange(current_date.year, current_date.month)[1]
        month_end = current_date.replace(day=last_day)
        
        # Limiter à la date de fin si nécessaire
        if month_end > end_date:
            month_end = end_date
            
        start_str = current_date.strftime('%Y-%m-%d')
        end_str = month_end.strftime('%Y-%m-%d')
        plage_date = f"{start_str} {end_str}"
        
        for keywords_tech in keywords_techs:
            for country_code in country_codes:
                try:
                    print(f"Recherche {keywords_tech}, {country_code}")
                    pytrend.build_payload([keywords_tech], cat=0, timeframe=plage_date, geo=country_code, gprop='')
                    result = pytrend.interest_over_time()
                    
                    if not result.empty:
                        print("Succès !")
                        for date, row in result.iterrows():
                            data_point = {
                                "keyword": keywords_tech,
                                "country": country_code,
                                "date": date.strftime('%Y-%m-%d'),
                                "search_frequency": int(row[keywords_tech]),
                                "isPartial": row['isPartial']
                            }
                            all_data.append(data_point) 
                        print(f"Ajouté {len(result)} points de données")
                        pause2 = random.uniform(1, 1)
                        print(f"Temps de pause {pause2:.2f} secondes")
                        time.sleep(pause2)
                    else:
                        print("Aucune donnée")
                except Exception as e:
                    print(f"Erreur pour {keywords_tech} en {country_code}: {e}")
                    pause2 = random.uniform(10, 20)
                    print(f"Temps de pause {pause2:.2f} secondes")
                    time.sleep(pause2)
        
        # Passer au mois suivant
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
    df_with_dates.write.mode("append").partitionBy("annee_insert", "mois_insert", "jour_insert").parquet(output_path)
    print(f"Les données ont été écrites dans {output_path}")

resulta = get_trends(keywords_techs, country_codes, start_date='2025-05-01', end_date='2025-06-30')
print(resulta)
json_to_parquet(resulta, output_path)
print("Traitement terminé avec succès !")
