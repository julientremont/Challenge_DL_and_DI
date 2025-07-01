from pyspark.sql.functions import col, year, month, dayofmonth, to_date
from pytrends.request import TrendReq
from calendar import monthrange


from src.utils.sparkmanager import spark_manager

keywords_techs = [
    # Langages de programmation populaires
    'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
    
    # Frameworks et bibliothèques web
    'react', 'angular', 'vue.js', 'django',]
country_codes = [ "FR"]


def get_trends_histo(keywords_techs, country_codes, start_date, end_date):
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
                            # Sauvegarder en parquet
                            output_paths = f"../../data/bronze/gtrends/{country_code}/{keywords_tech}"
                            json_to_parquet(data_point, output_paths)
                            
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



from datetime import datetime, timedelta
import time
import random

def get_trends_day(keywords_techs, country_codes, date=None):
    pytrend = TrendReq(hl='fr', tz=360)
    
    # Si aucune date n'est fournie, prendre la veille
    if date is None:
        current_date = datetime.now() - timedelta(days=1)
    else:
        # Convertir la date string en objet datetime si nécessaire
        if isinstance(date, str):
            current_date = datetime.strptime(date, '%Y-%m-%d')
        else:
            current_date = date
    
    print(f"Récupération des données pour le {current_date.strftime('%Y-%m-%d')}")
    
    for keywords_tech in keywords_techs:
        for country_code in country_codes:
            try:
                print(f"Recherche {keywords_tech}, {country_code} pour la date {current_date.strftime('%Y-%m-%d')}")
                
                # Format de timeframe pour un jour spécifique
                timeframe = current_date.strftime('%Y-%m-%d')
                
                pytrend.build_payload([keywords_tech], cat=0, timeframe=timeframe, geo=country_code, gprop='')
                result = pytrend.interest_over_time()
                
                if not result.empty:
                    print("Succès !")
                    for date_index, row in result.iterrows():
                        data_point = {
                            "keyword": keywords_tech,
                            "country": country_code,
                            "date": date_index.strftime('%Y-%m-%d'),
                            "search_frequency": int(row[keywords_tech]),
                            "isPartial": row['isPartial']
                        }
                        # Sauvegarder en parquet
                        output_paths = f"./datas/bronze/gtrends/{country_code}/{keywords_tech}"
                        json_to_parquet(data_point, output_paths)
                        
                    pause2 = random.uniform(2, 5)  # Pause plus réaliste
                    print(f"Temps de pause {pause2:.2f} secondes")
                    time.sleep(pause2)
                else:
                    print("Aucune donnée")
                    
            except Exception as e:
                print(f"Erreur pour {keywords_tech} en {country_code}: {e}")
                pause2 = random.uniform(30, 60)  # Pause plus longue en cas d'erreur
                print(f"Temps de pause {pause2:.2f} secondes")
                time.sleep(pause2)

def json_to_parquet(json_data, output_path):
    spark = spark_manager.get_session()
    df = spark.createDataFrame([json_data])
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    df_with_dates = df \
        .withColumn("annee_insert", year(col("date"))) \
        .withColumn("mois_insert", month(col("date"))) \
        .withColumn("jour_insert", dayofmonth(col("date")))
    print(f"Les données ont été écrites dans {output_path}")
    spark_manager.write_parqu
    et(df_with_dates, output_path, mode="append", partition_by=["annee_insert", "mois_insert", "jour_insert"])


# importer les trends sur une plage donné.
resulta = get_trends_histo(keywords_techs, country_codes, start_date='2025-05-01', end_date='2025-06-01')
 #resulta = get_trends_day(keywords_techs, country_codes, date=None)
print("Traitement terminé avec succès !")
