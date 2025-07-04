import pandas as pd
from pytrends.request import TrendReq
import os
import sys
from calendar import monthrange
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from datetime import datetime
import time
import random

keywords_techs = [
    # Langages de programmation populaires
    'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
    
    # Frameworks et bibliothèques web
    'react', 'angular', 'vue.js', 'django',
]

country_codes = ["FR","AI","BE","CH", "DE", "ES", "GB", "IT", "NL", "PL"]

def get_trends_histo(keywords_techs, country_codes, start_date, end_date):
    """Récupère les données Google Trends pour les mots-clés et pays spécifiés"""
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
                        data_points = []
                        
                        for date, row in result.iterrows():
                            data_point = {
                                "keyword": keywords_tech,
                                "country": country_code,
                                "date": date.strftime('%Y-%m-%d'),
                                "search_frequency": int(row[keywords_tech]),
                                "isPartial": row['isPartial']
                            }
                            data_points.append(data_point)
                        
                        # Convertir en DataFrame et sauvegarder
                        if data_points:
                            df = pd.DataFrame(data_points)
                            output_path = f"../../data/bronze/gtrends/{country_code}/{keywords_tech}"
                            dataframe_to_parquet(df, output_path)
                            
                        pause_time = random.uniform(1, 2)
                        print(f"Temps de pause {pause_time:.2f} secondes")
                        time.sleep(pause_time)
                    else:
                        print("Aucune donnée")
                        
                except Exception as e:
                    print(f"Erreur pour {keywords_tech} en {country_code}: {e}")
                    pause_time = random.uniform(10, 20)
                    print(f"Temps de pause {pause_time:.2f} secondes")
                    time.sleep(pause_time)
        
        # Passer au mois suivant
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)

def dataframe_to_parquet(df, output_path):
    """Sauvegarde un DataFrame pandas en format parquet avec partitionnement"""
    # Ajouter les colonnes de partitionnement
    df['date'] = pd.to_datetime(df['date'])
    df['annee_insert'] = df['date'].dt.year
    df['mois_insert'] = df['date'].dt.month
    df['jour_insert'] = df['date'].dt.day
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_path, exist_ok=True)
    
    print(f"Les données ont été écrites dans {output_path}")
    
    # Sauvegarder en parquet avec partitionnement
    df.to_parquet(
        output_path, 
        index=False, 
        partition_cols=['annee_insert', 'mois_insert', 'jour_insert'],
        engine='pyarrow'
    )

def main():
    """Fonction principale pour collecter les données Google Trends"""
    # Importer les trends sur une plage donnée
    print("Début de la collecte des données Google Trends...")
    
    # Utiliser les mêmes dates que l'original
    start_date = '2025-06-07'
    end_date = '2025-07-03'
    
    # Alternatives commentées comme dans l'original:
    # yesterday = datetime.now() - timedelta(days=1)
    # date_yesterday = yesterday.strftime('%Y-%m-%d')
    # start_date = date_yesterday
    # end_date = date_yesterday
    
    get_trends_histo(keywords_techs, country_codes, start_date=start_date, end_date=end_date)
    print("Traitement terminé avec succès !")

if __name__ == "__main__":
    main()