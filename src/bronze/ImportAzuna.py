from pyspark.sql.functions import col, year, month, dayofmonth, to_date
from datetime import datetime
import requests
import os
import sys
import json
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager
from src.utils.paths import get_bronze_path


pays_europeens = {
    "at": "Autriche",
    "be": "Belgique", 
    "ch": "Suisse",
    "de": "Allemagne",
    "es": "Espagne",
    "fr": "France",
    "gb": "Royaume-Uni",
    "it": "Italie",
    "nl": "Pays-Bas",
    "pl": "Pologne"
}

payload = {}
headers = {}

def get_azuna_histo(pays_europeens, key):
    url = f"http://api.adzuna.com/v1/api/jobs/{key}/history"
    params = {
        'app_id': '456b91b9',
        'app_key': 'b01c99237e96dc6d48903aba5a0d1fff',
        'where': pays_europeens[key],
        'category': 'it-jobs'
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Erreur HTTP {response.status_code} pour {key}")
            return None
        
        content_type = response.headers.get('content-type', '')
        if 'html' in content_type.lower():
            print(f"Erreur serveur (HTML) pour {key}")
            return None
        
        data = response.json()
        
        if 'exception' in data:
            if data['exception'] == 'UNSUPPORTED_COUNTRY':
                print(f"Pays non supporté: {key} ({pays_europeens[key]})")
            else:
                print(f"Erreur API pour {key}: {data['exception']}")
            return None

        if '__CLASS__' in data and 'HistoricalSalary' in data['__CLASS__']:
            print(f"Données récupérées pour {key}")
            return data
        else:
            print(f"Format de données inattendu pour {key}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Erreur de requête pour {key}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur JSON pour {key}: {e}")
        return None

def get_azuna_dispertion( key):
    url = f"http://api.adzuna.com/v1/api/jobs/{key}/histogram"
    params = {
        'app_id': '456b91b9',
        'app_key': 'b01c99237e96dc6d48903aba5a0d1fff',
        'what': 'it-jobs'
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code != 200:
            print(f"Erreur HTTP {response.status_code} pour {key}")
            return None
        
        content_type = response.headers.get('content-type', '')
        if 'html' in content_type.lower():
            print(f"Erreur serveur (HTML) pour {key}")
            return None
        
        data = response.json()
        
        if 'exception' in data:
            if data['exception'] == 'UNSUPPORTED_COUNTRY':
                print(f"Pays non supporté: {key} ({pays_europeens[key]})")
            else:
                print(f"Erreur API pour {key}: {data['exception']}")
            return None

        if '__CLASS__' in data and 'SalaryHistogram' in data['__CLASS__']:
            print(f"Données récupérées pour {key}")
            return data
        else:
            print(f"Format de données inattendu pour {key}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Erreur de requête pour {key}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur JSON pour {key}: {e}")
        return None
    
def json_to_parquet(donnees_json, chemin_sortie):
    spark = spark_manager.get_session()
    if isinstance(donnees_json, list):
        df = spark.createDataFrame(donnees_json)
    else:
        df = spark.createDataFrame([donnees_json])
    colonnes = df.columns
    if 'month' in colonnes:
        colonne_date = 'month'
        df = df.withColumnRenamed('month', 'date')
    elif 'date' in colonnes:
        colonne_date = 'date'
    else:
        raise ValueError("Aucune colonne 'date' ou 'month' trouvée dans les données")
    df = df.withColumn('date', to_date(col('date'), "yyyy-MM"))
    df_avec_dates = df \
        .withColumn("annee_insertion", year(col('date'))) \
        .withColumn("mois_insertion", month(col('date')))
    
    spark_manager.write_parquet(df_avec_dates, chemin_sortie, mode="overwrite", partition_by=["annee_insertion", "mois_insertion"])

def main():
    print("Récupération des données historique d'Azuna...")
    for key in pays_europeens.keys():
        time.sleep(3)
        json_data = get_azuna_histo(pays_europeens, key)
        print("Données récupérées avec succès.")
        data_final = []
        for date, salary_value in json_data['month'].items():
            data_point = {
                "keyword": 'it-jobs',
                "country": pays_europeens[key],
                "date": date,
                "average_salary":  float(salary_value)
            }
            data_final.append(data_point)  # Ajouter à la liste
        
        base_path = get_bronze_path('adzuna_jobs')
        output_path = str(base_path / "Salary" / key)
        json_to_parquet(data_final, output_path)

    print("Récupération des données histograme d'Azuna...")
    for key in pays_europeens.keys():
            json_data = get_azuna_dispertion(key)
            print("Données récupérées avec succès.")
            histogram = json_data['histogram']
            current_date = datetime.now().strftime("%Y-%m-%d")
            for salary_range, count in histogram.items():
                data_point = {
                    "keyword": 'it-jobs',
                    "country": pays_europeens[key],
                    "salary_range": salary_range,
                    "job_count":  float(count),
                    "date": current_date 
                }
                data_final.append(data_point)  # Ajouter à la liste
            base_path = get_bronze_path('adzuna_jobs')
            output_path = str(base_path / "Dispertion" / key)
            json_to_parquet(data_final, output_path)
if __name__ == "__main__":
    main()