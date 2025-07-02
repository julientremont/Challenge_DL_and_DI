from pyspark.sql.functions import col, year, month, dayofmonth, to_date
from pytrends.request import TrendReq
from calendar import monthrange
import requests
import os
import sys
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager


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

def get_azuna_data(pays_europeens, key):
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


def json_to_parquet(json_data, output_path):
    spark = spark_manager.get_session()
    df = spark.createDataFrame([json_data])
    df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    df_with_dates = df \
        .withColumn("annee_insert", year(col("date"))) \
        .withColumn("mois_insert", month(col("date"))) \
        .withColumn("jour_insert", dayofmonth(col("date")))
    print(f"Les données ont été écrites dans {output_path}")
    spark_manager.write_parquet(df_with_dates, output_path, mode="append", partition_by=["annee_insert", "mois_insert", "jour_insert"])

print("Récupération des données d'Azuna...")
for key in pays_europeens.keys():

        json_data = get_azuna_data(pays_europeens, key)
        print("Données récupérées avec succès.")
        if json_data:
            if 'month' in json_data and json_data['month']:
                for date, salary_value in json_data['month'].items():
                    data_point = {
                        "keyword": 'it-jobs',
                        "country": pays_europeens[key],
                        "date": date,
                        "average_salary": salary_value
                    }
                    output_path = f"./data/bronze/azuna/{key}"
                    json_to_parquet(data_point, output_path)
            else:
                print(f"Structure de données inattendue pour {key}")
                print(f"Clés disponibles : {list(json_data.keys())}")
        else:
            print(f"Aucune donnée pour {key}")