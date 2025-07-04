import pandas as pd
from datetime import datetime
import requests
import os
import sys
import json
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))


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

def get_azuna_histo(pays_europeens, key):
    """Récupère les données historiques de salaire pour un pays donné"""
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

def get_azuna_dispertion(key):
    """Récupère les données d'histogramme de salaire pour un pays donné"""
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
    
def dataframe_to_parquet(df, chemin_sortie):
    """Sauvegarde un DataFrame pandas en format parquet avec partitionnement"""
    # Ajouter les colonnes de partitionnement
    df['date'] = pd.to_datetime(df['date'])
    df['annee_insertion'] = df['date'].dt.year
    df['mois_insertion'] = df['date'].dt.month
    
    # Créer le répertoire si nécessaire
    os.makedirs(chemin_sortie, exist_ok=True)
    
    # Sauvegarder en parquet avec partitionnement
    df.to_parquet(
        chemin_sortie, 
        index=False, 
        partition_cols=['annee_insertion', 'mois_insertion'],
        engine='pyarrow'
    )

def main():
    """Fonction principale pour collecter toutes les données Adzuna"""
    print("Récupération des données historiques d'Adzuna...")
    
    # Collecter les données historiques
    for key in pays_europeens.keys():
        time.sleep(3)  # Respecter les limites de l'API
        json_data = get_azuna_histo(pays_europeens, key)
        
        if json_data is None:
            continue
            
        print("Données récupérées avec succès.")
        data_final = []
        
        for date, salary_value in json_data['month'].items():
            data_point = {
                "keyword": 'it-jobs',
                "country": pays_europeens[key],
                "date": date,
                "average_salary": float(salary_value)
            }
            data_final.append(data_point)
        
        # Convertir en DataFrame et sauvegarder
        if data_final:
            df = pd.DataFrame(data_final)
            output_path = f"./data/bronze/azuna/Salary/{key}"
            dataframe_to_parquet(df, output_path)
    
    print("Récupération des données histogramme d'Adzuna...")
    
    # Collecter les données d'histogramme
    for key in pays_europeens.keys():
        time.sleep(3)  # Respecter les limites de l'API
        json_data = get_azuna_dispertion(key)
        
        if json_data is None:
            continue
            
        print("Données récupérées avec succès.")
        histogram = json_data['histogram']
        current_date = datetime.now().strftime("%Y-%m-%d")
        data_final = []
        
        for salary_range, count in histogram.items():
            data_point = {
                "keyword": 'it-jobs',
                "country": pays_europeens[key],
                "salary_range": salary_range,
                "job_count": float(count),
                "date": current_date 
            }
            data_final.append(data_point)
        
        # Convertir en DataFrame et sauvegarder
        if data_final:
            df = pd.DataFrame(data_final)
            output_path = f"../../data/bronze/azuna/Dispertion/{key}"
            dataframe_to_parquet(df, output_path)

if __name__ == "__main__":
    main()