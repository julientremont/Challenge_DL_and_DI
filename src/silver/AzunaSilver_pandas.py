import sys
import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from functools import reduce
import glob

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sqlmanager import sql_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AdzunaSilverProcessor:
    """Adzuna jobs silver layer processor using pandas and SQLManager"""
    
    def __init__(self):
        self.table_name = "adzuna_jobs_silver"
        self.pays_europeens = {
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

    def supprimer_lignes_vides(self, df, seuil_pct=0.9):
        """Remove rows with too many null values"""
        nb_colonnes = len(df.columns)
        
        null_counts = df.isnull().sum(axis=1)
        pct_nulls = null_counts / nb_colonnes
        
        df_filtered = df[pct_nulls < seuil_pct].copy()
        
        return df_filtered

    def get_bronze_parquet(self, output_path):
        """Read bronze parquet data"""
        try:
            if os.path.isdir(output_path):
                parquet_files = glob.glob(os.path.join(output_path, "**/*.parquet"), recursive=True)
                if parquet_files:
                    dfs = []
                    for file in parquet_files:
                        df = pd.read_parquet(file)
                        dfs.append(df)
                    return pd.concat(dfs, ignore_index=True)
                else:
                    return pd.read_parquet(output_path)
            else:
                return pd.read_parquet(output_path)
        except Exception as e:
            logger.error(f"Error reading parquet from {output_path}: {e}")
            raise

    def create_mysql_table(self):
        """Create MySQL table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job_title VARCHAR(255),
            country_code VARCHAR(10),
            date DATE,
            average_salary DECIMAL(15,2),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_quality_score INT,
            INDEX idx_country_date (country_code, date),
            INDEX idx_job_title (job_title),
            INDEX idx_date (date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """
        try:
            with sql_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(create_table_sql)
                conn.commit()
                logger.info(f"Created/verified table: {self.table_name}")
        except Exception as e:
            logger.error(f"Failed to create table: {e}")
            raise

    def save_to_mysql(self, df):
        """Save dataframe to MySQL using SQLManager"""
        try:
            df_clean = df.copy()
            
            df_clean = df_clean.where(pd.notnull(df_clean), None)
            
            columns = df_clean.columns.tolist()
            values = [tuple(row) for row in df_clean.values]
            
            placeholders = ', '.join(['%s'] * len(columns))
            #replace NaN with None for MySQL compatibility
            insert_query = f"""
                INSERT INTO {self.table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                processed_at = VALUES(processed_at),
                data_quality_score = VALUES(data_quality_score)
            """
            
            affected_rows = sql_manager.execute_bulk_insert(insert_query, values)
            logger.info(f"Successfully saved {affected_rows} rows to {self.table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to MySQL: {e}")
            raise

    def process_salary_data(self):
        """Process salary data from bronze layer"""
        logger.info("Processing Adzuna salary data...")
        
        dataframes = []
        
        for key in self.pays_europeens.keys():
            output_path = f"../../data/bronze/azuna/Salary/{key}"
            try:
                df = self.get_bronze_parquet(output_path)
                dataframes.append(df)
                logger.info(f"Loaded salary data for {key}")
            except Exception as e:
                logger.warning(f"Could not load salary data for {key}: {e}")
                continue
        
        if not dataframes:
            logger.warning("No salary data found to process")
            return None
        
        merged_df = pd.concat(dataframes, ignore_index=True)
        
        columns_to_drop = ['annee_insertion', 'mois_insertion']
        existing_columns = [col for col in columns_to_drop if col in merged_df.columns]
        if existing_columns:
            merged_df = merged_df.drop(columns=existing_columns)
        
        merged_df = merged_df.drop_duplicates()
        
        rename_mapping = {
            'keyword': 'job_title'
        }
        merged_df = merged_df.rename(columns=rename_mapping)
        
        final_dataframes = []
        for key in self.pays_europeens.keys():
            country_df = merged_df[merged_df['country'] == self.pays_europeens[key]].copy()
            if not country_df.empty:
                country_df['country_code'] = key.upper()
                country_df['country_name'] = self.pays_europeens[key]
                final_dataframes.append(country_df)
        
        if final_dataframes:
            final_df = pd.concat(final_dataframes, ignore_index=True)
            
            clean_df = self.supprimer_lignes_vides(final_df)
            
            clean_df['processed_at'] = datetime.now()
            clean_df['data_quality_score'] = 85
            clean_df['salary_range'] = None
            clean_df['job_count'] = None
            
            clean_df['date'] = pd.to_datetime(clean_df['date'])
            
            return clean_df
        
        return None
    
    def process_dispersion_data(self):
        """Process job dispersion data from bronze layer"""
        logger.info("Processing Adzuna dispersion data...")
        
        dataframes = []
        
        for key in self.pays_europeens.keys():
            output_path = f"../../data/bronze/azuna/Dispertion/{key}"
            try:
                df = self.get_bronze_parquet(output_path)
                dataframes.append(df)
                logger.info(f"Loaded dispersion data for {key}")
            except Exception as e:
                logger.warning(f"Could not load dispersion data for {key}: {e}")
                continue
        
        if not dataframes:
            logger.warning("No dispersion data found to process")
            return None
        
        merged_df = pd.concat(dataframes, ignore_index=True)
        
        columns_to_drop = ['annee_insertion', 'mois_insertion', 'average_salary']
        existing_columns = [col for col in columns_to_drop if col in merged_df.columns]
        if existing_columns:
            merged_df = merged_df.drop(columns=existing_columns)
        
        merged_df = merged_df.drop_duplicates()
        
        rename_mapping = {
            'keyword': 'job_title'
        }
        merged_df = merged_df.rename(columns=rename_mapping)
        
        final_dataframes = []
        for key in self.pays_europeens.keys():
            country_df = merged_df[merged_df['country'] == self.pays_europeens[key]].copy()
            if not country_df.empty:
                country_df['country_code'] = key.upper()
                final_dataframes.append(country_df)
        
        if final_dataframes:
            final_df = pd.concat(final_dataframes, ignore_index=True)
            
            clean_df = self.supprimer_lignes_vides(final_df)
            
            clean_df = clean_df.dropna(subset=["salary_range", "job_count"])
            
            clean_df['processed_at'] = datetime.now()
            clean_df['data_quality_score'] = 80
            clean_df['average_salary'] = None
            
            clean_df['date'] = pd.to_datetime(clean_df['date'])
            
            return clean_df
        
        return None
    
    def process(self):
        """Main processing function"""
        logger.info("Starting Adzuna silver layer processing...")
        
        self.create_mysql_table()
        
        salary_df = self.process_salary_data()
        dispersion_df = self.process_dispersion_data()
        
        if salary_df is not None and dispersion_df is not None:
            dfs_to_concat = []
            
            if not salary_df.empty and not salary_df.isna().all().all():
                dfs_to_concat.append(salary_df)
            
            if not dispersion_df.empty and not dispersion_df.isna().all().all():
                dfs_to_concat.append(dispersion_df)
            
            if dfs_to_concat:
                all_columns = set()
                for df in dfs_to_concat:
                    all_columns.update(df.columns)
                
                aligned_dfs = []
                for df in dfs_to_concat:
                    df_copy = df.copy()
                    for col in all_columns:
                        if col not in df_copy.columns:
                            df_copy[col] = None

                    df_copy = df_copy[sorted(all_columns)]
                    aligned_dfs.append(df_copy)
                
                combined_df = pd.concat(aligned_dfs, ignore_index=True)
                combined_df = combined_df.drop(columns=['country', 'job_count', 'salary_range','country_name'], errors='ignore')
                combined_df = combined_df.dropna(subset=['average_salary'], how='all')
                self.save_to_mysql(combined_df)
                logger.info("Successfully processed and saved combined Adzuna data")
            else:
                logger.warning("Both dataframes are empty or all-NA, nothing to save")
        elif salary_df is not None:
            self.save_to_mysql(salary_df)
            logger.info("Successfully processed and saved Adzuna salary data")
        elif dispersion_df is not None:
            self.save_to_mysql(dispersion_df)
            logger.info("Successfully processed and saved Adzuna dispersion data")
        else:
            logger.warning("No data to process")

def main():
    """Main execution function"""
    processor = AdzunaSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()