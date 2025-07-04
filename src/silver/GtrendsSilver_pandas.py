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

class GTrendsSilverProcessor:
    """Google Trends silver layer processor using pandas and SQLManager"""
    
    def __init__(self):
        self.table_name = "trends_silver"
        self.keywords_techs = [
            # Langages de programmation populaires
            'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
            'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
            
            # Frameworks et bibliothèques web
            'react', 'angular', 'django',
        ]
        
        self.country_codes = ["FR", "AT", "BE", "CH", "DE", "ES", "GB", "IT", "NL", "PL"]

    def supprimer_lignes_vides(self, df, seuil_pct=0.7):
        """Remove rows with too many null values"""
        nb_colonnes = len(df.columns)
        
        null_counts = df.isnull().sum(axis=1)
        pct_nulls = null_counts / nb_colonnes
        
        df_filtered = df[pct_nulls < seuil_pct].copy()
        
        return df_filtered

    def get_bronze_parquet(self, output_path):
        """Read bronze parquet data"""
        try:
            # Handle partitioned parquet files
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
            keyword VARCHAR(255),
            country_code VARCHAR(10),
            date DATE,
            search_frequency INT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            data_quality_score INT,
            INDEX idx_keyword_country_date (keyword, country_code, date),
            INDEX idx_country_date (country_code, date),
            INDEX idx_keyword (keyword),
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
            insert_query = f"""
                INSERT INTO {self.table_name} ({', '.join(columns)})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                search_frequency = VALUES(search_frequency),
                processed_at = VALUES(processed_at),
                data_quality_score = VALUES(data_quality_score)
            """
            
            affected_rows = sql_manager.execute_bulk_insert(insert_query, values)
            logger.info(f"Successfully saved {affected_rows} rows to {self.table_name}")
            
        except Exception as e:
            logger.error(f"Error saving to MySQL: {e}")
            raise

    def process(self):
        """Main processing function for Google Trends data"""
        logger.info("Starting Google Trends silver layer processing...")
        
        self.create_mysql_table()
        
        dataframes = []
        
        # Process each country and keyword combination
        for country_code in self.country_codes:
            for keyword in self.keywords_techs:
                output_path = f"../../data/bronze/gtrends/{country_code}/{keyword}"
                try:
                    df = self.get_bronze_parquet(output_path)
                    df['country_code'] = country_code
                    dataframes.append(df)
                    logger.info(f"Loaded data for {country_code} - {keyword}")
                except Exception as e:
                    logger.warning(f"Could not load data for {country_code} - {keyword}: {e}")
                    continue
        
        if not dataframes:
            logger.warning("No data found to process")
            return
        
        merged_df = pd.concat(dataframes, ignore_index=True)
        
        columns_to_drop = ['isPartial', 'annee_insert', 'mois_insert', 'jour_insert','country']
        existing_columns = [col for col in columns_to_drop if col in merged_df.columns]
        if existing_columns:
            merged_df = merged_df.drop(columns=existing_columns)
        
        merged_df = merged_df.drop_duplicates()
        
        merged_df = merged_df.dropna(subset=["search_frequency"])
        
        clean_df = self.supprimer_lignes_vides(merged_df)
        
        clean_df['processed_at'] = datetime.now()
        clean_df['data_quality_score'] = 90
        
        clean_df['date'] = pd.to_datetime(clean_df['date'])

        
        clean_df['search_frequency'] = clean_df['search_frequency'].astype(int)
        
        self.save_to_mysql(clean_df)
        
        logger.info("Google Trends silver layer processing completed!")

def main():
    """Main execution function"""
    processor = GTrendsSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()