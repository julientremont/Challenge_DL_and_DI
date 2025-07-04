import pandas as pd
import os
import glob
import logging

from src.utils.sqlmanager import sql_manager
from src.utils.mysql_schemas import create_table

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubReposSilverProcessor:
    def __init__(self, bronze_path: str = "../../data/bronze/github_repos"):
        self.bronze_path = bronze_path
        self.table_name = "github_repos_silver"
        
    def load_bronze_data(self) -> pd.DataFrame:
        """Load all parquet files from bronze layer"""
        parquet_files = glob.glob(os.path.join(self.bronze_path, "*.parquet"))
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {self.bronze_path}")
            return pd.DataFrame()
        
        logger.info(f"Found {len(parquet_files)} parquet files to process")
        
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
                logger.info(f"Loaded {len(df)} records from {os.path.basename(file)}")
            except Exception as e:
                logger.error(f"Error loading {file}: {e}")
                
        if not dfs:
            return pd.DataFrame()
            
        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(combined_df)} total records from bronze layer")
        
        return combined_df
    
    def clean_and_normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize the GitHub repos data"""
        if df.empty:
            return df
            
        logger.info("Starting data cleaning and normalization...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['id'], keep='last').copy()  # Use .copy() to avoid warnings
        logger.info(f"Removed {initial_count - len(df)} duplicate repositories")
        
        df.loc[:, 'name_cleaned'] = df['name'].str.strip().str.lower()
        df.loc[:, 'name_cleaned'] = df['name_cleaned'].str.replace(r'[^\w\-\.]', '', regex=True)
        df = df.drop(columns=['name'], errors='ignore')
        
        df.loc[:, 'technology_normalized'] = df['technology'].str.strip().str.lower()
        
        df.loc[:, 'name'] = df['name'].fillna('unknown')
        df.loc[:, 'technology_normalized'] = df['technology_normalized'].fillna('unknown')
        df.loc[:, 'search_type'] = df['search_type'].fillna('unknown')
        
        numeric_fields = ['stars_count', 'forks_count', 'watchers_count', 'open_issues_count']
        for field in numeric_fields:
            df.loc[:, field] = pd.to_numeric(df[field], errors='coerce').fillna(0)
            df.loc[:, field] = df[field].clip(lower=0)
        
        df.loc[:, 'created_at_cleaned'] = pd.to_datetime(df['created_at'], errors='coerce', utc=True).dt.tz_localize(None)
        df.loc[:, 'created_at_cleaned'] = df['created_at_cleaned'].fillna(pd.Timestamp('2008-01-01'))  # GitHub launch date as default
        
        df.loc[:, 'collected_at_cleaned'] = pd.to_datetime(df['collected_at'], errors='coerce', utc=True).dt.tz_localize(None)
        df.loc[:, 'collected_at_cleaned'] = df['collected_at_cleaned'].fillna(pd.Timestamp.now())
        
        df = self._recalculate_metrics(df)
        
        df.loc[:, 'processed_at'] = pd.Timestamp.now()
        df.loc[:, 'data_quality_score'] = self._calculate_quality_score(df)
        
        silver_columns = [
            'id', 'name', 'name_cleaned', 'technology_normalized', 'search_type',
            'stars_count', 'forks_count', 'watchers_count', 'open_issues_count',
            'created_at_cleaned', 'collected_at_cleaned',
            'popularity_score', 'days_since_creation', 'activity_score', 'activity_level',
            'processed_at', 'data_quality_score'
        ]
        
        df_silver = df[silver_columns].copy()
        
        logger.info(f"Cleaned dataset contains {len(df_silver)} repositories")
        return df_silver
    
    def _recalculate_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Recalculate activity metrics with cleaned data"""
        now = pd.Timestamp.now().tz_localize(None)
        df.loc[:, 'days_since_creation'] = (now - df['created_at_cleaned']).dt.days
        df.loc[:, 'days_since_creation'] = df['days_since_creation'].clip(lower=0)
        
        df.loc[:, 'popularity_score'] = (
            df['stars_count'] * 1.0 + 
            df['forks_count'] * 2.0 + 
            df['watchers_count'] * 0.5
        )
        
        age_penalty = (df['days_since_creation'] - 365).clip(lower=0) * 0.05
        df.loc[:, 'activity_score'] = (df['popularity_score'] - age_penalty).clip(lower=0)
        
        df.loc[:, 'activity_level'] = pd.cut(
            df['activity_score'],
            bins=[-float('inf'), 100, 1000, float('inf')],
            labels=['low', 'medium', 'high']
        ).astype(str)
        
        return df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        score = 100
        
        quality_score = pd.Series([score] * len(df), index=df.index)
        
        quality_score -= (df['days_since_creation'] > 3650) * 10  # 10 years
        
        quality_score -= (df['popularity_score'] == 0) * 20
        
        quality_score -= df['name'].str.contains(r'test|tmp|temp', case=False, na=False) * 15
        
        return quality_score.clip(lower=0, upper=100)
    
    def create_mysql_table(self):
        """Create MySQL table for silver layer data using centralized schema"""
        success = create_table(self.table_name)
        if success:
            logger.info(f"Created/verified table: {self.table_name}")
        else:
            logger.error(f"Failed to create table: {self.table_name}")
            raise Exception(f"Failed to create table: {self.table_name}")
    
    def save_to_mysql(self, df: pd.DataFrame):
        """Save cleaned data to MySQL"""
        if df.empty:
            logger.warning("No data to save to MySQL")
            return
            
        data_tuples = []
        for _, row in df.iterrows():
            data_tuples.append((
                int(row['id']),
                str(row['name']),
                str(row['name_cleaned']),
                str(row['technology_normalized']),
                str(row['search_type']),
                int(row['stars_count']),
                int(row['forks_count']),
                int(row['watchers_count']),
                int(row['open_issues_count']),
                row['created_at_cleaned'],
                row['collected_at_cleaned'],
                float(row['popularity_score']),
                int(row['days_since_creation']),
                float(row['activity_score']),
                str(row['activity_level']),
                row['processed_at'],
                int(row['data_quality_score'])
            ))
        
        insert_sql = f"""
        INSERT INTO {self.table_name} (
            id, name, name_cleaned, technology_normalized, search_type,
            stars_count, forks_count, watchers_count, open_issues_count,
            created_at_cleaned, collected_at_cleaned, popularity_score,
            days_since_creation, activity_score, activity_level,
            processed_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            name_cleaned = VALUES(name_cleaned),
            technology_normalized = VALUES(technology_normalized),
            search_type = VALUES(search_type),
            stars_count = VALUES(stars_count),
            forks_count = VALUES(forks_count),
            watchers_count = VALUES(watchers_count),
            open_issues_count = VALUES(open_issues_count),
            created_at_cleaned = VALUES(created_at_cleaned),
            collected_at_cleaned = VALUES(collected_at_cleaned),
            popularity_score = VALUES(popularity_score),
            days_since_creation = VALUES(days_since_creation),
            activity_score = VALUES(activity_score),
            activity_level = VALUES(activity_level),
            processed_at = VALUES(processed_at),
            data_quality_score = VALUES(data_quality_score)
        """
        
        with sql_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            logger.info(f"Inserted/updated {len(data_tuples)} records in {self.table_name}")
    
    def process(self):
        """Main processing function"""
        logger.info("Starting GitHub repos silver layer processing...")
        
        bronze_df = self.load_bronze_data()
        if bronze_df.empty:
            logger.warning("No data found in bronze layer")
            return
        
        silver_df = self.clean_and_normalize(bronze_df)
        
        self.create_mysql_table()
        self.save_to_mysql(silver_df)
        
        self._print_summary(silver_df)
        
        logger.info("GitHub repos silver layer processing completed!")
    
    def _print_summary(self, df: pd.DataFrame):
        """Print processing summary"""
        if df.empty:
            return
            
        print("\n=== GitHub Repos Silver Layer Summary ===")
        print(f"Total repositories processed: {len(df)}")
        print(f"Technologies covered: {df['technology_normalized'].nunique()}")
        print(f"Average data quality score: {df['data_quality_score'].mean():.1f}")
        
        print("\nTop technologies by repository count:")
        tech_counts = df['technology_normalized'].value_counts().head(10)
        for tech, count in tech_counts.items():
            print(f"  {tech}: {count}")
        
        print("\nActivity level distribution:")
        activity_dist = df['activity_level'].value_counts()
        for level, count in activity_dist.items():
            print(f"  {level}: {count}")

def main():
    """Main execution function"""
    processor = GitHubReposSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()