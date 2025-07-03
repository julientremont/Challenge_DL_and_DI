import pandas as pd
import os
import glob
import sys
from typing import List
import logging
import pycountry

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sqlmanager import sql_manager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EuroTechJobsSilverProcessor:
    def __init__(self, bronze_path: str = "../../data/bronze/eurotechjobs"):
        self.bronze_path = bronze_path
        self.table_name = "eurotechjobs_silver"
        
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
        """Clean and normalize the EuroTechJobs data"""
        if df.empty:
            return df
            
        logger.info("Starting data cleaning and normalization...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['url']).copy()
        logger.info(f"Removed {initial_count - len(df)} duplicate records")
        
        df = self._clean_job_titles(df)
        df = self._clean_companies(df) 
        df = self._normalize_locations(df)
        df = self._process_technologies(df)
        df = self._normalize_job_types(df)
        df = self._clean_categories(df)
        
        columns_to_drop = ['year', 'month', 'day', 'technologies', 'scraped_at',
                          'job_title', 'company', 'location', 'job_type', 'category']
        for col in columns_to_drop:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        column_mapping = {
            'job_title_cleaned': 'job_title',
            'job_title_normalized': 'job_title_category', 
            'company_cleaned': 'company',
            'location_cleaned': 'location',
            'technologies_cleaned': 'technologies',
            'job_type_normalized': 'job_type',
            'category_cleaned': 'category'
        }
        
        df = df.rename(columns=column_mapping)
        
        df['processed_at'] = pd.Timestamp.now()
        df['data_quality_score'] = self._calculate_quality_score(df)
        
        final_columns = [
            'job_title', 'job_title_category', 'company', 'location', 'country_code',
            'technologies', 'primary_technology', 'tech_count', 'job_type', 'category',
            'url', 'processed_at', 'data_quality_score'
        ]
        
        existing_columns = [col for col in final_columns if col in df.columns]
        df = df[existing_columns].copy()
        
        logger.info(f"Cleaned dataset contains {len(df)} records with {len(df.columns)} columns")
        return df
    
    def _clean_job_titles(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize job titles"""
        if 'job_title' not in df.columns:
            df['job_title_cleaned'] = 'unknown'
            return df
        
        df['job_title_cleaned'] = df['job_title'].str.strip()
        df['job_title_cleaned'] = df['job_title_cleaned'].fillna('unknown')
        
        title_patterns = {
            r'.*software\s+engineer.*': 'Software Engineer',
            r'.*software\s+developer.*': 'Software Developer', 
            r'.*full[\s\-]*stack.*': 'Full Stack Developer',
            r'.*frontend.*|.*front[\s\-]*end.*': 'Frontend Developer',
            r'.*backend.*|.*back[\s\-]*end.*': 'Backend Developer',
            r'.*data\s+scientist.*': 'Data Scientist',
            r'.*data\s+engineer.*': 'Data Engineer',
            r'.*devops.*': 'DevOps Engineer',
            r'.*systems?\s+engineer.*': 'Systems Engineer',
            r'.*security.*engineer.*': 'Security Engineer',
            r'.*ml.*engineer.*|.*machine\s+learning.*': 'ML Engineer',
            r'.*business\s+analyst.*': 'Business Analyst'
        }
        
        df['job_title_normalized'] = df['job_title_cleaned']
        for pattern, normalized in title_patterns.items():
            mask = df['job_title_cleaned'].str.contains(pattern, case=False, na=False)
            df.loc[mask, 'job_title_normalized'] = normalized
        
        return df
    
    def _clean_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize company names"""
        if 'company' not in df.columns:
            df['company_cleaned'] = 'unknown'
            return df
        
        df['company_cleaned'] = df['company'].str.strip()
        df['company_cleaned'] = df['company_cleaned'].fillna('unknown')
        
        df['company_cleaned'] = df['company_cleaned'].str.replace(r'\s+(Ltd|Limited|Inc|Corp|Corporation|GmbH|S\.A\.|B\.V\.)\.?$', '', regex=True, case=False)
        df['company_cleaned'] = df['company_cleaned'].str.strip()
        
        return df
    
    def _normalize_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize locations to country codes"""
        if 'location' not in df.columns:
            df['location_cleaned'] = 'unknown'
            df['country_code'] = 'unknown'
            return df
        
        df['location_cleaned'] = df['location'].str.strip()
        df['location_cleaned'] = df['location_cleaned'].fillna('unknown')
        
        df['country_code'] = df['location_cleaned'].apply(self._extract_country_code)
        
        return df
    
    def _extract_country_code(self, location: str) -> str:
        """Extract ISO country code from location string"""
        if pd.isna(location) or location.lower() == 'unknown':
            return 'unknown'
        
        location = str(location).strip()
        
        location_mappings = {
            'netherlands': 'NL',
            'germany': 'DE', 
            'france': 'FR',
            'united kingdom': 'GB',
            'uk': 'GB',
            'spain': 'ES',
            'italy': 'IT',
            'belgium': 'BE',
            'austria': 'AT',
            'switzerland': 'CH',
            'poland': 'PL',
            'czech republic': 'CZ',
            'hungary': 'HU',
            'slovakia': 'SK',
            'slovenia': 'SI',
            'croatia': 'HR',
            'bulgaria': 'BG',
            'romania': 'RO',
            'greece': 'GR',
            'portugal': 'PT',
            'ireland': 'IE',
            'denmark': 'DK',
            'sweden': 'SE',
            'norway': 'NO',
            'finland': 'FI',
            'estonia': 'EE',
            'latvia': 'LV',
            'lithuania': 'LT',
            'malta': 'MT',
            'cyprus': 'CY',
            'luxembourg': 'LU'
        }
        
        location_lower = location.lower()
        for country_name, code in location_mappings.items():
            if country_name in location_lower:
                return code
        
        try:
            parts = location.split(',')
            country_part = parts[-1].strip() if parts else location
            
            country = pycountry.countries.get(name=country_part)
            if country:
                return country.alpha_2
            
            try:
                country = pycountry.countries.search_fuzzy(country_part)[0]
                return country.alpha_2
            except LookupError:
                pass
                
        except Exception:
            pass
        
        words = location_lower.split()
        for word in reversed(words):
            word = word.strip('.,')
            if word in location_mappings:
                return location_mappings[word]
        
        return 'unknown'
    
    def _process_technologies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and normalize technology lists"""
        if 'technologies' not in df.columns:
            df['technologies_cleaned'] = None
            df['tech_count'] = 0
            return df
        
        df['technologies_list'] = df['technologies'].apply(self._parse_technologies)
        df['technologies_cleaned'] = df['technologies_list'].apply(lambda x: ','.join(x) if x else None)
        df['tech_count'] = df['technologies_list'].apply(len)
        
        df['primary_technology'] = df['technologies_list'].apply(self._extract_primary_tech)
        
        return df
    
    def _parse_technologies(self, tech_string: str) -> List[str]:
        """Parse technology string into clean list"""
        if pd.isna(tech_string) or tech_string == '':
            return []
        
        techs = [tech.strip().lower() for tech in str(tech_string).split(',') if tech.strip()]
        
        tech_mappings = {
            'javascript': 'javascript',
            'js': 'javascript',
            'typescript': 'typescript',
            'ts': 'typescript',
            'python': 'python',
            'java': 'java',
            'c#': 'csharp',
            'csharp': 'csharp',
            'c++': 'cpp',
            'cpp': 'cpp',
            'react': 'react',
            'angular': 'angular',
            'vue': 'vue',
            'nodejs': 'nodejs',
            'node.js': 'nodejs',
            'docker': 'docker',
            'kubernetes': 'kubernetes',
            'k8s': 'kubernetes',
            'aws': 'aws',
            'azure': 'azure',
            'gcp': 'gcp',
            'mysql': 'mysql',
            'postgresql': 'postgresql',
            'mongodb': 'mongodb',
            'redis': 'redis'
        }
        
        normalized_techs = []
        for tech in techs:
            normalized = tech_mappings.get(tech, tech)
            if normalized and normalized not in normalized_techs:
                normalized_techs.append(normalized)
        
        return normalized_techs
    
    def _extract_primary_tech(self, tech_list: List[str]) -> str:
        """Extract primary technology from list (prioritize programming languages)"""
        if not tech_list:
            return 'unknown'
        
        programming_languages = ['python', 'javascript', 'java', 'csharp', 'cpp', 'php', 'ruby', 'go', 'rust']
        
        for lang in programming_languages:
            if lang in tech_list:
                return lang
        
        return tech_list[0]
    
    def _normalize_job_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize job types to standard categories"""
        if 'job_type' not in df.columns:
            df['job_type_normalized'] = 'mid_level'
            return df
        
        df['job_type_normalized'] = df['job_type'].str.lower().str.strip()
        df['job_type_normalized'] = df['job_type_normalized'].fillna('mid_level')
        
        type_mappings = {
            'senior': 'senior',
            'lead': 'senior',
            'principal': 'senior',
            'staff': 'senior',
            'junior': 'junior',
            'entry': 'junior',
            'graduate': 'junior',
            'intern': 'internship',
            'internship': 'internship',
            'trainee': 'internship',
            'mid-level': 'mid_level',
            'mid_level': 'mid_level',
            'midlevel': 'mid_level'
        }
        
        df['job_type_normalized'] = df['job_type_normalized'].replace(type_mappings)
        df['job_type_normalized'] = df['job_type_normalized'].fillna('mid_level')
        
        return df
    
    def _clean_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize job categories"""
        if 'category' not in df.columns:
            df['category_cleaned'] = 'unknown'
            return df
        
        df['category_cleaned'] = df['category'].str.strip()
        df['category_cleaned'] = df['category_cleaned'].fillna('unknown')
        
        df['category_cleaned'] = df['category_cleaned'].str.replace(' ', '_').str.lower()
        
        return df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> pd.Series:
        """Calculate a data quality score for each job posting"""
        score = 100
        
        quality_score = pd.Series([score] * len(df), index=df.index)
        
        quality_score -= df.get('job_title_cleaned', pd.Series(['unknown']*len(df))).eq('unknown') * 20
        quality_score -= df.get('company_cleaned', pd.Series(['unknown']*len(df))).eq('unknown') * 15
        quality_score -= df.get('country_code', pd.Series(['unknown']*len(df))).eq('unknown') * 10
        quality_score -= df.get('tech_count', pd.Series([0]*len(df))).eq(0) * 25
        
        has_complete_info = (
            df.get('job_title_cleaned', pd.Series(['unknown']*len(df))).ne('unknown') &
            df.get('company_cleaned', pd.Series(['unknown']*len(df))).ne('unknown') &
            df.get('country_code', pd.Series(['unknown']*len(df))).ne('unknown') &
            df.get('tech_count', pd.Series([0]*len(df))).gt(0)
        )
        quality_score += has_complete_info * 10
        
        return quality_score.clip(lower=0, upper=100)
    
    def create_mysql_table(self):
        """Create MySQL table for EuroTechJobs silver layer data"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            
            job_title VARCHAR(500),
            job_title_category VARCHAR(255),
            company VARCHAR(255),
            
            location VARCHAR(255),
            country_code VARCHAR(3),
            
            technologies TEXT,
            primary_technology VARCHAR(100),
            tech_count INT DEFAULT 0,
            
            job_type VARCHAR(50),
            category VARCHAR(100),
            url VARCHAR(1000),
            
            processed_at DATETIME NOT NULL,
            data_quality_score TINYINT DEFAULT 0,
            
            INDEX idx_country (country_code),
            INDEX idx_job_title (job_title(255)),
            INDEX idx_job_title_category (job_title_category),
            INDEX idx_company (company),
            INDEX idx_primary_tech (primary_technology),
            INDEX idx_job_type (job_type),
            INDEX idx_category (category),
            INDEX idx_quality_score (data_quality_score DESC),
            INDEX idx_processed_at (processed_at DESC),
            FULLTEXT INDEX ft_technologies (technologies),
            UNIQUE KEY unique_job_url (url(767))
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """
        
        with sql_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(create_table_sql)
            conn.commit()
            logger.info(f"Created/verified table: {self.table_name}")
    
    def save_to_mysql(self, df: pd.DataFrame):
        """Save cleaned data to MySQL using sqlmanager"""
        if df.empty:
            logger.warning("No data to save to MySQL")
            return
            
        db_columns = list(df.columns)
        
        df_clean = df.where(pd.notnull(df), None)
        
        data_tuples = []
        for _, row in df_clean.iterrows():
            tuple_row = []
            for val in row:
                if pd.isna(val) or str(val).lower() == 'nan':
                    tuple_row.append(None)
                else:
                    tuple_row.append(val)
            data_tuples.append(tuple(tuple_row))
        
        placeholders = ', '.join(['%s'] * len(db_columns))
        insert_sql = f"""
        INSERT IGNORE INTO {self.table_name} (
            {', '.join(db_columns)}
        ) VALUES ({placeholders})
        """
        
        affected_rows = sql_manager.execute_bulk_insert(insert_sql, data_tuples, batch_size=100)
        logger.info(f"Successfully inserted {affected_rows} records into {self.table_name}")
    
    def _print_summary(self, df: pd.DataFrame):
        """Print processing summary statistics"""
        if df.empty:
            return
            
        logger.info("\n" + "="*50)
        logger.info("EUROTECHJOBS SILVER LAYER SUMMARY")
        logger.info("="*50)
        
        logger.info(f"Total records processed: {len(df)}")
        logger.info(f"Average data quality score: {df['data_quality_score'].mean():.1f}")
        
        country_counts = df['country_code'].value_counts().head(10)
        logger.info(f"\nTop countries:")
        for country, count in country_counts.items():
            logger.info(f"  {country}: {count}")
        
        primary_tech_counts = df['primary_technology'].value_counts().head(10)
        logger.info(f"\nTop primary technologies:")
        for tech, count in primary_tech_counts.items():
            logger.info(f"  {tech}: {count}")
        
        job_type_counts = df['job_type'].value_counts()
        logger.info(f"\nJob type distribution:")
        for job_type, count in job_type_counts.items():
            logger.info(f"  {job_type}: {count}")
        
        high_quality = len(df[df['data_quality_score'] >= 80])
        medium_quality = len(df[(df['data_quality_score'] >= 60) & (df['data_quality_score'] < 80)])
        low_quality = len(df[df['data_quality_score'] < 60])
        
        logger.info(f"\nData quality distribution:")
        logger.info(f"  High quality (80+): {high_quality}")
        logger.info(f"  Medium quality (60-79): {medium_quality}")
        logger.info(f"  Low quality (<60): {low_quality}")
    

    def process(self):
        """Main processing function"""
        logger.info("Starting EuroTechJobs silver layer processing...")
        
        bronze_df = self.load_bronze_data()
        if bronze_df.empty:
            logger.warning("No data found in bronze layer")
            return
        
        silver_df = self.clean_and_normalize(bronze_df)
        
        self.create_mysql_table()
        self.save_to_mysql(silver_df)
        
        self._print_summary(silver_df)
        
        logger.info("EuroTechJobs silver layer processing completed!")

def main():
    """Main execution function"""
    processor = EuroTechJobsSilverProcessor()
    processor.process()

if __name__ == "__main__":
    main()