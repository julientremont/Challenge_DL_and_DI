#!/usr/bin/env python3
"""
Gold Layer - Table Unification Script
Crée un entrepôt de données unifié à partir des 5 tables silver.
Architecture en étoile avec tables de dimensions et de faits.
"""

import sys
import logging
import mysql.connector

from src.utils import config

config = config.Config()
# Configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection
DB_CONFIG = {
    'user': config.mysql_user,
    'password': config.mysql_password,
    'host': config.mysql_host,
    'database': 'silver',
    'port': config.mysql_port,
}

DB_CONFIG_gold = {
    'user': config.mysql_user,
    'password': config.mysql_password,
    'host': config.mysql_host,
    'database': 'gold',
    'port': config.mysql_port,

}

class GoldDataWarehouse:
    """Gestionnaire de l'entrepôt de données Gold"""
    
    def __init__(self):
        self.conn_silver = None  # Connection to read from silver DB
        self.conn_gold = None    # Connection to write to gold DB
        self.dimension_schemas = self._get_dimension_schemas()
        self.fact_schemas = self._get_fact_schemas()
    
    def _get_dimension_schemas(self):
        """Schémas des tables de dimensions avec noms compréhensibles"""
        return {
            # Table de dates
            'dim_calendar': """
                CREATE TABLE IF NOT EXISTS dim_calendar (
                    date_key DATE PRIMARY KEY,
                    day SMALLINT,
                    month SMALLINT,
                    quarter SMALLINT,
                    year SMALLINT,
                    day_of_week SMALLINT,
                    week_of_year SMALLINT,
                    is_weekend BOOLEAN DEFAULT FALSE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table des pays
            'dim_country': """
                CREATE TABLE IF NOT EXISTS dim_country (
                    id_country INT AUTO_INCREMENT PRIMARY KEY,
                    country_code CHAR(3) UNIQUE NOT NULL,
                    country_name VARCHAR(100),
                    region VARCHAR(50),
                    continent VARCHAR(30)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table des entreprises
            'dim_company': """
                CREATE TABLE IF NOT EXISTS dim_company (
                    id_company INT AUTO_INCREMENT PRIMARY KEY,
                    company_name VARCHAR(255) NOT NULL,
                    company_normalized VARCHAR(255),
                    sector VARCHAR(100),
                    size_category VARCHAR(50),
                    INDEX idx_company_name (company_name),
                    INDEX idx_company_normalized (company_normalized)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table des technologies/compétences
            'dim_technology': """
                CREATE TABLE IF NOT EXISTS dim_technology (
                    id_technology INT AUTO_INCREMENT PRIMARY KEY,
                    technology_name VARCHAR(100) NOT NULL COLLATE utf8mb4_unicode_ci,
                    technology_category VARCHAR(50),
                    technology_type ENUM('language', 'framework', 'tool', 'database', 'cloud', 'other') DEFAULT 'other',
                    popularity_rank INT,
                    UNIQUE KEY unique_tech_name (technology_name),
                    INDEX idx_tech_category (technology_category),
                    INDEX idx_tech_type (technology_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table des sources de données
            'dim_data_source': """
                CREATE TABLE IF NOT EXISTS dim_data_source (
                    id_source SMALLINT AUTO_INCREMENT PRIMARY KEY,
                    source_name VARCHAR(50) NOT NULL UNIQUE,
                    source_type ENUM('job_board', 'survey', 'repository', 'trends') NOT NULL,
                    source_description TEXT,
                    is_active BOOLEAN DEFAULT TRUE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table des métiers/rôles
            'dim_job_role': """
                CREATE TABLE IF NOT EXISTS dim_job_role (
                    id_job_role INT AUTO_INCREMENT PRIMARY KEY,
                    role_title VARCHAR(255) NOT NULL,
                    role_category VARCHAR(100),
                    seniority_level ENUM('junior', 'mid', 'senior', 'lead', 'manager', 'unknown') DEFAULT 'unknown',
                    role_type ENUM('developer', 'data', 'devops', 'design', 'management', 'other') DEFAULT 'other',
                    INDEX idx_role_category (role_category),
                    INDEX idx_seniority (seniority_level)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        }
    
    def _get_fact_schemas(self):
        """Schémas des tables de faits avec noms compréhensibles"""
        return {
            # Table de faits principale - Activité Tech
            'analysis_tech_activity': """
                CREATE TABLE IF NOT EXISTS analysis_tech_activity (
                    id_activity BIGINT AUTO_INCREMENT PRIMARY KEY,
                    date_key DATE NOT NULL,
                    id_country INT,
                    id_technology INT,
                    id_source SMALLINT,
                    
                    -- Métriques jobs
                    job_count INT DEFAULT 0,
                    avg_salary_usd DECIMAL(10,2),
                    
                    -- Métriques développeurs (StackOverflow)
                    developer_count INT DEFAULT 0,
                    avg_developer_salary DECIMAL(10,2),
                    
                    -- Métriques popularité (GitHub + Trends)
                    github_stars INT DEFAULT 0,
                    github_forks INT DEFAULT 0,
                    search_volume INT DEFAULT 0,
                    popularity_score DECIMAL(12,2) DEFAULT 0,
                    
                    -- Métadonnées
                    data_quality_score TINYINT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    FOREIGN KEY (date_key) REFERENCES dim_calendar(date_key),
                    FOREIGN KEY (id_country) REFERENCES dim_country(id_country),
                    FOREIGN KEY (id_technology) REFERENCES dim_technology(id_technology),
                    FOREIGN KEY (id_source) REFERENCES dim_data_source(id_source),
                    
                    INDEX idx_date_tech (date_key, id_technology),
                    INDEX idx_country_tech (id_country, id_technology),
                    INDEX idx_source (id_source)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table de faits - Emplois détaillés
            'analysis_job_details': """
                CREATE TABLE IF NOT EXISTS analysis_job_details (
                    id_job BIGINT AUTO_INCREMENT PRIMARY KEY,
                    date_key DATE NOT NULL,
                    id_country INT,
                    id_company INT,
                    id_job_role INT,
                    id_source SMALLINT,
                    
                    -- Détails du poste
                    job_title VARCHAR(500),
                    salary_usd DECIMAL(10,2),
                    salary_range VARCHAR(50),
                    job_type ENUM('full_time', 'part_time', 'contract', 'freelance', 'unknown') DEFAULT 'unknown',
                    seniority ENUM('junior', 'mid', 'senior', 'lead', 'unknown') DEFAULT 'unknown',
                    
                    -- Métadonnées
                    data_quality_score TINYINT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    
                    FOREIGN KEY (date_key) REFERENCES dim_calendar(date_key),
                    FOREIGN KEY (id_country) REFERENCES dim_country(id_country),
                    FOREIGN KEY (id_company) REFERENCES dim_company(id_company),
                    FOREIGN KEY (id_job_role) REFERENCES dim_job_role(id_job_role),
                    FOREIGN KEY (id_source) REFERENCES dim_data_source(id_source),
                    
                    INDEX idx_date_country (date_key, id_country),
                    INDEX idx_company (id_company),
                    INDEX idx_salary (salary_usd)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            
            # Table de liaison - Technologies par job
            'bridge_job_technologies': """
                CREATE TABLE IF NOT EXISTS bridge_job_technologies (
                    id_job BIGINT,
                    id_technology INT,
                    is_primary BOOLEAN DEFAULT FALSE,
                    
                    PRIMARY KEY (id_job, id_technology),
                    FOREIGN KEY (id_job) REFERENCES analysis_job_details(id_job) ON DELETE CASCADE,
                    FOREIGN KEY (id_technology) REFERENCES dim_technology(id_technology)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        }
    
    def connect(self):
        """Connexion aux bases de données silver et gold"""
        try:
            # Connexion à la base silver (lecture)
            self.conn_silver = mysql.connector.connect(**DB_CONFIG)
            cursor_silver = self.conn_silver.cursor()
            cursor_silver.execute("SET collation_connection = utf8mb4_unicode_ci")
            cursor_silver.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor_silver.close()
            logger.info("Connexion réussie à la base silver")
            
            # Connexion à la base gold (écriture)
            self.conn_gold = mysql.connector.connect(**DB_CONFIG_gold)
            cursor_gold = self.conn_gold.cursor()
            cursor_gold.execute("SET collation_connection = utf8mb4_unicode_ci")
            cursor_gold.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
            cursor_gold.close()
            logger.info("Connexion réussie à la base gold")
            
            return True
        except Exception as e:
            logger.error(f"Erreur de connexion: {e}")
            return False
    
    def disconnect(self):
        """Fermeture des connexions"""
        if self.conn_silver:
            self.conn_silver.close()
            logger.info("Connexion silver fermée")
        if self.conn_gold:
            self.conn_gold.close()
            logger.info("Connexion gold fermée")
    
    def create_gold_tables(self):
        """Création de toutes les tables gold"""
        cursor = self.conn_gold.cursor()
        
        try:
            # Créer les dimensions d'abord
            logger.info("🏗️ Création des tables de dimensions...")
            for table_name, schema in self.dimension_schemas.items():
                cursor.execute(schema)
                logger.info(f"✅ Table {table_name} créée")
            
            # Puis les faits
            logger.info("🏗️ Création des tables de faits...")
            for table_name, schema in self.fact_schemas.items():
                cursor.execute(schema)
                logger.info(f"✅ Table {table_name} créée")
            
            self.conn_gold.commit()
            logger.info("🎉 Toutes les tables Gold créées avec succès")
            
        except Exception as e:
            logger.error(f"Erreur lors de la création des tables: {e}")
            self.conn_gold.rollback()
            raise
    
    def populate_dimensions(self):
        """Peuplement des tables de dimensions"""
        cursor_gold = self.conn_gold.cursor()
        cursor_silver = self.conn_silver.cursor()
        
        try:
            # 1. Peuplement dim_data_source
            logger.info("📊 Peuplement de dim_data_source...")
            sources = [
                ('trends_silver', 'trends', 'Google Trends - données de recherche'),
                ('stackoverflow_survey_silver', 'survey', 'StackOverflow Developer Survey'),
                ('adzuna_jobs_silver', 'job_board', 'Adzuna Job Board'),
                ('github_repos_silver', 'repository', 'GitHub Repositories'),
                ('eurotechjobs_silver', 'job_board', 'EuroTechJobs Job Board')
            ]
            
            cursor_gold.executemany("""
                INSERT IGNORE INTO dim_data_source (source_name, source_type, source_description)
                VALUES (%s, %s, %s)
            """, sources)
            
            # 2. Peuplement dim_country depuis toutes les sources
            logger.info("📊 Peuplement de dim_country...")
            cursor_gold.execute("""
                INSERT IGNORE INTO dim_country (country_code, country_name, region)
                SELECT DISTINCT 
                    country_code COLLATE utf8mb4_unicode_ci,
                    CASE country_code
                        WHEN 'FR' THEN 'France'
                        WHEN 'DE' THEN 'Germany'
                        WHEN 'UK' THEN 'United Kingdom'
                        WHEN 'NL' THEN 'Netherlands'
                        WHEN 'ES' THEN 'Spain'
                        WHEN 'IT' THEN 'Italy'
                        WHEN 'AT' THEN 'Austria'
                        ELSE country_code
                    END as country_name,
                    'Europe' as region
                FROM (
                    SELECT CONVERT(country_code USING utf8mb4) COLLATE utf8mb4_unicode_ci as country_code FROM silver.trends_silver WHERE country_code IS NOT NULL
                    UNION
                    SELECT CONVERT(country_code USING utf8mb4) COLLATE utf8mb4_unicode_ci as country_code FROM silver.adzuna_jobs_silver WHERE country_code IS NOT NULL
                    UNION 
                    SELECT CONVERT(country_code USING utf8mb4) COLLATE utf8mb4_unicode_ci as country_code FROM silver.eurotechjobs_silver WHERE country_code IS NOT NULL
                ) countries
                WHERE country_code IS NOT NULL AND country_code != ''
            """)
            
            # 3. Peuplement dim_technology
            logger.info("📊 Peuplement de dim_technology...")
            cursor_gold.execute("""
                INSERT IGNORE INTO dim_technology (technology_name, technology_category, technology_type)
                SELECT DISTINCT 
                    CONVERT(tech_name USING utf8mb4) COLLATE utf8mb4_unicode_ci as tech_name,
                    CASE 
                        WHEN tech_name IN ('Python', 'JavaScript', 'Java', 'C++', 'Go', 'Rust', 'PHP', 'Ruby') THEN 'Programming Language'
                        WHEN tech_name IN ('React', 'Vue', 'Angular', 'Django', 'Flask', 'Spring') THEN 'Framework'
                        WHEN tech_name IN ('MySQL', 'PostgreSQL', 'MongoDB', 'Redis') THEN 'Database'
                        WHEN tech_name IN ('AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes') THEN 'Cloud/DevOps'
                        ELSE 'Other'
                    END as category,
                    CASE 
                        WHEN tech_name IN ('Python', 'JavaScript', 'Java', 'C++', 'Go', 'Rust', 'PHP', 'Ruby') THEN 'language'
                        WHEN tech_name IN ('React', 'Vue', 'Angular', 'Django', 'Flask', 'Spring') THEN 'framework'
                        WHEN tech_name IN ('MySQL', 'PostgreSQL', 'MongoDB', 'Redis') THEN 'database'
                        WHEN tech_name IN ('AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes') THEN 'cloud'
                        ELSE 'other'
                    END as type
                FROM (
                    SELECT CONVERT(keyword USING utf8mb4) COLLATE utf8mb4_unicode_ci as tech_name FROM silver.trends_silver
                    UNION
                    SELECT CONVERT(technology_normalized USING utf8mb4) COLLATE utf8mb4_unicode_ci as tech_name FROM silver.github_repos_silver WHERE technology_normalized IS NOT NULL
                    UNION
                    SELECT CONVERT(primary_technology USING utf8mb4) COLLATE utf8mb4_unicode_ci as tech_name FROM silver.eurotechjobs_silver WHERE primary_technology IS NOT NULL
                    UNION
                    SELECT CONVERT(primary_language USING utf8mb4) COLLATE utf8mb4_unicode_ci as tech_name FROM silver.stackoverflow_survey_silver WHERE primary_language IS NOT NULL
                ) techs
                WHERE tech_name IS NOT NULL AND tech_name != ''
            """)
            
            # 4. Peuplement dim_company
            logger.info("📊 Peuplement de dim_company...")
            cursor_gold.execute("""
                INSERT IGNORE INTO dim_company (company_name, company_normalized, sector)
                SELECT DISTINCT 
                    company,
                    LOWER(TRIM(company)) as company_normalized,
                    'Technology' as sector
                FROM silver.eurotechjobs_silver 
                WHERE company IS NOT NULL AND company != ''
            """)
            
            # 5. Peuplement dim_job_role
            logger.info("📊 Peuplement de dim_job_role...")
            cursor_gold.execute("""
                INSERT IGNORE INTO dim_job_role (role_title, role_category, seniority_level, role_type)
                SELECT DISTINCT 
                    role_title,
                    CASE 
                        WHEN role_title LIKE '%developer%' OR role_title LIKE '%engineer%' THEN 'Development'
                        WHEN role_title LIKE '%data%' THEN 'Data'
                        WHEN role_title LIKE '%manager%' THEN 'Management'
                        ELSE 'Other'
                    END as category,
                    CASE 
                        WHEN role_title LIKE '%senior%' OR role_title LIKE '%lead%' THEN 'senior'
                        WHEN role_title LIKE '%junior%' THEN 'junior'
                        ELSE 'mid'
                    END as seniority,
                    CASE 
                        WHEN role_title LIKE '%developer%' OR role_title LIKE '%engineer%' THEN 'developer'
                        WHEN role_title LIKE '%data%' THEN 'data'
                        WHEN role_title LIKE '%manager%' THEN 'management'
                        ELSE 'other'
                    END as type
                FROM (
                    SELECT primary_role as role_title FROM silver.stackoverflow_survey_silver WHERE primary_role IS NOT NULL
                    UNION
                    SELECT job_title_category as role_title FROM silver.eurotechjobs_silver WHERE job_title_category IS NOT NULL
                ) roles
                WHERE role_title IS NOT NULL AND role_title != ''
            """)
            
            # 6. Peuplement dim_calendar
            logger.info("📊 Peuplement de dim_calendar...")
            cursor_gold.execute("""
                INSERT IGNORE INTO dim_calendar (date_key, day, month, quarter, year, day_of_week, week_of_year, is_weekend)
                SELECT DISTINCT 
                    date_val,
                    DAY(date_val),
                    MONTH(date_val),
                    QUARTER(date_val),
                    YEAR(date_val),
                    DAYOFWEEK(date_val),
                    WEEK(date_val),
                    DAYOFWEEK(date_val) IN (1, 7) as is_weekend
                FROM (
                    SELECT date as date_val FROM silver.trends_silver WHERE date IS NOT NULL
                    UNION
                    SELECT date as date_val FROM silver.adzuna_jobs_silver WHERE date IS NOT NULL
                    UNION
                    SELECT DATE(processed_at) as date_val FROM silver.github_repos_silver WHERE processed_at IS NOT NULL
                    UNION
                    SELECT DATE(processed_at) as date_val FROM silver.stackoverflow_survey_silver WHERE processed_at IS NOT NULL
                    UNION
                    SELECT DATE(processed_at) as date_val FROM silver.eurotechjobs_silver WHERE processed_at IS NOT NULL
                ) dates
                WHERE date_val IS NOT NULL
            """)
            
            self.conn_gold.commit()
            logger.info("🎉 Toutes les dimensions peuplées avec succès")
            
        except Exception as e:
            logger.error(f"Erreur lors du peuplement des dimensions: {e}")
            self.conn_gold.rollback()
            raise
    
    def populate_facts(self):
        """Peuplement des tables de faits"""
        cursor_gold = self.conn_gold.cursor()
        
        try:
            logger.info("📈 Peuplement de analysis_tech_activity...")
            
            # Agrégation des données par technologie, pays et date
            cursor_gold.execute("""
                INSERT INTO analysis_tech_activity (
                    date_key, id_country, id_technology, id_source,
                    search_volume, data_quality_score
                )
                SELECT 
                    t.date,
                    c.id_country,
                    tech.id_technology,
                    s.id_source,
                    t.search_frequency,
                    t.data_quality_score
                FROM silver.trends_silver t
                JOIN gold.dim_technology tech ON CONVERT(t.keyword USING utf8mb4) COLLATE utf8mb4_unicode_ci = tech.technology_name
                JOIN gold.dim_country c ON CONVERT(t.country_code USING utf8mb4) COLLATE utf8mb4_unicode_ci = c.country_code
                JOIN gold.dim_data_source s ON s.source_name = 'trends_silver'
                WHERE t.date IS NOT NULL
            """)
            
            # Ajouter les données GitHub
            cursor_gold.execute("""
                INSERT INTO analysis_tech_activity (
                    date_key, id_technology, id_source,
                    github_stars, github_forks, popularity_score, data_quality_score
                )
                SELECT 
                    DATE(g.processed_at),
                    tech.id_technology,
                    s.id_source,
                    g.stars_count,
                    g.forks_count,
                    g.popularity_score,
                    g.data_quality_score
                FROM silver.github_repos_silver g
                JOIN gold.dim_technology tech ON CONVERT(g.technology_normalized USING utf8mb4) COLLATE utf8mb4_unicode_ci = tech.technology_name
                JOIN gold.dim_data_source s ON s.source_name = 'github_repos_silver'
                WHERE g.processed_at IS NOT NULL
                ON DUPLICATE KEY UPDATE
                    github_stars = VALUES(github_stars),
                    github_forks = VALUES(github_forks),
                    popularity_score = VALUES(popularity_score)
            """)
            
            logger.info("📈 Peuplement de analysis_job_details...")
            
            # Jobs EuroTechJobs
            cursor_gold.execute("""
                INSERT INTO analysis_job_details (
                    date_key, id_country, id_company, id_job_role, id_source,
                    job_title, job_type, data_quality_score
                )
                SELECT 
                    DATE(e.processed_at),
                    c.id_country,
                    comp.id_company,
                    jr.id_job_role,
                    s.id_source,
                    e.job_title,
                    CASE e.job_type
                        WHEN 'full_time' THEN 'full_time'
                        WHEN 'part_time' THEN 'part_time'
                        ELSE 'unknown'
                    END,
                    e.data_quality_score
                FROM silver.eurotechjobs_silver e
                LEFT JOIN gold.dim_country c ON CONVERT(e.country_code USING utf8mb4) COLLATE utf8mb4_unicode_ci = c.country_code
                LEFT JOIN gold.dim_company comp ON e.company = comp.company_name
                LEFT JOIN gold.dim_job_role jr ON e.job_title_category = jr.role_title
                JOIN gold.dim_data_source s ON s.source_name = 'eurotechjobs_silver'
                WHERE e.processed_at IS NOT NULL
            """)
            
            # Peupler la table de liaison job-technologies
            logger.info("📈 Peuplement de bridge_job_technologies...")
            cursor_gold.execute("""
                INSERT IGNORE INTO bridge_job_technologies (id_job, id_technology, is_primary)
                SELECT 
                    jd.id_job,
                    tech.id_technology,
                    TRUE
                FROM gold.analysis_job_details jd
                JOIN silver.eurotechjobs_silver e ON jd.job_title = e.job_title
                JOIN gold.dim_technology tech ON CONVERT(e.primary_technology USING utf8mb4) COLLATE utf8mb4_unicode_ci = tech.technology_name
                WHERE e.primary_technology IS NOT NULL
            """)
            
            self.conn_gold.commit()
            logger.info("🎉 Toutes les tables de faits peuplées avec succès")
            
        except Exception as e:
            logger.error(f"Erreur lors du peuplement des faits: {e}")
            self.conn_gold.rollback()
            raise
    
    def generate_summary_report(self):
        """Génère un rapport de synthèse de l'entrepôt"""
        cursor = self.conn_gold.cursor(dictionary=True)
        
        print("\n" + "="*60)
        print("📊 RAPPORT DE SYNTHÈSE - DATA WAREHOUSE GOLD")
        print("="*60)
        
        # Compter les enregistrements par table
        tables = ['dim_calendar', 'dim_country', 'dim_company', 'dim_technology', 'dim_data_source', 'dim_job_role', 
                 'analysis_tech_activity', 'analysis_job_details', 'bridge_job_technologies']
        
        print("\n🗃️ CONTENU DES TABLES:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"{table:30} : {count:,} lignes")
        
        # Quelques analyses rapides
        print("\n📈 ANALYSES RAPIDES:")
        
        # Top technologies
        cursor.execute("""
            SELECT t.technology_name, 
                   COALESCE(SUM(fa.search_volume), 0) + COALESCE(SUM(fa.github_stars), 0) as total_activity
            FROM analysis_tech_activity fa
            JOIN dim_technology t ON fa.id_technology = t.id_technology
            GROUP BY t.id_technology, t.technology_name
            HAVING total_activity > 0
            ORDER BY total_activity DESC
            LIMIT 5
        """)
        
        print("\n🔥 Top 5 Technologies (activité combinée):")
        for row in cursor.fetchall():
            print(f"   {row['technology_name']:15} : {row['total_activity']:,}")
        
        # Top pays pour les jobs
        cursor.execute("""
            SELECT c.country_name, COUNT(*) as job_count
            FROM analysis_job_details fj
            JOIN dim_country c ON fj.id_country = c.id_country
            GROUP BY c.id_country, c.country_name
            ORDER BY job_count DESC
            LIMIT 5
        """)
        
        print("\n🌍 Top 5 Pays (nombre d'offres):")
        for row in cursor.fetchall():
            print(f"   {row['country_name']:15} : {row['job_count']} offres")

def main():
    """Fonction principale"""
    logger.info("🚀 Démarrage de la création du Data Warehouse Gold")
    
    dw = GoldDataWarehouse()
    
    try:
        # Connexion
        if not dw.connect():
            return False
        
        # Création des tables
        dw.create_gold_tables()
        
        # Peuplement des dimensions
        dw.populate_dimensions()
        
        # Peuplement des faits
        dw.populate_facts()
        
        # Rapport de synthèse
        dw.generate_summary_report()
        
        logger.info("✅ Data Warehouse Gold créé avec succès!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur: {e}")
        return False
    finally:
        dw.disconnect()

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)