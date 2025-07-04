# Tech Market Data Pipeline

## Participants

Ethan TOMASO            ethan.tomaso@efrei.net
Antoine VANDEPLANQUE    antoine.vandeplanque@efrei.net
Elliot FESQUET          elliot.fesquet@efrei.net
Julien TREMONT-RAIMI    julien.tremont-raimi@efrei.net

## Lien du document

lien : https://docs.google.com/document/d/1EOfA2tu8fYubmOR61WkQ9J_R52FHTkS33tC6r0z90lI/edit?usp=sharing

## Vue d'ensemble

Pipeline de données complet pour analyser les tendances du marché technologique européen. Ce projet collecte, traite et unifie des données provenant de multiples sources pour fournir des insights sur les technologies populaires, les offres d'emploi et les tendances du marché.

## Architecture

### Structure des données (Bronze → Silver → Gold)

```
Bronze Layer (Données brutes)
├── EuroTechJobs (offres d'emploi)
├── GitHub Repos (popularité des technologies)
├── StackOverflow Survey (préférences développeurs)
├── Adzuna Jobs (salaires et emplois)
└── Google Trends (tendances de recherche)

Silver Layer (Données nettoyées)
├── Normalisation des données
├── Déduplication
├── Validation de qualité
└── Stockage MySQL

Gold Layer (Données analysables)
├── Entrepôt de données unifié
├── Tables de dimensions
├── Tables de faits
└── Métriques business
```

## Sources de données

### 1. EuroTechJobs
- **Objectif** : Offres d'emploi tech européennes
- **Données** : Titres de postes, entreprises, technologies, localisations
- **Fréquence** : Collecte périodique

### 2. GitHub Repositories
- **Objectif** : Popularité des technologies open source
- **Données** : Stars, forks, activité des projets
- **Couverture** : Langages et frameworks populaires

### 3. StackOverflow Survey
- **Objectif** : Préférences et tendances des développeurs
- **Données** : Technologies utilisées, salaires, démographie
- **Années** : 2021-2024

### 4. Adzuna Jobs API
- **Objectif** : Données salariales et volume d'emplois
- **Couverture** : 10 pays européens
- **Métriques** : Salaires moyens, distribution

### 5. Google Trends
- **Objectif** : Intérêt de recherche pour les technologies
- **Données** : Volume de recherche par pays et technologie
- **Granularité** : Données quotidiennes

## Installation et Configuration

### Prérequis
```bash
# Dépendances Python
pip install -r requirements.txt

# Base de données MySQL
# Spark (pour le traitement des données)
# Accès Internet pour les APIs
```

### Variables d'environnement
```bash
# MySQL
MYSQL_HOST=localhost
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_PORT=3306

# APIs
ADZUNA_APP_ID=your_app_id
ADZUNA_APP_KEY=your_app_key
```

## Utilisation

### 1. Collecte des données (Bronze)
```bash
# Collecte EuroTechJobs
python src/bronze/import_eurotechjobs.py

# Collecte GitHub
python src/bronze/import_github_repos.py

# Collecte StackOverflow
python src/bronze/import_stackoverflow_survey.py

# Collecte Adzuna
python src/bronze/importAdzuna.py

# Collecte Google Trends
python src/bronze/importGtrends.py
```

### 2. Traitement et nettoyage (Silver)
```bash
# Traitement des données
python src/silver/eurotechjobs_silver.py
python src/silver/github_repos_silver.py
python src/silver/stackoverflow_survey_silver.py
python src/silver/adzuna_silver.py
python src/silver/trends_silver.py
```

### 3. Création de l'entrepôt (Gold)
```bash
# Unification des données
python src/gold/table_unification.py
```

## Schéma de données

### Tables Silver (Données nettoyées)
```sql
-- Table des tendances de recherche
CREATE TABLE silver.Trends (
    id INT AUTO_INCREMENT PRIMARY KEY,
    country_code VARCHAR(2) NOT NULL,
    date DATE NOT NULL,
    keyword VARCHAR(100) NOT NULL,
    search_frequency INT NOT NULL,
    country VARCHAR(50) NOT NULL,
    INDEX idx_country_code (country_code),
    INDEX idx_date (date),
    INDEX idx_keyword (keyword)
);

-- Table des salaires moyens
CREATE TABLE average_salaries (
    id INT PRIMARY KEY AUTO_INCREMENT,
    average_salary DECIMAL(10,2) NOT NULL,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    job_title VARCHAR(100) NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

-- Table des statistiques d'emploi
CREATE TABLE job_statistics (
    id INT PRIMARY KEY AUTO_INCREMENT,
    country VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    job_title VARCHAR(100) NOT NULL,
    job_count DECIMAL(10,1) NOT NULL,
    salary_range INT NOT NULL,
    country_code VARCHAR(10) NOT NULL,
    processed_at TIMESTAMP NOT NULL
);

-- Index pour optimiser les requêtes
CREATE INDEX idx_avg_salaries_country_date ON average_salaries(country, date);
CREATE INDEX idx_avg_salaries_job_title ON average_salaries(job_title);
CREATE INDEX idx_job_stats_country_date ON job_statistics(country, date);
CREATE INDEX idx_job_stats_salary_range ON job_statistics(salary_range);
```

### Tables de dimensions (Gold)
- `dim_technology` : Catalogue des technologies
- `dim_country` : Pays européens
- `dim_company` : Entreprises
- `dim_job_role` : Rôles et métiers
- `dim_calendar` : Dimensions temporelles

### Tables de faits (Gold)
- `analysis_tech_activity` : Activité des technologies (popularité, emplois)
- `analysis_job_details` : Détails des offres d'emploi
- `bridge_job_technologies` : Relations emploi-technologies

## Qualité des données

### Métriques de qualité
- Score de qualité par enregistrement (0-100)
- Validation des données obligatoires
- Détection des doublons
- Nettoyage des valeurs aberrantes

### Contrôles automatiques
- Validation des salaires (1K-1M USD)
- Normalisation des noms de technologies
- Mapping des pays vers codes ISO
- Déduplication sur URLs et IDs

## Technologies utilisées

### Traitement des données
- **Apache Spark** : Traitement big data
- **PySpark** : Interface Python pour Spark
- **Pandas** : Manipulation de données

### Stockage
- **MySQL** : Base de données relationnelle
- **Parquet** : Format de stockage colonnaire

### APIs et collecte
- **Requests** : Appels API
- **BeautifulSoup** : Web scraping
- **PyTrends** : Google Trends API

## Exemples d'analyses

### Technologies les plus populaires
```sql
SELECT t.technology_name, 
       SUM(fa.search_volume) as total_searches,
       AVG(fa.github_stars) as avg_stars
FROM analysis_tech_activity fa
JOIN dim_technology t ON fa.id_technology = t.id_technology
GROUP BY t.technology_name
ORDER BY total_searches DESC;
```

### Évolution des salaires par pays
```sql
SELECT c.country_name, 
       DATE_FORMAT(fj.date_key, '%Y-%m') as month,
       AVG(fj.salary_usd) as avg_salary
FROM analysis_job_details fj
JOIN dim_country c ON fj.id_country = c.id_country
WHERE fj.salary_usd IS NOT NULL
GROUP BY c.country_name, month
ORDER BY month DESC;
```

## Monitoring et logs

### Système de logging
- Logs détaillés pour chaque étape
- Suivi des erreurs et exceptions
- Métriques de performance

### Alertes
- Échec de collecte de données
- Problèmes de qualité des données
- Erreurs de traitement

## Contribution

### Structure du code
```
src/
├── bronze/          # Collecte des données brutes
├── silver/          # Nettoyage et normalisation
├── gold/            # Entrepôt de données unifié
├── utils/           # Utilitaires (Spark, MySQL)
└── api/             # Interface API (optionnel)
```

### Standards de qualité
- Code documenté et testé
- Gestion d'erreurs robuste
- Configuration centralisée
- Logs structurés

## Limitations

- Dépendant de la disponibilité des APIs externes
- Volumes de données limités par les quotas API
- Données historiques limitées pour certaines sources
- Couverture géographique centrée sur l'Europe

## Roadmap

- [ ] Ajout d'autres sources de données (LinkedIn, Indeed)
- [ ] Interface de visualisation (dashboard)
- [ ] Prédictions et modèles ML
- [ ] Automatisation complète (scheduling)
- [ ] API REST pour consultation des données

## Support

Pour toute question ou problème, veuillez créer une issue dans le repository ou contacter l'équipe de développement.