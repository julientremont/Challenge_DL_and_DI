import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from .config import config

class SparkManager:
    """Gestionnaire de session Spark avec intégration MySQL et utilitaires de traitement de données"""
    
    def __init__(self, app_name: str = "DataProcessingApp"):
        self.config = config
        self.app_name = app_name
        self._spark_session: Optional[SparkSession] = None
        self._is_initialized = False
        self.logger = logging.getLogger(__name__)
        
    def get_session(self) -> SparkSession:
        """Retourne la session Spark, la crée si nécessaire"""
        if not self._is_initialized or not self.is_session_active():
            self._spark_session = self.create_session()
            self._is_initialized = True
        return self._spark_session
    
    def create_session(self) -> SparkSession:
        """Crée une nouvelle session Spark avec la configuration .env"""
        try:
            spark_config = self.config.get_spark_config()
            jar_path = r"C:\drivers\mysql-connector-java-8.0.33.jar"
            
            builder = SparkSession.builder.appName(self.app_name)
            
            for key, value in spark_config.items():
                builder = builder.config(key, value)
            
            # Configuration du driver MySQL avec JAR local
            builder = builder.config("spark.jars", jar_path)
            builder = builder.config("spark.driver.extraClassPath", jar_path)
            builder = builder.config("spark.executor.extraClassPath", jar_path)
            
            # Alternative Maven (gardez cette ligne au cas où)
            builder = builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            
            self._spark_session = builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            # Test de vérification du driver MySQL
            try:
                self._spark_session._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
                self.logger.info("✓ Driver MySQL chargé avec succès")
            except Exception as driver_error:
                self.logger.warning(f"⚠ Driver MySQL non trouvé: {driver_error}")
            
            self.logger.info(f"Session Spark créée avec succès: {self.app_name}")
            return self._spark_session
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la création de la session Spark: {e}")
            raise
    def stop_session(self) -> None:
        """Arrête la session Spark"""
        if self._spark_session:
            try:
                self._spark_session.stop()
                self.logger.info("Session Spark arrêtée")
            except Exception as e:
                self.logger.error(f"Erreur lors de l'arrêt de la session: {e}")
            finally:
                self._spark_session = None
                self._is_initialized = False
    
    def restart_session(self) -> SparkSession:
        """Redémarre la session Spark"""
        self.stop_session()
        return self.get_session()
    
    def is_session_active(self) -> bool:
        """Vérifie si la session Spark est active"""
        try:
            return (self._spark_session is not None and 
                   not self._spark_session.sparkContext._jsc.sc().isStopped())
        except:
            return False
    
    def read_from_mysql(self, query: str, table: Optional[str] = None) -> DataFrame:
        """Lit des données depuis MySQL via JDBC"""
        spark = self.get_session()
        
        jdbc_properties = {
            "user": self.config.mysql_user,
            "password": self.config.mysql_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        jdbc_url = f"jdbc:mysql://{self.config.mysql_host}:{self.config.mysql_port}/{self.config.mysql_database}"
        
        try:
            if query:
                # Lecture avec requête personnalisée
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("query", query) \
                    .option("user", self.config.mysql_user) \
                    .option("password", self.config.mysql_password) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .load()
            elif table:
                # Lecture d'une table complète
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table) \
                    .option("user", self.config.mysql_user) \
                    .option("password", self.config.mysql_password) \
                    .option("driver", "com.mysql.cj.jdbc.Driver") \
                    .load()
            else:
                raise ValueError("Soit 'query' soit 'table' doit être spécifié")
            
            self.logger.info(f"Données lues depuis MySQL: {df.count()} lignes")
            return df
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture MySQL: {e}")
            raise
    
    def write_to_mysql(self, df: DataFrame, table: str, mode: str = "append") -> None:
        """Écrit un DataFrame vers MySQL"""
        jdbc_url = f"jdbc:mysql://{self.config.mysql_host}:{self.config.mysql_port}/{self.config.mysql_database}"
        
        jdbc_properties = {
            "user": self.config.mysql_user,
            "password": self.config.mysql_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        try:
            df.write.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table) \
                .option("user", self.config.mysql_user) \
                .option("password", self.config.mysql_password) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode(mode) \
                .save()
            
            self.logger.info(f"Données écrites vers MySQL table '{table}': {df.count()} lignes")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture MySQL: {e}")
            raise
    
    def read_parquet(self, path: str) -> DataFrame:
        """Lit un fichier Parquet"""
        try:
            spark = self.get_session()
            df = spark.read.parquet(path)
            self.logger.info(f"Fichier Parquet lu: {path}, {df.count()} lignes")
            return df
        except Exception as e:
            self.logger.error(f"Erreur lors de la lecture Parquet: {e}")
            raise
    
    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite", 
                     partition_by: Optional[list] = None) -> None:
        """Écrit un DataFrame en format Parquet"""
        try:
            writer = df.write.mode(mode)
            
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            
            writer.parquet(path)
            self.logger.info(f"DataFrame écrit en Parquet: {path}")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture Parquet: {e}")
            raise
    
    def validate_dataframe(self, df: DataFrame, required_columns: list) -> bool:
        """Valide qu'un DataFrame contient les colonnes requises"""
        df_columns = set(df.columns)
        required_columns_set = set(required_columns)
        
        missing_columns = required_columns_set - df_columns
        
        if missing_columns:
            self.logger.error(f"Colonnes manquantes: {missing_columns}")
            return False
        
        self.logger.info("Validation DataFrame réussie")
        return True
    
    def get_session_info(self) -> Dict[str, Any]:
        """Retourne les informations de la session Spark"""
        if not self.is_session_active():
            return {"status": "inactive"}
        
        spark = self.get_session()
        return {
            "status": "active",
            "app_name": spark.sparkContext.appName,
            "app_id": spark.sparkContext.applicationId,
            "master": spark.sparkContext.master,
            "ui_enabled": self.config.spark_ui_enabled,
            "ui_port": self.config.spark_ui_port if self.config.spark_ui_enabled else None,
            "driver_memory": self.config.spark_driver_memory,
            "executor_memory": self.config.spark_executor_memory,
            "executor_cores": self.config.spark_executor_cores
        }
    
    def __enter__(self):
        """Support du context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nettoyage automatique lors de la sortie du context manager"""
        self.stop_session()


# Instance globale pour faciliter l'utilisation
spark_manager = SparkManager()
