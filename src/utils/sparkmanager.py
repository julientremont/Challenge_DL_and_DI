import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from src.utils.config import config


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
            
            builder = SparkSession.builder.appName(self.app_name)
            
            for key, value in spark_config.items():
                builder = builder.config(key, value)

            builder  = builder.master("local[1]")
            
            builder = builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            builder = builder.config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
            builder = builder.config("spark.python.worker.faulthandler.enabled", "true")

            self._spark_session = builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            
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
    

    def is_session_active(self) -> bool:
        """Vérifie si la session Spark est active"""
        try:
            return (self._spark_session is not None and 
                   not self._spark_session.sparkContext._jsc.sc().isStopped())
        except:
            return False
    
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
    
    def __enter__(self):
        """Support du context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nettoyage automatique lors de la sortie du context manager"""
        self.stop_session()


spark_manager = SparkManager()
