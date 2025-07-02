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
        """Crée une nouvelle session Spark avec la configuration .env et gestion robuste des erreurs"""
        try:
            spark_config = self.config.get_spark_config()
            jar_path = r"C:\drivers\mysql-connector-java-8.0.33.jar"
            
            builder = SparkSession.builder.appName(self.app_name)
            
            for key, value in spark_config.items():
                builder = builder.config(key, value)

            builder = builder.master("local[1]")
            
            # Configuration du driver MySQL avec JAR local
            builder = builder.config("spark.jars", jar_path)
            builder = builder.config("spark.driver.extraClassPath", jar_path)
            builder = builder.config("spark.executor.extraClassPath", jar_path)
            
            # Alternative Maven (gardez cette ligne au cas où)
            builder = builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            
            # CONFIGURATION CRITIQUE POUR RÉSOUDRE L'ERREUR DE PARSING DE DATE
            builder = builder.config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            
            # Performances et adaptation
            builder = builder.config("spark.sql.adaptive.enabled", "true")
            builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            builder = builder.config("spark.sql.adaptive.skewJoin.enabled", "true")
            builder = builder.config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            
            # Sérialisation et performances
            builder = builder.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            builder = builder.config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
            builder = builder.config("spark.python.worker.faulthandler.enabled", "true")
            
            # GESTION ROBUSTE DES FICHIERS (Solution à votre erreur Parquet)
            builder = builder.config("spark.sql.files.ignoreCorruptFiles", "true")
            builder = builder.config("spark.sql.files.ignoreMissingFiles", "true")
            builder = builder.config("spark.sql.execution.pandas.convertToArrowArraySafely", "true")
            
            # GESTION MÉMOIRE ET STABILITÉ
            builder = builder.config("spark.driver.memory", "4g")
            builder = builder.config("spark.driver.maxResultSize", "2g")
            builder = builder.config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
            
            # OPTIMISATION PARQUET
            builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
            builder = builder.config("spark.sql.parquet.mergeSchema", "false")
            builder = builder.config("spark.sql.parquet.filterPushdown", "true")
            builder = builder.config("spark.sql.parquet.aggregatePushdown", "true")
            
            # OPTIMISATIONS ADDITIONNELLES
            builder = builder.config("spark.sql.broadcastTimeout", "36000")
            builder = builder.config("spark.network.timeout", "800s")
            builder = builder.config("spark.executor.heartbeatInterval", "60s")
            
            # CONFIGURATIONS ADDITIONNELLES POUR LA STABILITÉ
            builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "false")
            builder = builder.config("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
            
            self._spark_session = builder.getOrCreate()
            self._spark_session.sparkContext.setLogLevel("WARN")
            
            # VÉRIFICATIONS POST-CRÉATION
            # Test de vérification du driver MySQL
            try:
                self._spark_session._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
                self.logger.info("Driver MySQL chargé avec succès")
            except Exception as driver_error:
                self.logger.warning(f"Driver MySQL non trouvé: {driver_error}")
            
            # Vérification des configurations critiques
            critical_configs = [
                "spark.sql.files.ignoreCorruptFiles",
                "spark.sql.adaptive.enabled",
                "spark.serializer",
                "spark.sql.legacy.timeParserPolicy"
            ]
            
            for config in critical_configs:
                value = self._spark_session.conf.get(config, "non-défini")
                self.logger.info(f"{config}: {value}")
            
            # INFORMATIONS DE DEBUG
            self.logger.info(f"Session Spark créée avec succès: {self.app_name}")
            self.logger.info(f"Version Spark: {self._spark_session.version}")
            self.logger.info(f"Mémoire driver configurée: {self._spark_session.conf.get('spark.driver.memory', 'défaut')}")
            
            return self._spark_session
            
        except Exception as e:
            self.logger.error(f"Erreur lors de la création de la session Spark: {e}")
            self.logger.error("Suggestions de dépannage:")
            self.logger.error("1. Vérifiez que Java 8/11 est installé")
            self.logger.error("2. Vérifiez le chemin du JAR MySQL")
            self.logger.error("3. Vérifiez les variables d'environnement JAVA_HOME et SPARK_HOME")
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
        """Écrit un DataFrame en format Parquet avec gestion d'erreurs améliorée"""
        try:
            # Vérification préalable du DataFrame
            if df is None:
                raise ValueError("Le DataFrame est None")
            
            # Vérification que le DataFrame n'est pas vide
            if df.count() == 0:
                self.logger.warning("Le DataFrame est vide, aucune donnée à écrire")
                return
            
            writer = df.write.mode(mode)
            
            if partition_by:
                # Vérification que les colonnes de partitionnement existent
                df_columns = df.columns
                for col in partition_by:
                    if col not in df_columns:
                        self.logger.warning(f"Colonne de partitionnement '{col}' non trouvée. Colonnes disponibles: {df_columns}")
                        # Supprimer les colonnes inexistantes
                        partition_by = [c for c in partition_by if c in df_columns]
                
                if partition_by:  # S'il reste des colonnes valides
                    writer = writer.partitionBy(*partition_by)
            
            writer.parquet(path)
            self.logger.info(f"DataFrame écrit en Parquet: {path}")
            
        except Exception as e:
            self.logger.error(f"Erreur lors de l'écriture Parquet: {e}")
            self.logger.error(f"Chemin: {path}")
            if partition_by:
                self.logger.error(f"Partitions demandées: {partition_by}")
            
            # Tentative d'écriture sans partitionnement en cas d'échec
            if partition_by:
                self.logger.info("Tentative d'écriture sans partitionnement...")
                try:
                    df.write.mode(mode).parquet(path)
                    self.logger.info(f"Écriture réussie sans partitionnement: {path}")
                except Exception as e2:
                    self.logger.error(f"Échec même sans partitionnement: {e2}")
                    raise e2
            else:
                raise e
    
    def __enter__(self):
        """Support du context manager"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Nettoyage automatique lors de la sortie du context manager"""
        self.stop_session()


spark_manager = SparkManager()