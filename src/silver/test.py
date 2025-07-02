from pyspark.sql.functions import col, when, isnan, isnull, sum as spark_sum, lit
from pyspark.sql.types import NumericType, DoubleType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
from functools import reduce
import operator
import os

# Charger les variables d'environnement
load_dotenv()

# Variables d'environnement pour MySQL
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

keywords_techs = [
    # Langages de programmation populaires
    'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
    
    # Frameworks et bibliothèques web
    'react', 'angular', 'django',
]

def create_spark_session():
    """Crée une session Spark avec le driver MySQL via Maven"""
    return SparkSession.builder \
        .appName("GTrendsBronzeToSilver") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def test_mysql_driver(spark):
    """Test si le driver MySQL est bien chargé"""
    try:
        spark._jvm.Class.forName("com.mysql.cj.jdbc.Driver")
        print("✅ Driver MySQL chargé avec succès")
        return True
    except Exception as e:
        print(f"❌ Driver MySQL non trouvé: {e}")
        return False

def supprimer_lignes_vides(df, seuil_pct=0.7):
    """Supprime les lignes avec trop de valeurs nulles"""
    nb_colonnes = len(df.columns)
    null_conditions = []
    
    for c in df.columns:
        col_type = dict(df.dtypes)[c]
        if col_type.lower() in ['double', 'float']:
            null_condition = when(col(c).isNull() | isnan(col(c)), 1).otherwise(0)
        else:
            null_condition = when(col(c).isNull(), 1).otherwise(0)
        
        null_conditions.append(null_condition)
    
    sum_nulls = reduce(operator.add, null_conditions)
    df_with_null_count = df.withColumn("nb_nulls", sum_nulls)
    df_with_pct = df_with_null_count.withColumn(
        "pct_nulls", 
        col("nb_nulls") / nb_colonnes
    )
    df_filtre = df_with_pct.filter(col("pct_nulls") < seuil_pct)
    df_final = df_filtre.drop("nb_nulls", "pct_nulls")
    
    return df_final

def get_bronze_parquet(spark, output_path):
    """Lit un fichier Parquet avec la session Spark fournie"""
    return spark.read.parquet(output_path)

def merge_multiple_dataframes_reduce(df1, df2):
    """Fusionne deux DataFrames"""
    return df1.union(df2)

def write_to_mysql(spark, df, table_name, mode="append"):
    """Écrit un DataFrame vers MySQL"""
    try:
        print(f"💾 Écriture de {df.count()} lignes vers la table {table_name}...")
        
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}") \
            .option("dbtable", table_name) \
            .option("user", MYSQL_USER) \
            .option("password", MYSQL_PASSWORD) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode(mode) \
            .save()
        
        print(f"✅ Données écrites avec succès dans la table {table_name}")
        
    except Exception as e:
        print(f"❌ Erreur lors de l'écriture vers MySQL: {e}")
        raise

if __name__ == "__main__":
    spark = None
    try:
        print("🚀 Démarrage du traitement GTrends Bronze -> Silver")
        
        # 1. Création de la session Spark
        print("⚡ Création de la session Spark...")
        spark = create_spark_session()
        
        # 2. Test du driver MySQL
        if not test_mysql_driver(spark):
            raise Exception("Driver MySQL non disponible")
        
        # 3. Lecture des fichiers Parquet
        print(f"📖 Lecture de {len(keywords_techs)} fichiers Parquet...")
        dataframes = []
        
        for keyword in keywords_techs:
            output_path = f"./data/bronze/gtrends/FR/{keyword}"
            try:
                df = get_bronze_parquet(spark, output_path)
                count = df.count()
                dataframes.append(df)
                print(f"   ✓ {keyword}: {count} lignes")
            except Exception as e:
                print(f"   ✗ Erreur avec {keyword}: {e}")
        
        if not dataframes:
            raise Exception("Aucun DataFrame chargé!")
        
        # 4. Fusion des DataFrames
        print("🔄 Fusion des DataFrames...")
        merged_df = reduce(merge_multiple_dataframes_reduce, dataframes)
        total_rows = merged_df.count()
        print(f"   ✓ DataFrame fusionné: {total_rows} lignes")
        
        # 5. Nettoyage des données
        print("🧹 Nettoyage des données...")
        drop_df = merged_df.drop('isPartial', 'annee_insert', 'mois_insert', 'jour_insert') \
                          .dropDuplicates()
        
        rename_df = drop_df.withColumnRenamed('date', 'Date') \
                          .withColumnRenamed('search_frequency', 'Search frequency') \
                          .withColumnRenamed('country', 'Country code') \
                          .withColumnRenamed('keyword', 'Keyword')
        
        add_df = rename_df.withColumn('Country', lit('French'))
        final_df = supprimer_lignes_vides(add_df)
        
        final_count = final_df.count()
        print(f"   ✓ Données nettoyées: {final_count} lignes")
        
        # 6. Affichage du schéma pour vérification
        print("📊 Schéma final:")
        final_df.printSchema()
        
        # 7. Échantillon des données
        print("🔍 Échantillon des données:")
        final_df.show(5, truncate=False)
        
        # 8. Écriture vers MySQL
        write_to_mysql(spark, final_df, 'Trends_FR')
        print("✅ Traitement terminé avec succès!")
        
    except Exception as e:
        print(f"❌ Erreur durant le traitement: {e}")
        raise
    
    finally:
        # Arrêt de la session Spark
        if spark:
            spark.stop()
            print("🔌 Session Spark fermée")