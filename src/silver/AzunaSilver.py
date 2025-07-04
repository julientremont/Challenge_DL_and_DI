import sys

from pyspark.sql.functions import col, when, isnan
from pyspark.sql.types import NumericType, DoubleType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
from functools import reduce
import operator
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager
from src.utils.sqlmanager import sql_manager

# Charger les variables d'environnement
load_dotenv()

pays_europeens = {
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

jdbc_url = "jdbc:mysql://localhost:3306/silver"
properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

def supprimer_lignes_vides(df, seuil_pct=0.9):
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

def get_bronze_parquet(spark,output_path):
    return spark.read.parquet(output_path)

def merge_multiple_dataframes_reduce(df1, df2):
    return df1.union(df2)

# Créer une session avec téléchargement automatique du driver
def create_spark_session():
    return SparkSession.builder.appName("GTrendsBronzeToSilver") \
        .config("spark.jars", "mysql-connector-j-9.3.0.jar") \
        .getOrCreate()

def post_dataframe_mysql(spark, df, table_name):
    try:
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
        print(f"Données sauvegardées dans {table_name}")
    except Exception as e:
        print(f"Erreur : {e}")
        raise
    
dataframes = []
spark = spark_manager.create_session()
for key in pays_europeens.keys():
    output_path = f"./data/bronze/azuna/Salary/{key}"
    df = get_bronze_parquet(spark,output_path)
    dataframes.append(df)
    merged_df = reduce(merge_multiple_dataframes_reduce, dataframes)

drop_df = merged_df.drop('annee_insertion', 'mois_insertion') \
            .dropDuplicates()
rename_df = drop_df.withColumnRenamed('date', 'Date') \
                   .withColumnRenamed('average_salary', 'Average salary') \
                     .withColumnRenamed('keyword', 'Job title')
for key in pays_europeens.keys():
    add_df = rename_df.withColumn('Country code', upper(lit(key)))
    add_df = add_df.withColumn('processed_at', current_timestamp())
final_df = supprimer_lignes_vides(add_df)

post_dataframe_mysql(spark, final_df, 'adzuna_jobs_silver')

dataframes2 = []
spark = spark_manager.create_session()
for key in pays_europeens.keys():
    output_path = f"./data/bronze/azuna/Dispertion/{key}"
    df = get_bronze_parquet(spark,output_path)
    dataframes2.append(df)
    merged_df = reduce(merge_multiple_dataframes_reduce, dataframes2)

drop_df = merged_df.drop('annee_insertion', 'mois_insertion','average_salary') \
            .dropDuplicates()
rename_df = drop_df.withColumnRenamed('date', 'Date') \
                   .withColumnRenamed('country', 'Country') \
                    .withColumnRenamed('keyword', 'Job title')\
                    .withColumnRenamed('salary_range', 'Salary range')\
                    .withColumnRenamed('job_count', 'Job count')\
                     
for key in pays_europeens.keys():
    add_df = rename_df.withColumn('Country code', upper(lit(key)))
    add_df = add_df.withColumn('processed_at', current_timestamp())
dropnull_df = supprimer_lignes_vides(add_df)
dropnull_df = dropnull_df.dropna(subset=["Salary range"])
final_df = dropnull_df.dropna(subset=['Job count'])


post_dataframe_mysql(spark, final_df, 'adzuna_dispertion_jobs_silver')