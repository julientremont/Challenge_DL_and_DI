from pyspark.sql.functions import col, when, isnan, isnull, sum as spark_sum
from pyspark.sql.types import NumericType, DoubleType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv
from functools import reduce
import operator
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.utils.sparkmanager import spark_manager

# Charger les variables d'environnement
load_dotenv()

keywords_techs = [
    # Langages de programmation populaires
    'python', 'javascript', 'java', 'c++', 'c#', 'php', 'ruby', 'go', 'rust', 'kotlin',
    'swift', 'typescript', 'scala', 'perl', 'r programming', 'matlab', 'dart', 'elixir',
    
    # Frameworks et bibliothèques web
    'react', 'angular', 'django',
]

country_codes = ["FR","AT", "BE" "CH", "DE", "ES", "GB", "IT", "NL", "PL",]


jdbc_url = "jdbc:mysql://localhost:3306/silver"
properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver",
}

def supprimer_lignes_vides(df, seuil_pct=0.7):
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
spark = create_spark_session()
for key in country_codes:
    for keyword in keywords_techs:
        output_path = f"./data/bronze/gtrends/FR/{keyword}"
        df = get_bronze_parquet(spark,output_path)
        dataframes.append(df)
        add_df = dataframes.withColumn('Country', lit(key))
        merged_df = reduce(merge_multiple_dataframes_reduce, dataframes)

drop_df = merged_df.drop('isPartial', 'annee_insert', 'mois_insert', 'jour_insert') \
            .dropDuplicates()
rename_df = drop_df.withColumnRenamed('date', 'Date') \
                   .withColumnRenamed('search_frequency', 'Search frequency') \
                     .withColumnRenamed('country', 'Country code')\
                     .withColumnRenamed('keyword', 'Keyword')

dropnull_df = rename_df.dropna(subset=["Search frequency"])
final_df = supprimer_lignes_vides(dropnull_df)
post_dataframe_mysql(spark, final_df, 'Trends_FR')

