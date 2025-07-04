from pyspark.sql import SparkSession
import requests
from src.utils.sparkmanager import spark_manager



def get_api():
    country_list = ["austria", "belgium", "bulgaria", "croatia", "cyprus", "czechia", "denmark", "estonia", "finland",
                    "france", "germany", "greece", "hungary", "ireland", "italy", "latvia", "lithuania", "netherlands",
                    "norway", "poland", "portugal", "romania", "serbia", "slovakia", "slovenia", "spain", "sweden",
                    "switzerland", "uk"]

    tot_response = []
    for country in country_list:
        url = f"https://jobicy.com/api/v2/remote-jobs?geo={country}&industry=engineering"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers)

        response_json = response.json()["jobs"]
        tot_response.extend(response_json)

    with spark_manager as sm:
        spark = sm.get_session()
        df = spark.createDataFrame(tot_response)
        spark_manager.write_parquet(df, "data/bronze/jobicy/", mode="overwrite")


if __name__ == "__main__":
    get_api()