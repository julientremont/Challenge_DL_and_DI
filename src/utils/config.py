import os
from typing import Optional


class Config:
    def __init__(self):
        self.mysql_host = os.getenv('MYSQL_HOST', 'localhost')
        self.mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
        self.mysql_database = os.getenv('MYSQL_DATABASE', 'silver')
        self.mysql_user = os.getenv('MYSQL_USER', 'tatane')
        self.mysql_password = os.getenv('MYSQL_PASSWORD', 'tatane')
        
        self.spark_driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '2g')
        self.spark_executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
        self.spark_executor_cores = int(os.getenv('SPARK_EXECUTOR_CORES', '2'))
        self.spark_ui_enabled = os.getenv('SPARK_UI_ENABLED', 'true').lower() == 'true'
        self.spark_ui_port = int(os.getenv('SPARK_UI_PORT', '4040'))
    
    @property
    def mysql_url(self) -> str:
        return f"mysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
    
    def get_spark_config(self) -> dict:
        return {
            'spark.driver.memory': self.spark_driver_memory,
            'spark.executor.memory': self.spark_executor_memory,
            'spark.executor.cores': str(self.spark_executor_cores),
            'spark.ui.enabled': str(self.spark_ui_enabled).lower(),
            'spark.ui.port': str(self.spark_ui_port)
        }


config = Config()