from datetime import datetime as dt, timedelta as td
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import findspark
import sys
import os

findspark.init()
os.environ['PYTHONPATH'] = '/.../local/lib/python3.8'
os.environ['HADOOP_CONF_DIR'] = '/.../hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/.../bin/python3'
os.environ['YARN_CONF_DIR'] = '/.../hadoop/conf'
os.environ['SPARK_HOME'] = '/.../lib/spark'
os.environ['JAVA_HOME'] = '/...'

def main():

        spark = SparkSession.builder \
                .appName("events_stg_to_ods") \
                .master("yarn") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "4") \
                .config("spark.driver.cores", "4") \
                .config("spark.ui.port", "...") \
                .getOrCreate()

        # дата, за которую получаем инкремент (прошедший день)
        date = (dt.strptime(sys.argv[1], '%Y-%m-%d') - td(days = 1)).strftime('%Y-%m-%d')
        stg_path_events = sys.argv[2]   # корневая директория таблицы событий в STG
        ods_path_events = sys.argv[3]   # корневая директория таблицы событий в ODS
        
        # читаем инкремент из STG
        events = spark.read.parquet(f'{stg_path_events}/date={date}')
  
        # записываем инкремент в ODS
        events.write \
                .partitionBy('event_type') \
                .mode('overwrite') \
                .format('parquet') \
                .save(f'{ods_path_events}/date={date}')
        
        spark.stop()

if __name__ == "__main__":
        main()