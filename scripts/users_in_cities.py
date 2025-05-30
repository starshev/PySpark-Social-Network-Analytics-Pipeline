from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import findspark
import math
import sys
import os

findspark.init()
os.environ['PYTHONPATH'] = '/.../local/lib/python3.8'
os.environ['HADOOP_CONF_DIR'] = '/.../hadoop/conf'
os.environ['PYSPARK_PYTHON'] = '/.../bin/python3'
os.environ['YARN_CONF_DIR'] = '/.../hadoop/conf'
os.environ['SPARK_HOME'] = '/.../lib/spark'
os.environ['JAVA_HOME'] = '/...'

# функция, которая возвращает ближайший город к месту события
def closest_city(lat, lon, cities) -> str:

    # алгоритм вычисления расстояния между точками на сфере
    def distance_measurer(lat1, lon1, lat2, lon2):

        r = 6371   # радиус Земли в км
        # преобразуем элементы формулы из градусов в радианы
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)

        # вычисляем расстояние
        a = (math.sin(delta_phi / 2) ** 2 +
             math.cos(phi1) * math.cos(phi2) *
             math.sin(delta_lambda / 2) ** 2)
        return 2 * r * math.asin(math.sqrt(a))

    # находим ближайший город
    closest_city = None
    min_distance = float('inf')
    
    for city in cities:
        distance = distance_measurer(lat, lon, city['lat'], city['lon'])
        if distance < min_distance:
            min_distance = distance
            closest_city = city['city']

    return closest_city

# возвращает датафрейм с сообщениями за всю историю (отправитель, таймстемп, город)
def messages_in_cities(source_path, geo_df, session) -> DataFrame:
             
        # сборщик городов и их координат в список словарей (для udf-функции)
        cities_list = [
                {'city' : row['city'], 'lat' : row['lat'], 'lon' : row['lon']} for row in geo_df.collect()
                ]
        # регистрируем пользовательскую функцию для использования в Spark
        city_finder = F.udf(lambda lat, lon: closest_city(lat, lon, cities_list), StringType())

        # читаем сообщения из таблицы событий в ODS
        messages = session.read.parquet(source_path).filter(F.col('event_type') == 'message') \
                .selectExpr('event.message_from as user_id', 'event.message_ts as message_ts', 'lat as lat', 'lon as lon') \
                .withColumn('message_ts', F.col('message_ts').cast('timestamp'))

        # добавляем поле с городом, в котором отправлено сообщение
        messages = messages.withColumn('city', city_finder('lat', 'lon')).drop('lat', 'lon')

        return messages

def main():

        date = sys.argv[1]   # дата расчёта витрины
        ods_path_events = sys.argv[2]   # корневая директория таблицы событий в ODS
        ods_path_geo_handbook = sys.argv[3]   # директория гео-справочника городов
        cdm_path_users_in_cities = sys.argv[4]   # корневая директория витрины о нахождении юзеров в городах

        spark = SparkSession.builder \
                .appName(f"users_in_cities/date={date}") \
                .master("yarn") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "4") \
                .config("spark.driver.cores", "4") \
                .config("spark.ui.port", "...") \
                .getOrCreate()
        
        # читаем гео-справочник городов
        geo = spark.read.parquet(ods_path_geo_handbook).drop('zone_id').cache()
        
        # получаем сообщения за всю историю (отправитель, таймстемп, город)
        messages = messages_in_cities(ods_path_events, geo, spark).cache()

        # вычисляем актуальный город для каждого пользователя (как город последнего сообщения)
        window = Window.partitionBy('user_id').orderBy(F.desc('message_ts'))
        act_cities = messages.withColumn('act_city', F.first('city').over(window)) \
                .withColumn('row_number', F.row_number().over(window)) \
                .filter(F.col('row_number') == 1) \
                .select('user_id', 'act_city', 'message_ts')
        # преобразуем время последнего сообщения в локальное время города
        act_cities = act_cities.join(geo, act_cities.act_city == geo.city, how = 'left').drop('city', 'lat', 'lon')
        act_cities = act_cities.withColumn('local_time', F.from_utc_timestamp('message_ts', F.col('tz'))).drop('message_ts', 'tz')
        
        # создаем проект витрины, добавляем в него уникальных юзеров, актуальный город и локальное время последнего сообщения
        users_in_cities = act_cities.select('user_id', 'act_city', 'local_time')

        # создаем вспомогательный датафрейм для вычисления домашнего города
        messages2 = messages.select('user_id', 'message_ts', F.date_trunc('day', F.col('message_ts')).alias('day'), 'city')
        window = Window.partitionBy('user_id').orderBy(F.desc('message_ts'))
        # вычисляем город предыдущего сообщения
        messages2 = messages2.withColumn('prev_city', F.lag('city').over(window))
        # маркируем как 0 если город тот же, или как 1, если город был другой
        messages2 = messages2.withColumn('new_or_same', F.when(F.col('prev_city').isNull(), 0) \
                .when(F.col('city') != F.col('prev_city'), 1) \
                .otherwise(0))
        # считаем нарастающую сумму маркеров, которая становится уникальной меткой каждой непрерывной последовательности городов
        messages2 = messages2.withColumn('seq_label', F.sum('new_or_same').over(window)).cache()
        # фильтруем последовательности городов, включающие более 27 календарных дней подряд
        home_cities = messages2.groupBy('user_id', 'city', 'seq_label') \
                .agg(F.countDistinct('day').alias('days_of_stay'), F.max('message_ts').alias('max_ts')) \
                .where('days_of_stay > 27') \
                .drop('seq_label', 'days_of_stay', 'prev_city', 'new_or_same')
        # для каждого пользователя оставляем только самую недавнюю последовательность, таким образом определяем домашний город
        window = Window.partitionBy('user_id').orderBy(F.desc('max_ts'))
        home_cities = home_cities.withColumn('date_rank', F.row_number().over(window)) \
                .filter(F.col('date_rank') == 1) \
                .drop('date_rank', 'max_ts') \
                .withColumnRenamed('city', 'home_city')
        
        # добавляем домашние города в проектируемую витрину
        users_in_cities = users_in_cities.join(home_cities, on = 'user_id', how = 'left').select('user_id', 'act_city', 'home_city', 'local_time')

        # считаем кол-во путешествий (смен города) для каждого пользователя
        city_changes = messages2.filter(F.col('new_or_same') == 1) \
                .groupBy('user_id') \
                .agg(F.count('*').alias('travel_count'))
        
        # добавляем метрику путешествий в проект витрины
        users_in_cities = users_in_cities.join(city_changes, on = 'user_id', how = 'left')

        # получаем массив городов, в которые перемещался каждый пользователь
        travel_array = messages2.filter(F.col('new_or_same') == 1) \
                .orderBy('message_ts') \
                .groupBy('user_id') \
                .agg(F.collect_list('city').alias('travel_array'))
        
        # добавляем массив в проект витрины
        users_in_cities = users_in_cities.join(travel_array, on = 'user_id', how = 'left')

        # выравниваем порядок полей в витрине
        users_in_cities = users_in_cities.select('user_id', 'act_city', 'home_city', 'travel_count', 'travel_array', 'local_time')
        # записываем витрину в CDM
        users_in_cities.write \
                .mode('overwrite') \
                .format('parquet') \
                .save(f'{cdm_path_users_in_cities}/date={date}')
        
        spark.stop()

if __name__ == "__main__":
        main()