from pyspark.sql.types import IntegerType, FloatType, DoubleType
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

# возвращает расстояние между двумя точками на сфере
def distance_measurer(lat1, lon1, lat2, lon2) -> float:

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

# возвращает id ближайшего города к месту события
def closest_city(lat, lon, cities) -> int:

    # находим ближайший город
    closest_city = None
    min_distance = float('inf')
    
    for city in cities:
        distance = distance_measurer(lat, lon, city['lat'], city['lon'])
        if distance < min_distance:
            min_distance = distance
            closest_city = city['zone_id']

    return closest_city

# возвращает датафрейм из гео-справочника городов 
def geo(geo_path, session) -> DataFrame:

    return session.read.parquet(geo_path)

# возвращает датафрейм с уникальными парами всех исторических собеседников
def collocutors_history(source_path, session) -> DataFrame:

    return session.read.parquet(source_path) \
        .filter(F.col('event_type') == 'message') \
        .selectExpr('event.message_from as message_from', 'event.message_to as message_to') \
        .distinct()

# возвращает уникальных пользователей за всю историю + их подписки на каналы + город, координаты и локальное время последнего сообщения
def compute_subscriptions_locations(source_path, geo_df, session) -> DataFrame:

    # сборщик городов и их координат в список словарей (для udf-функции)
    cities_list = [
        {'zone_id' : row['zone_id'], 'lat' : row['lat'], 'lon' : row['lon']} for row in geo_df.collect()
        ]
    # регистрируем udf-функцию для использования в Spark
    city_finder = F.udf(lambda lat, lon: closest_city(lat, lon, cities_list) if lat is not None and lon is not None else None, IntegerType())

    # читаем факты подписок за всю историю
    subscriptions = session.read.parquet(source_path).filter(F.col('event_type') == 'subscription')

    # получаем каналы, на которые подписан пользователь + дату и координаты последнего сообщения
    window = Window.partitionBy('event.user').orderBy(F.col('event.message_ts').desc())
    subscriptions = subscriptions.select(
        F.col('event.user').alias('user_id'), 
        F.col('event.subscription_channel').alias('channel_id'),
        F.first('event.message_ts').over(window).alias('last_message_ts'),
        F.first('lat').over(window).alias('last_message_lat'),
        F.first('lon').over(window).alias('last_message_lon')) \
        .distinct() \
        .filter((F.col('channel_id').isNotNull()) & (F.col('user_id').isNotNull())) \
        .withColumn('user_id', F.col('user_id').cast('long')) \
        .withColumn('last_message_ts', F.col('last_message_ts').cast('timestamp'))
    
    # группируем по уникальным пользователям и собираем каналы в массив
    subscriptions = subscriptions.groupBy('user_id') \
        .agg(
            F.collect_set('channel_id').alias('sub_channels_array'),
            F.first('last_message_ts').alias('last_message_ts'),
            F.first('last_message_lat').alias('last_message_lat'),
            F.first('last_message_lon').alias('last_message_lon')
        )

    # добавляем город последнего сообщения
    subscriptions = subscriptions.withColumn('city_id', city_finder('last_message_lat', 'last_message_lon'))

    # преобразуем время последнего сообщения в локальное время города
    subscriptions = subscriptions.join(geo_df, subscriptions.city_id == geo_df.zone_id, how = 'left').drop('lat', 'lon')
    subscriptions = subscriptions.withColumn('local_time', F.from_utc_timestamp('last_message_ts', F.col('tz'))).drop('last_message_ts', 'tz')
    subscriptions = subscriptions.select('user_id', 'sub_channels_array', 'city_id', 'last_message_lat', 'last_message_lon', 'local_time')

    return subscriptions

def main():

    date = sys.argv[1]   # дата расчёта витрины
    ods_path_events = sys.argv[2]   # корневая директория таблицы событий в ODS
    ods_path_geo_handbook = sys.argv[3]   # директория гео-справочника городов
    cdm_path_users_matches = sys.argv[4]   # корневая директория витрины о событиях в городах

    spark = SparkSession.builder \
        .appName(f"users_matches/date={date}") \
        .master("yarn") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.cores", "4") \
        .config("spark.ui.port", "...") \
        .getOrCreate()

    # регистрируем udf-функцию для вычисления расстояния между двумя точками на сфере
    distance_calculator = F.udf(distance_measurer, FloatType())

    # читаем гео-справочник городов
    geo_handbook = geo(ods_path_geo_handbook, spark).drop('city').cache()

    # получаем уникальные пары всех исторических собеседников
    collocutors = collocutors_history(ods_path_events, spark).cache()

    # уникальный пользователь + массив каналов, на которые он подписан + координаты, город и локальное время последнего отправленного сообщения
    sub_loc = compute_subscriptions_locations(ods_path_events, geo_handbook, spark).cache()
    
    # к каждому пользователю присоединяем каждого пользователя, если расстояние между координатами их последних отправленных сообщений < 1км
    # и если у них есть хотя бы один общий канал среди подписок
    udf_distance_measurer = F.udf(distance_measurer, DoubleType())
    sub_loc_pairs = sub_loc.select(F.col('user_id').alias('pair'), F.col('last_message_lat').alias('lm_lat'), 
                                   F.col('last_message_lon').alias('lm_lon'), F.col('sub_channels_array').alias('sub_chan_ar'))
    cohesive_subscribers = sub_loc.alias('a').crossJoin(sub_loc_pairs.alias('b')) \
        .withColumn('distance', udf_distance_measurer(F.col('a.last_message_lat'), F.col('a.last_message_lon'), 
                                                      F.col('b.lm_lat'), F.col('b.lm_lon'))) \
        .filter(
            (F.col('a.user_id') != F.col('b.pair')) &
            F.col('a.last_message_lat').isNotNull() &
            F.col('a.last_message_lon').isNotNull() &
            F.col('b.lm_lat').isNotNull() &
            F.col('b.lm_lon').isNotNull() &
            (F.col('distance') < 1) &
            (F.size(F.array_intersect(F.col('a.sub_channels_array'), F.col('b.sub_chan_ar'))) > 0)) \
        .selectExpr('a.user_id as user_left', 'b.pair as user_right', 'a.city_id as zone_id', 'a.local_time as local_time')
    
    # оставляем только те пары пользователей, у которых ранее не было переписки
    cohesive_subscribers = cohesive_subscribers.join(collocutors, 
        (F.col('user_left') == F.col('message_from')) & (F.col('user_right') == F.col('message_to')) |
        (F.col('user_left') == F.col('message_to')) & (F.col('user_right') == F.col('message_from')),
        how = 'left_anti')

    # собираем витрину
    users_matches = cohesive_subscribers.withColumn('processed_dttm', F.lit(date)) \
        .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')

    # записываем витрину в CDM
    users_matches.write \
            .mode('overwrite') \
            .format('parquet') \
            .save(f'{cdm_path_users_matches}/date={date}')
        
    spark.stop()

if __name__ == "__main__":
        main()