from datetime import datetime as dt, timedelta as td
from pyspark.sql.types import IntegerType
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

# возвращает список директорий за нужную глубину (по всем событиям)
def input_paths(given_date, depth, base_path) -> list:

    input_paths = []
    date_point = dt.strptime(given_date, '%Y-%m-%d')
    for i in range(depth):
        date_point -= td(days = 1)
        date_string = f"{base_path}/date={dt.strftime(date_point, '%Y-%m-%d')}"
        input_paths.append(date_string)

    return input_paths

# возвращает список директорий за нужную глубину (только по сообщениям)
def input_paths_messages(given_date, depth, base_path) -> list:

    input_paths = []
    date_point = dt.strptime(given_date, '%Y-%m-%d')
    for i in range(depth):
        date_point -= td(days = 1)
        date_string = f"{base_path}/date={dt.strftime(date_point, '%Y-%m-%d')}/event_type=message"
        input_paths.append(date_string)

    return input_paths

# возвращает zone_id ближайшего города к месту события
def closest_city(lat, lon, cities) -> int:

    # возвращает расстояние между точками на сфере
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
            closest_city = city['zone_id']

    return closest_city

# возвращает датафрейм из гео-справочника городов 
def geo(geo_path, session) -> DataFrame:

    return session.read.parquet(geo_path)

# возвращает датафрейм с кол-вами сообщений, реакций и подписок за нужную историческую глубину по городам
def mes_rea_sub_stats(run_date, depth, source_path, geo_df, session) -> DataFrame:
             
    # сборщик городов и их координат в список словарей (для udf-функции)
    cities_list = [
        {'zone_id' : row['zone_id'], 'lat' : row['lat'], 'lon' : row['lon']} for row in geo_df.collect()
        ]
    # регистрируем udf-функцию для использования в Spark
    city_finder = F.udf(lambda lat, lon: closest_city(lat, lon, cities_list) if lat is not None and lon is not None else None, IntegerType())        

    # читаем события и их координаты за нужную историческую глубину + добавляем зону (город события)
    paths = input_paths(run_date, depth, source_path)
    events = session.read.option('basePath', source_path).parquet(*paths).selectExpr('event_type as event_type', 'lat as lat', 'lon as lon') \
        .withColumn('zone_id', city_finder('lat', 'lon')).drop('lat', 'lon')

    # считаем кол-ва событий в городах
    events = events.groupBy('zone_id', 'event_type').agg(F.count('*').alias('counts'))

    # разворачиваем статистику горизонтально + добавляем нулевые колонки для событий (на случай их отсутствия в данных)
    events = events.groupBy('zone_id').pivot('event_type').agg(F.first('counts'))
    if 'message' not in events.columns:
        events = events.withColumn('message', F.lit(0))
    if 'reaction' not in events.columns:
        events = events.withColumn('reaction', F.lit(0))
    if 'subscription' not in events.columns:
        events = events.withColumn('subscription', F.lit(0))

    return events

# возвращает датафрейм с кол-вами новых пользователей за нужную историческую глубину по городам
def new_users(run_date, depth, source_path, geo_df, session) -> DataFrame:

    # сборщик городов и их координат в список словарей (для udf-функции)
    cities_list = [
        {'zone_id' : row['zone_id'], 'lat' : row['lat'], 'lon' : row['lon']} for row in geo_df.collect()
        ]
    # регистрируем udf-функцию для использования в Spark
    city_finder = F.udf(lambda lat, lon: closest_city(lat, lon, cities_list) if lat is not None and lon is not None else None, IntegerType())

    # получаем данные по сообщениям за нужную историческую глубину
    paths = input_paths_messages(run_date, depth, source_path)
    messages = session.read.option('basePath', source_path).parquet(*paths) \
        .selectExpr('event.message_from as user_id', 'event.message_ts as message_ts', 'lat as lat', 'lon as lon') \
        .filter('user_id is not null')

    # оставляем только первые сообщения пользователей + добавляем зону (город) первого сообщения
    window = Window.partitionBy('user_id').orderBy(F.asc('message_ts'))
    messages = messages.withColumn('row_number', F.row_number().over(window)) \
            .filter(F.col('row_number') == 1) \
            .drop('row_number', 'message_ts')
            
    # добавляем город первого сообщения
    messages = messages.withColumn('zone_id', city_finder('lat', 'lon')).drop('lat', 'lon')

    # получаем лонг-лист всех исторических пользователей, за вычетом периода расчета витрины
    history_threshold = (dt.strptime(run_date, "%Y-%m-%d") - td(days = depth)).strftime("%Y-%m-%d")
    history_users = session.read.parquet(source_path) \
        .where(F.col('date') < history_threshold)
    history_users = history_users.selectExpr('event.message_from as user_id') \
        .union(history_users.selectExpr('event.message_to as user_id')) \
        .union(history_users.selectExpr('event.user as user_id').withColumn('user_id', F.col('user_id').cast('long'))) \
        .filter('user_id is not null') \
        .distinct()

    # получаем новых пользователей за прошедшую неделю по городам
    messages = messages.join(history_users, on = 'user_id', how = 'left_anti')
    registered_users = messages.distinct().groupBy('zone_id').agg(F.count('user_id').alias('user'))

    return registered_users

def main():

        date = sys.argv[1]   # дата расчёта витрины
        ods_path_events = sys.argv[2]   # корневая директория таблицы событий в ODS
        ods_path_geo_handbook = sys.argv[3]   # директория гео-справочника городов
        cdm_path_events_in_cities = sys.argv[4]   # корневая директория витрины о событиях в городах

        spark = SparkSession.builder \
                .appName(f"events_in_cities/date={date}") \
                .master("yarn") \
                .config("spark.executor.memory", "4g") \
                .config("spark.executor.cores", "4") \
                .config("spark.driver.cores", "4") \
                .config("spark.ui.port", "...") \
                .getOrCreate()

        # читаем гео-справочник городов
        geo_handbook = geo(ods_path_geo_handbook, spark).drop('city').cache()

        # получаем статистику по сообщениям, реакциям и подпискам в городах за прошедшую неделю
        mes_rea_sub_7 = mes_rea_sub_stats(date, 7, ods_path_events, geo_handbook, spark).withColumnRenamed('message', 'week_message') \
            .withColumnRenamed('reaction', 'week_reaction') \
            .withColumnRenamed('subscription', 'week_subscription') \
            .cache()

        # получаем статистику по сообщениям, реакциям и подпискам в городах за прошедший месяц
        mes_rea_sub_30 = mes_rea_sub_stats(date, 30, ods_path_events, geo_handbook, spark).withColumnRenamed('message', 'month_message') \
            .withColumnRenamed('reaction', 'month_reaction') \
            .withColumnRenamed('subscription', 'month_subscription') \
            .cache()
        
        # получаем кол-во новых пользователей за прошедшую неделю по городам
        new_users_7 = new_users(date, 7, ods_path_events, geo_handbook, spark).withColumnRenamed('user', 'week_user').cache()

        # получаем кол-во новых пользователей за прошедший месяц по городам
        new_users_30 = new_users(date, 30, ods_path_events, geo_handbook, spark).withColumnRenamed('user', 'month_user').cache()

        # собираем витрину
        events_in_cities = geo_handbook.select('zone_id').join(mes_rea_sub_7, on = 'zone_id', how = 'left') \
            .join(new_users_7, on = 'zone_id', how = 'left') \
            .join(mes_rea_sub_30, on = 'zone_id', how = 'left') \
            .join(new_users_30, on = 'zone_id', how = 'left') \
            .withColumn('month', F.month(F.to_date(F.lit(date)))) \
            .withColumn('week', F.weekofyear(F.to_date(F.lit(date))))
        
        # выравниваем порядок полей + заполняем NaN нулями
        events_in_cities = events_in_cities.select('month', 'week', 'zone_id', 'week_message', 'week_reaction', 'week_subscription', 'week_user', \
                                                   'month_message', 'month_subscription', 'month_reaction', 'month_user').fillna(0)
        
        # записываем витрину в CDM
        events_in_cities.write \
                .mode('overwrite') \
                .format('parquet') \
                .save(f'{cdm_path_events_in_cities}/date={date}')
        
        spark.stop()

if __name__ == "__main__":
    main()