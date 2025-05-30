## Data Catalog

#### STG

- таблица событий  <br>
директория: /.../data/geo/**events** <br>
формат: PARQUET <br>
партиции: .../**date**=YYYY-MM-DD/ <br>

#### ODS

- таблица событий  <br>
директория: /.../data/**events** <br>
формат: PARQUET <br>
партиции: .../**date**=YYYY-MM-DD/**event_type**=... <br>
периодичность расчёта: 1 раз в сутки, инкрементально за прошедший день

#### CDM

- статический гео-справочник с координатами городов <br>
директория: /.../data/**geo_handbook** <br>
формат: PARQUET <br>
периодичность расчёта: разовая загрузка из CSV, обновляется по требованию <br>
**zone_id** - идентификатор города - integer <br>
**city** - город - string <br>
**lat** - широта - double <br>
**lon** - долгота - double <br>
**tz** - таймзона - string

- витрина о нахождении юзеров в городах <br>
директория: /.../data/analytics/**users_in_cities** <br>
формат: PARQUET <br>
партиции: .../**date**=YYYY-MM-DD <br>
периодичность расчёта: 1 раз в сутки (на данных за всю историю) <br>
**user_id** - идентификатор пользователя - string <br>
**act_city** - актуальный город (из которого отправлено последнее сообщение) - string <br>
**home_city** - домашний город - string <br>
**travel_count** - кол-во посещенных городов - integer <br>
**travel_array** - массив посещенных городов в порядке посещения - array, element: string <br>
**local_time** - местное время последнего сообщения - timestamp

- витрина о кол-вах событий в городах <br>
директория: /.../data/analytics/**events_in_cities** <br>
формат: PARQUET <br>
партиции: .../**date**=YYYY-MM-DD <br>
периодичность расчёта: 1 раз в сутки (за прошедшие 7 дней + за прошедшие 30 дней) <br>
**month** - месяц расчёта витрины (номер месяца года) - integer <br>
**week** - неделя расчёта витрины (номер недели год) - integer <br>
**zone_id** - идентификатор города - integer <br>
**week_message** - кол-во сообщений за неделю - integer <br>
**week_reaction** - кол-во реакций за неделю - integer <br>
**week_subscription** - кол-во подписок за неделю - integer <br>
**week_user** - кол-во регистраций за неделю - integer <br>
**month_message** - кол-во сообщений за месяц - integer <br>
**month_reaction** - кол-во реакций за месяц - integer <br>
**month_subscription** - кол-во подписок за месяц - integer <br>
**month_user** - кол-во регистраций за месяц - integer

- витрина для рекомендации друзей <br>
директория: /.../data/analytics/**users_matches** <br>
формат: PARQUET <br>
партиции: .../**date**=YYYY-MM-DD
периодичность расчёта: 1 раз в сутки (на данных за всю историю) <br>
**user_left** - первый пользователь - long <br>
**user_right** - второй пользователь - long <br>
**processed_dttm** - дата расчёта витрины - string <br>
**zone_id** - идентификатор города последнего сообщения - integer <br>
**local_time** - местное время последнего сообщения - timestamp

#### файлы пайплайна

/dags/**au_analytics_dag.py** - Airflow DAG <br>
/scripts/**events_stg_to_ods.py** - джоба Spark для инкрементальной загрузки событий из STG в ODS <br>
/scripts/**users_in_cities.py** - джоба Spark для расчёта витрины о нахождении юзеров в городах <br>
/scripts/**events_in_cities.py** - джоба Spark для расчёта витрины о кол-вах событий в городах <br>
/scripts/**users_matches.py** - джоба Spark для расчёта витрины для рекомендации друзей