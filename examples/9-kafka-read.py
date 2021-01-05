#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Project: spd-sxmcc
"""
@file: 9-kafka-read.py
@author: lyndon
@time: Created on 2021-01-04 18:14
@env: Python
@desc:
@ref:
@blog:
"""

import os
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, TableConfig, DataTypes, CsvTableSink, WriteMode, \
    SqlDialect
from pyflink.table.descriptors import Schema, Kafka, Json, Rowtime

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
s_env.set_parallelism(1)

# 必须开启checkpoint，时间间隔为毫秒，否则不能输出数据
s_env.enable_checkpointing(3000)

# st_env = StreamTableEnvironment.create(s_env, TableConfig())
env_settings = EnvironmentSettings.Builder().use_blink_planner().build()

st_env = StreamTableEnvironment.create(stream_execution_environment=s_env, environment_settings=env_settings)
# st_env.use_catalog("default_catalog")
# st_env.use_database("default_database")
# st_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)

# dir_kafka_sql_connect = os.path.join(os.path.abspath(os.path.dirname(__file__)),
#                                      'flink-sql-connector-kafka_2.11-1.11.2.jar')
# print("dir_kafka_sql_connect:", dir_kafka_sql_connect)
# st_env.get_config().get_configuration().set_string("pipeline.jars", 'file://' + dir_kafka_sql_connect)

# st_env.get_config().get_configuration().set_string("kafka.jars", "file:///cygdrive/d/iPython/src/my-pyflink-learn/lib/lib/flink-sql-connector-kafka_2.11-1.12.0.jar;file:///cygdrive/d/iPython/src/my-pyflink-learn/lib/lib/flink-json-1.12.0.jar;file:///cygdrive/d/iPython/src/my-pyflink-learn/lib/lib/flink-table-blink_2.12-1.12.0.jar;file:///cygdrive/d/iPython/src/my-pyflink-learn/lib/lib/flink-table_2.12-1.12.0.jar;")
# 'connector.type' = 'kafka',
# 'connector.version' = 'universal',
# 'connector.topic' = 'flink_test2',
# 'connector.properties.bootstrap.servers' = '192.168.100.1:9092',
# 'connector.properties.zookeeper.connect' = '192.168.100.1:2181',
# 'connector.properties.group.id' = 'testGroup',
# 'connector.startup.mode' = 'earliest-offset',
# 'format' = 'json'

kafka_servers = "playgrounds_kafka_1:9092"
zk_servers = "playgrounds_zookeeper_1:2181"
kafka_consumer_group_id = "group8"  # group ID
source_topic = "flinkTest"  # 源数据

sourceKafkaDdl = f"""
create table sourceKafka(
id INT,
name VARCHAR
)
with(
              'connector.type' = 'kafka',
              'connector.version' = 'universal',
              'connector.topic' = '{source_topic}',
              'connector.properties.bootstrap.servers' = '{kafka_servers}',
              'connector.properties.zookeeper.connect' = '{zk_servers}',
              'connector.properties.group.id' = '{kafka_consumer_group_id}',
              'connector.startup-mode' = 'latest-offset',
              'format.type' = 'json'
)
"""

st_env.execute_sql(sourceKafkaDdl)

fieldNames = ["id", "name"]
fieldTypes = [DataTypes.INT(), DataTypes.STRING()]
csvSink = CsvTableSink(fieldNames, fieldTypes, "/opt/examples/data/result.csv", ",", 1, WriteMode.OVERWRITE)
st_env.register_table_sink("csvTableSink", csvSink)

resultQuery = st_env.sql_query("select * from sourceKafka")
resultQuery.insert_into("csvTableSink")

st_env.execute("pyflink-kafka-v2")
