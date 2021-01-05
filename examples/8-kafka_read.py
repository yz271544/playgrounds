#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Project: spd-sxmcc
"""
@file: 8-kafka_read.py
@author: lyndon
@time: Created on 2021-01-04 15:54
@env: Python
@desc:
@ref:
@blog:
"""

import os
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, EnvironmentSettings, DataTypes, CsvTableSink, WriteMode


def from_kafka_to_kafka_demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    s_env.set_parallelism(1)

    # use blink table planner
    st_env = StreamTableEnvironment \
        .create(s_env, environment_settings=EnvironmentSettings
                .new_instance()
                .in_streaming_mode()
                .use_blink_planner().build())

    # register source and sink
    register_rides_source(st_env)
    register_rides_sink(st_env)

    st_env.sql_query("SELECT * FROM sourceKafka").insert_into("csvTableSink")
    # query
    # st_env.execute("INSERT INTO sinkFile SELECT * FROM sourceKafka")
    st_env.execute("pyflink-kafka-v2")


def register_rides_source(st_env):
    kafka_servers = "192.168.100.1:9092"
    zk_servers = "192.168.100.1:2181"
    kafka_consumer_group_id = "group8"  # group ID
    source_topic = "flink_test2"  # 源数据

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

    st_env.execute(sourceKafkaDdl)


def register_rides_sink(st_env):
    dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result.csv')
    fieldNames = ["id", "name"]
    fieldTypes = [DataTypes.INT(), DataTypes.STRING()]
    csvSink = CsvTableSink(fieldNames, fieldTypes, dir_result, ",", 1, WriteMode.OVERWRITE)
    st_env.register_table_sink("csvTableSink", csvSink)

    # dir_result = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'result.csv')
    #
    # sink_ddl = f"""
    #     CREATE TABLE sinkFile (
    #         id INT,
    #         name VARCHAR
    #     ) WITH (
    #         'connector' = 'filesystem',
    #         'path' = 'file://{dir_result}',
    #         'format' = 'csv'
    #     )
    # """
    # st_env.execute_sql(sink_ddl)


if __name__ == '__main__':
    from_kafka_to_kafka_demo()
