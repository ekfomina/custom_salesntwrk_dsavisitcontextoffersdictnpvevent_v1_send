"""
Здесь реализован запуск перегрузки данных
из Hive-a в Kafka
используя spark-submit и pyspark
для проекта DSA FOR NBO
"""
import json
import logging.config
import os
import pprint
import time
from argparse import ArgumentParser
from datetime import datetime
from json import JSONDecodeError

import fastjsonschema
import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql.types import BinaryType

import scripts.work_mode as WM
from scripts.utils import CTLStatistics
from scripts.utils import load_schema
from scripts.utils import setup_logger

logger = logging.getLogger(__name__)

# тк это 1 из 2 интеграций, которые делались не совсем для привычных целей
# то только синтетическая таблица и используется
SCHEMA = "custom_salesntwrk_int_fasttrack"

TABLE = ""
SYNTHETIC_TABLE = "dsavisitcontextoffersdictnpvevent_synthetic"
METADATA_TABLE = ""

COLUMNS_TO_KAFKA = ["json_line"]

PARTITION_COLUMN = ""
FIELD_IN_METADATA_TABLE = ""


@F.udf(returnType=BinaryType())
def prepare(json_line: Column):
    """
    сериализует и валидирует json line

    ВНИМАНИЕ
    1)  схема для валидации изпользуется из глобального скоупа
        такое себе решение, но как параметр для udf кажется не лучше

    Args:
        json_line:

    Returns:

    """
    try:
        json_line = json.loads(json_line)
    except JSONDecodeError as e:
        logger.debug(f"try fix quotes in json\n{e}")
        json_line = json_line.replace("'", '"')
        json_line = json.loads(json_line)

    valid_json_line = VALIDATE_SCHEMA(json_line)

    json_line_str = json.dumps(valid_json_line)
    json_line_serialized = json_line_str.encode('utf-8')

    return json_line_serialized


def parse_arguments(parser):

    parser.add_argument("--n_partition",
                        dest="n_partition",
                        type=int,
                        help="number parititon for repartition",
                        required=True)

    parser.add_argument("--loading_id",
                        dest="loading_id",
                        type=int,
                        help="loading_id from CTL",
                        required=True)
    parser.add_argument("--ctl_url",
                        dest="ctl_url",
                        type=str,
                        help="ctl url for use rest api",
                        required=True)
    parser.add_argument("--ctl_entity_id",
                        dest="ctl_entity_id",
                        type=int,
                        help="id ctl workflow",
                        required=True)

    # параметры для логирования
    parser.add_argument("--logstash_url",
                        dest="logstash_url",
                        type=str,
                        help="url for logging via logstash (REST API)",
                        required=True)
    parser.add_argument("--app_id",
                        dest="app_id",
                        type=str,
                        help="app id for Журналирование КАП",
                        required=True)

    # параметры для работы с кафкой
    parser.add_argument("--topic_name",
                        dest="topic_name",
                        type=str,
                        help="topic name of kafka",
                        required=True)
    parser.add_argument("--kafka_bootstrap_servers",
                        dest="kafka_bootstrap_servers",
                        type=str,
                        help="'<host1>:<port1>,<host2>:<port2>' for kafka server, one string, server separator=,",
                        required=True)
    parser.add_argument("--kafka_security_protocol",
                        dest="kafka_security_protocol",
                        type=str,
                        help="go to apache kafka documentation",
                        required=True)
    parser.add_argument("--json_schema_file_name",
                        dest="json_schema_file_name",
                        type=str,
                        help="name of file with json schema",
                        required=True)
    parser.add_argument("--keystore_jks_file_name",
                        dest="keystore_jks_file_name",
                        type=str,
                        help="name of file with jks keystore",
                        required=False,
                        default=None)

    # параметры отвечающие за режим запуска потока
        # параметры передаются как строки потому что не всегдя в них флаг хранится
        # например this_date_mode равен 0 (не этот режим) или дате("2020-11-29")
    parser.add_argument("--increment_mode",
                        dest="increment_mode",
                        type=str,
                        help="get MAX(ctl_datechange) partition",
                        required=True)
    parser.add_argument("--point_date_mode",
                        dest="point_date_mode",
                        type=str,
                        help="get partition equal getting date",
                        required=True)
    parser.add_argument("--synthetic_mode",
                        dest="synthetic_mode",
                        type=str,
                        help="run on synthetic data",
                        required=True)
    parser.add_argument("--archive_mode",
                        dest="archive_mode",
                        type=str,
                        help="get last partition to kafka",
                        required=True)
    parser.add_argument("--kafka_batch_size",
                        dest="kafka_batch_size",
                        type=int,
                        help="kafka_batch_size",
                        required=True)
    parser.add_argument("--kafka_buffer_memory",
                        dest="kafka_buffer_memory",
                        type=int,
                        help="kafka_buffer_memory",
                        required=True)
    parser.add_argument("--kafka_linger_ms",
                        dest="kafka_linger_ms",
                        type=int,
                        help="kafka_linger_ms",
                        required=True)
    parser.add_argument("--kafka_compression_type",
                        dest="kafka_compression_type",
                        type=str,
                        help="kafka_compression_type",
                        required=True)
    parser.add_argument("--kafka_acks",
                        dest="kafka_acks",
                        type=int,
                        help="kafka_acks",
                        required=True)
    parser.add_argument("--kafka_client_id",
                        dest="kafka_client_id",
                        type=str,
                        help="show client.id in apache kafka",
                        required=True)

    known_args, unknown_args = parser.parse_known_args()

    return known_args


def get_kafka_params(kafka_bootstrap_servers: str,
                     kafka_security_protocol: str,
                     keystore_jks_file_name: str,
                     pass_for_ssl_cert: str,
                     kafka_batch_size: int,
                     kafka_buffer_memory: int,
                     kafka_linger_ms: int,
                     kafka_compression_type: str,
                     kafka_acks: int,
                     kafka_client_id: str) -> dict:
    """
    Заполняет словарь с параметрами для коннекта к кафке и возвращает его

    По сути написана просто чтобы визуально блок __main__ не захломлять

    Args:
        см документацию apache kafka
            kafka_bootstrap_servers:
            kafka_security_protocol:
            keystore_jks_file_name:
            pass_for_ssl_cert:
            kafka_batch_size:
            kafka_buffer_memory:
            kafka_linger_ms:
            kafka_compression_type:
            kafka_acks:
            kafka.client.id:

    Returns: python dict

    """
    kafka_params = {
                    "kafka.bootstrap.servers": kafka_bootstrap_servers,
                    "kafka.security.protocol": kafka_security_protocol,

                    "kafka.ssl.keystore.location": keystore_jks_file_name,
                    "kafka.ssl.truststore.location": keystore_jks_file_name,

                    "kafka.ssl.keystore.password": pass_for_ssl_cert,
                    "kafka.ssl.truststore.password": pass_for_ssl_cert,
                    "kafka.ssl.key.password": pass_for_ssl_cert,

                    "kafka.batch.size": int(kafka_batch_size),
                    "kafka.buffer.memory": int(kafka_buffer_memory),
                    "kafka.linger.ms": int(kafka_linger_ms),

                    "kafka.compression.type": kafka_compression_type,
                    "kafka.acks": int(kafka_acks),
                    "kafka.client.id": kafka_client_id,
                    }

    return kafka_params


if __name__ == "__main__":
    conf = SparkConf().setAppName('custom_salesntwrk_vsp_recsys_massvsp_send')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    setup_logger("logger.yaml")  # тк в контейнере конфиг в корень скачается
    logger.info("start_python")
    logger.ctl_info(f"Log in HDFS: {os.environ.get('PATH_TO_LOG_IN_HDFS')}")

    ctl_validfrom = datetime.now()

    logger.info("parse arguments")
    parser = ArgumentParser()
    args = parse_arguments(parser)
    logger.debug("parsed args:\n" + str(vars(args)))

    logger.info("read schema for validating json")
    json_schema = load_schema(args.json_schema_file_name)
    VALIDATE_SCHEMA = fastjsonschema.compile(json_schema)
    json_schema_readable = pprint.pformat(json_schema, indent=0)
    logger.debug(f"json schema:\n {json_schema_readable}")

    logger.info("selecting work mode")
    SelectedMode = WM.select_mode(increment_mode=args.increment_mode,
                                  point_date_mode=args.point_date_mode,
                                  synthetic_mode=args.synthetic_mode,
                                  archive_mode=args.archive_mode)

    cur_mode = SelectedMode(spark=spark,
                            schema=SCHEMA,
                            table=TABLE,
                            synthetic_table=SYNTHETIC_TABLE,
                            metadata_table=METADATA_TABLE,
                            columns_to_kafka=COLUMNS_TO_KAFKA,
                            partition_colum=PARTITION_COLUMN,
                            field_in_metadata_table=FIELD_IN_METADATA_TABLE,
                            value_date=args.point_date_mode,
                            )
    logger.info(f"current mode: {str(cur_mode)}")

    logger.info("get data to kafka")
    df = cur_mode.get_data_to_kafka()

    df.cache()
    n_rows = df.count()
    logger.info(f"input shape: {n_rows}")

    logger.info("serialized data")
    df = df.withColumn("value", prepare(F.col("json_line")))

    kafka_params = get_kafka_params(kafka_bootstrap_servers=args.kafka_bootstrap_servers,
                                    kafka_security_protocol=args.kafka_security_protocol,
                                    keystore_jks_file_name=args.keystore_jks_file_name,
                                    kafka_batch_size=args.kafka_batch_size,
                                    kafka_buffer_memory=args.kafka_buffer_memory,
                                    kafka_linger_ms=args.kafka_linger_ms,
                                    kafka_compression_type=args.kafka_compression_type,
                                    kafka_acks=args.kafka_acks,
                                    pass_for_ssl_cert="Ghtathfyc1",
                                    kafka_client_id=args.kafka_client_id)

    df = df.repartition(args.n_partition)

    start_time = int(time.time())

    logger.info("write data to kafka")
    (df.select(F.col("value"))
       .withColumn("topic", F.lit(args.topic_name))
       .write.format("kafka")
       .options(**kafka_params)
       .save()
     )

    end_time = int(time.time())
    duration = end_time - start_time

    tps = n_rows / duration
    logger.info(f"TPS = {tps}")

    query_plan = df._jdf.queryExecution().toString()
    logger.debug(f"plan of query:\n{query_plan}")

    # logger.info("update metadata table")
    # datamart_finished_dttm = datetime.now()
    # row_with_metadata = WM.RowWithMetadata(datamart="",
    #                                        datamart_finished_dttm=,
    #                                        mode=str(cur_mode),
    #                                        src_ctl_validfrom=cur_mode.src_ctl_validfrom,
    #                                        ctl_validfrom=,
    #                                        ctl_loading_id=args.loading_id)
    # cur_mode.update_metadata_table(row_with_metadata)

    logger.info("upload stl statistics")
    ctl_url_stat = args.ctl_url + "/v1/api/statval/m"
    ctl_stat = CTLStatistics(ctl_url=ctl_url_stat,
                             loading_id=args.loading_id,
                             ctl_entity_id=args.ctl_entity_id)
    # CNT_INSERT
    ctl_stat.upload_statistic(stat_id=910, stat_value=str(n_rows))

    # CHANGE
    ctl_stat.upload_statistic(stat_id=2, stat_value="1")

    logger.info("end_python")

    # нужно закрывать именно просле последнего лог мессаджа либо вообще не закрывтать
    # тк если используется хандлер в hdfs то после закрытия он не отработает
    spark.stop()
