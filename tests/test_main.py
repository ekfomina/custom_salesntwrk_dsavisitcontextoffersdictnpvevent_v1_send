import sys
from argparse import ArgumentParser

import pytest

from scripts.main import get_kafka_params
from scripts.main import parse_arguments


def test_get_kafka_params():
    kafka_bootstrap_servers = "10.107.8.207:9093,10.107.8.208:9093"
    kafka_security_protocol = "SSL"
    keystore_jks_file_name = "some_name.jks"
    pass_for_ssl_cert = "228"
    kafka_batch_size = 100
    kafka_buffer_memory = 1000
    kafka_linger_ms = 0
    kafka_compression_type = "none"
    kafka_acks = -1
    kafka_client_id = "ftstream"

    wanted_kafka_params = {
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

    real_kafka_params = get_kafka_params(kafka_bootstrap_servers=kafka_bootstrap_servers,
                                         kafka_security_protocol=kafka_security_protocol,
                                         keystore_jks_file_name=keystore_jks_file_name,
                                         pass_for_ssl_cert=pass_for_ssl_cert,
                                         kafka_batch_size=kafka_batch_size,
                                         kafka_buffer_memory=kafka_buffer_memory,
                                         kafka_linger_ms=kafka_linger_ms,
                                         kafka_compression_type=kafka_compression_type,
                                         kafka_acks=kafka_acks,
                                         kafka_client_id=kafka_client_id)

    assert wanted_kafka_params == real_kafka_params


def test_get_kafka_params_without_argument():
    """
    Тестирую вызов без агрументов
    """
    with pytest.raises(TypeError) as excinfo:
        get_kafka_params()

    assert "required positional argument" in str(excinfo.value)


def test_get_kafka_params_missing_one_param():
    """
    Тестирую вызов с отсутствующим аргументом
    """
    kafka_security_protocol = "SSL"
    keystore_jks_file_name = "some_name.jks"
    pass_for_keystore = "228"

    with pytest.raises(TypeError) as excinfo:
        get_kafka_params(kafka_security_protocol=kafka_security_protocol,
                         keystore_jks_file_name=keystore_jks_file_name,
                         pass_for_ssl_cert=pass_for_keystore)

    assert "required positional argument" in str(excinfo.value)


def test_parse_arguments():

    __sys_saved_argv = sys.argv  # перестроховка

    sys.argv = ['main.py',
                '--n_partition=4',
                '--loading_id=12121',
                '--ctl_url=loclahost',
                '--ctl_entity_id=343434',
                '--logstash_url=localhost',
                '--app_id=asdfad3234523fddfd',
                '--topic_name=TEST',
                '--kafka_bootstrap_servers=0.0.0.0',
                '--kafka_security_protocol=PLAINTEXT',
                '--json_schema_file_name=json_schema.json',
                '--keystore_jks_file_name=test.jks',
                '--increment_mode=0',
                '--synthetic_mode=1',
                '--archive_mode=0',
                '--point_date_mode=0',
                '--kafka_batch_size=100',
                '--kafka_buffer_memory=1000',
                '--kafka_linger_ms=0',
                '--kafka_compression_type=none',
                '--kafka_acks=-1',
                '--kafka_client_id=ftstream'
                ]

    try:
        parser = ArgumentParser()
        args = parse_arguments(parser)

        assert args.n_partition == 4
        assert args.topic_name == "TEST"

    finally:
        sys.argv = __sys_saved_argv  # перестроховка


def test_parse_arguments_incorrect_arg_type(capsys):
    """
    Тестирую неправильный тип данных у одного из аргументов
    """
    __sys_saved_argv = sys.argv  # перестроховка

    sys.argv = ['main.py',
                '--n_partition=0.4',
                '--loading_id=12121',
                '--ctl_url=loclahost',
                '--ctl_entity_id=343434',
                '--logstash_url=localhost',
                '--app_id=asdfad3234523fddfd',
                '--topic_name=TEST',
                '--kafka_bootstrap_servers=0.0.0.0',
                '--kafka_security_protocol=PLAINTEXT',
                '--avro_file_name=test.avsc',
                '--keystore_jks_file_name=test.jks',
                '--increment_mode=0',
                '--synthetic_mode=1',
                '--archive_mode=0',
                '--kafka_batch_size=100',
                '--kafka_buffer_memory=800',
                '--kafka_linger_ms=0',
                '--kafka_compression_type=none',
                '--kafka_acks=-1'
                ]

    try:
        with pytest.raises(SystemExit):
            parser = ArgumentParser()
            parse_arguments(parser)

        out, err = capsys.readouterr()

        assert " invalid int value" in err

    finally:
        sys.argv = __sys_saved_argv  # перестроховка


def test_parse_arguments_mistake_required_args(capsys):
    """
    Тестирую отсутствие обязательного аргумента
    """

    __sys_saved_argv = sys.argv  # перестроховка

    sys.argv = ['main.py',
                '--n_partition=4',
                '--loading_id=12121',
                '--ctl_url=loclahost',
                '--ctl_entity_id=343434',
                '--logstash_url=localhost',
                '--app_id=asdfad3234523fddfd',
                '--topic_name=TEST',
                '--kafka_bootstrap_servers=0.0.0.0',
                '--kafka_security_protocol=PLAINTEXT',
                '--avro_file_name=test.avsc',
                '--keystore_jks_file_name=test.jks',
                '--increment_mode=0',
                '--synthetic_mode=1',
                '--archive_mode=0']

    try:
        with pytest.raises(SystemExit):
            parser = ArgumentParser()
            parse_arguments(parser)

        out, err = capsys.readouterr()

        assert "the following arguments are required" in err

    finally:
        sys.argv = __sys_saved_argv  # перестроховка