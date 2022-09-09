import logging
import json

import yaml

from scripts.utils import setup_logger
from scripts.utils import SberLogstashFormatter


def test_sber_logstash_formatter():
    app_id = "666999666"

    log_record = logging.LogRecord(name="test_utils",
                                   level=logging.INFO,
                                   pathname="/project/test_utils/",
                                   lineno=10,
                                   msg="test_msg",
                                   args=None,
                                   exc_info=None)

    sber_logstash_formatter = SberLogstashFormatter(app_id=app_id)

    log_json_line = sber_logstash_formatter.format(log_record)

    log_dict = json.loads(log_json_line)

    assert log_dict["app_id"] == app_id
    assert isinstance(log_dict["timestamp"], int)
    assert log_dict["type_id"] == "Tech"
    assert log_dict["subtype_id"] == log_record.levelname
    assert log_dict["value"] == log_record.levelno
    assert log_dict["message"] == log_record.msg
    assert log_dict["name"] == log_record.name


def test_setup_logger_read_config(tmpdir):
    LOGGER_CONFIG = {"version": 1,
                     "formatters": {"console": {"format":"%(name)s - %(levelname)s - %(message)s",
                                                "datefmt": "%H:%M:%S"}},
                     "handlers": {"console": {"class": "logging.StreamHandler",
                                              "formatter": "console"}},
                     "root": {"level": "CRITICAL",
                              "handlers": ["console"],
                              "propagate": True}
                     }

    log_conf_path = tmpdir.join("logger.yaml")

    with open(log_conf_path, "w") as f:
        yaml.dump(LOGGER_CONFIG, f)

    logger = logging.getLogger()

    setup_logger(log_conf_path)

    isinstance(logger.handlers[0], logging.StreamHandler)
    assert logger.level == logging.CRITICAL
    assert logger.handlers[0].formatter.datefmt == LOGGER_CONFIG["formatters"]["console"]["datefmt"]
