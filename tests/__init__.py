"""
Модуль с тестами

Для тестов нужен apache-spark
    Установить его можно так (один из способов):
        brew install apache-spark
    Также нужно прописать в конфиге (pytest.ini) для запуска тестов путь %SPARK_HOME
    [pytest]
    spark_home =
    Узнать его можно так:
        1) pyspark
        2) import os
        3) os.environ.get('SPARK_HOME')

Чтобы получить coverage report нужно запустить
pytest . --cov scripts && coverage xml -o coverage-reports/coverage-report.xml
"""