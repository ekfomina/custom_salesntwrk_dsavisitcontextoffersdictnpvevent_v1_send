"""
В данном модуле реализованны различные режимы работы

Абстрактный класс WorkMode задает интерфейс для
корректной работы остального кода с различными режимами

На данный момент реализован 1 режим
1 - Тестовый/синтетический режим (SyntheticMode)
    суть данного режима в том, что не на всех стендах есть реальные данные
    и поэтому на всех стендах при раскатке витрины будут создаваться
    фейковая таблица, с такой же схемой что и у реальных данных (10 записей)
    и вот именно на них и будут запусктьс расчеты в данном режиме.
    В таблицу с метаинформацией дата перегрузки записываться не будет.
"""
import logging
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from typing import List, Union, Type

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

logger = logging.getLogger(__name__)


@dataclass()
class RowWithMetadata:
    """
    структура данных
    атрибуты это поля 1 записи в таблицы с метаинформацией
    Args:
        datamart (str): название витрины которцю перегружаем
        datamart_finished_dttm (datetime): Дата и время окончания перегрузки витрины
        mode (str): режим перегрузки данных
        src_ctl_validfrom (datetime): Максимальная дата бизнес актуальности источника
                                      (дата за которую выполнена перегрузка)
        ctl_validfrom (datetime):  Дата и время старта загрузки CTL
        ctl_loading_id (int): Идентификатор загрузки CTL, в рамках которой завершился расчет сущности
    """
    datamart: str
    datamart_finished_dttm: datetime
    mode: str
    src_ctl_validfrom: datetime
    ctl_validfrom: datetime
    ctl_loading_id: int


class WorkMode(ABC):
    def __init__(self,
                 spark: SparkSession,
                 schema: str,
                 table: str,
                 synthetic_table: str,
                 metadata_table: str,
                 columns_to_kafka: List[str],
                 partition_colum: str,
                 field_in_metadata_table: str,
                 value_date: str = None,
                 value_date_format='%Y-%m-%d'):
        """

        Args:
            spark: открытая спарк сессия.
            schema: название схемы в которой лежат данные.
            table: имя таблицы, данные из которой нужно перегрузитьв кафку.
            synthetic_table: имя таблицы, в которой лежат синтетические данные.
            metadata_table: имя таблицы с метаданными.
            columns_to_kafka: список с названием столбцов которые нужно перегрузить в кафку.
            partition_colum: столбец по которому данные партиционируются.
            field_in_metadata_table: название стоблца в таблице с метаданными в котором хранится
                                     максимальная дата бизнес актуальности источника
                                     (дата за которую выполнена перегрузка).
            value_date: параметр с датой, за которую данные нужно перегрузитьв кафку
                        нужен для Точечного режима (PointDateMode).
            value_date_format: маска, для преобразования строки в datetime
                               для параметра value_date.
        """
        self.spark = spark
        self.schema = schema
        self.table = table
        self.synthetic_table = synthetic_table
        self.metadata_table = metadata_table
        self._fmt_columns_to_kafka = ",".join(columns_to_kafka)
        self.partition_colum = partition_colum
        self.field_in_metadata_table = field_in_metadata_table
        self._raw_value_date = value_date
        self._value_date_format = value_date_format

        self.src_ctl_validfrom = self._get_src_ctl_validfrom()

    @classmethod
    def __str__(cls):
        return cls.__name__

    @abstractmethod
    def get_data_to_kafka(self) -> DataFrame:
        """
        Возвращает PySparkDataFrame с данными которые нужно
        записать в кафку

        Returns: Pyspark DataFrame

        """
        pass

    @abstractmethod
    def update_metadata_table(self, row_with_metadata: RowWithMetadata) -> None:
        """
        Обновляет данные в таблице с метаинформацией

        как именно обновяться данные (что запишетя/удалится)зависит от
        реализации данного класса поэтому для каждого режима запуска
        работа с таблицей с мета инфой может отличаться

        Args:
            row_with_metadata: структура данных, которая содержит поля
                               которые необходима в талицу с метаинфой записать

        Returns:

        """
        pass

    @abstractmethod
    def _get_src_ctl_validfrom(self) -> datetime:
        """
        Извлекается значение поля по которому данные партиционируются

        Это должна быть дата

        Returns:

        """
        pass


class QueryTemplates:
    def __init__(self):
        self.query_max_date = """
                                SELECT
                                    MAX(cast({field} as timestamp))
                                FROM
                                    {schema}.{table}
                               """

        # todo по хорошему, кастование нужно выпилить
        self.query_insert_row_to_metadata = """
                                            INSERT INTO {schema}.{table}
                                            VALUES ('{datamart}',
                                                    timestamp('{datamart_finished_dttm}'),
                                                    '{mode}', 
                                                    timestamp('{src_ctl_validfrom}'),
                                                    timestamp('{ctl_validfrom}'), 
                                                    {ctl_loading_id})
                                            """

        self.query_truncate_table = """TRUNCATE TABLE {schema}.{table}"""

        self.query_get_all_data = """
                                  SELECT
                                        {columns}
                                  FROM
                                        {schema}.{table}
                                  """

        self.query_get_one_timestamp_partition = """
                    SELECT
                        {columns}
                    FROM
                        {schema}.{table}
                    WHERE
                        cast({partition_col} as timestamp) = '{partition_value}'
                      """


# class IncrementMode(WorkMode, QueryTemplates):
#
#     def __init__(self, *args, **kwargs):
#         # порядок вызова очень важен
#         QueryTemplates.__init__(self)
#         WorkMode.__init__(self, *args, **kwargs)
#
#     def update_metadata_table(self, row_with_metadata: RowWithMetadata) -> None:
#         """
#         Записывает поля которые находятся в переданной структуре данных
#         в таблицу с метаинформацией
#
#         Args:
#             row_with_metadata: см доку структуры данных
#
#         Returns:
#
#         """
#         row = row_with_metadata
#
#         query = self.query_insert_row_to_metadata.format(schema=self.schema,
#                                                          table=self.metadata_table,
#                                                          datamart=row.datamart,
#                                                          datamart_finished_dttm=row.datamart_finished_dttm,
#                                                          mode=row.mode,
#                                                          src_ctl_validfrom=row.src_ctl_validfrom,
#                                                          ctl_validfrom=row.ctl_validfrom,
#                                                          ctl_loading_id=row.ctl_loading_id)
#         logger.debug(f"query for insert metadata value:\n{query}")
#         self.spark.sql(query)
#
#         return None
#
#     def get_data_to_kafka(self):
#         """
#         общая суть такая же как и у родительского класса
#         поэтому см документацию родительского класса
#
#         из дополнительного, делается проверка не записывалась ли
#         в кафку уже такая дата или уще более поздяя
#         И если запысывалась, то падает с ошибкой, тк не должно быть такого
#         """
#         # TODO переименовать тк не очень интуицию отражает
#         previous_upload_date = self._get_previous_upload_date()
#         current_upload_date = self.src_ctl_validfrom
#
#         logger.debug(f"max upload date in metadata table: {previous_upload_date}")
#         logger.debug(f"max date in source data: {current_upload_date}")
#
#         if previous_upload_date >= current_upload_date:
#             raise ValueError("previous upload date more or equal than current upload date")
#
#         query = self.query_get_one_timestamp_partition.format(columns=self._fmt_columns_to_kafka,
#                                                               schema=self.schema,
#                                                               table=self.table,
#                                                               partition_col=self.partition_colum,
#                                                               partition_value=self.src_ctl_validfrom)
#         logger.debug(f"query for getting data:\n{query}")
#
#         df = self.spark.sql(query)
#
#         return df
#
#     def _get_src_ctl_validfrom(self) -> datetime:
#         """
#         возвращает максимальное дату на которую есть данные
#
#         Returns: datetime.date
#
#         """
#         logger.debug("start getting {partition_col}".format(partition_col=self.partition_colum))
#         query = self.query_max_date.format(field=self.partition_colum,
#                                            schema=self.schema,
#                                            table=self.table)
#         logger.debug(f"query for get max ctl_datecannge:\n{query}")
#
#         raw_max_ctl_datechange = self.spark.sql(query)
#
#         max_ctl_datechange = raw_max_ctl_datechange.collect()[0][0]
#
#         logger.debug(str(type(max_ctl_datechange)))
#         if not isinstance(max_ctl_datechange, datetime):
#             raise ValueError("max_ctl_datechange must be datetime type")
#
#         return max_ctl_datechange
#
#     def _get_previous_upload_date(self) -> datetime:
#         """
#         Извлекает из таблицы с метаинформацией
#         последную дату которую в кафку загрузил
#         те значение поля партиционирования (а это дата)
#         Returns:
#
#         """
#         query = self.query_max_date.format(field=self.field_in_metadata_table,
#                                            schema=self.schema,
#                                            table=self.metadata_table)
#         logger.debug(f"query for get privious date val from matadata table:\n{query}")
#
#         raw_previous_upload_date = self.spark.sql(query)
#
#         previous_upload_date = raw_previous_upload_date.collect()[0][0]
#
#         logger.debug(str(type(previous_upload_date)))
#         if not isinstance(previous_upload_date, datetime):
#             raise ValueError("previous_upload_date must be datetime type")
#
#         return previous_upload_date


# class PointDateMode(WorkMode, QueryTemplates):
#
#     def __init__(self, *args, **kwargs):
#         # порядок вызова очень важен
#         QueryTemplates.__init__(self)
#         WorkMode.__init__(self, *args, **kwargs)
#
#     def update_metadata_table(self, row_with_metadata: RowWithMetadata) -> None:
#         """
#         Записывает поля которые находятся в переданной структуре данных
#         в таблицу с метаинформацией
#
#         Args:
#             row_with_metadata: см доку структуры данных
#
#         Returns:
#
#         """
#         row = row_with_metadata
#
#         query = self.query_insert_row_to_metadata.format(schema=self.schema,
#                                                          table=self.metadata_table,
#                                                          datamart=row.datamart,
#                                                          datamart_finished_dttm=row.datamart_finished_dttm,
#                                                          mode=row.mode,
#                                                          src_ctl_validfrom=row.src_ctl_validfrom,
#                                                          ctl_validfrom=row.ctl_validfrom,
#                                                          ctl_loading_id=row.ctl_loading_id)
#         logger.debug(f"query for insert metadata value:\n{query}")
#         self.spark.sql(query)
#
#         return None
#
#     def get_data_to_kafka(self):
#         """
#         см доку родительского класса
#
#         Returns:
#
#         """
#         query = self.query_get_one_timestamp_partition.format(columns=self._fmt_columns_to_kafka,
#                                                               schema=self.schema,
#                                                               table=self.table,
#                                                               partition_col=self.partition_colum,
#                                                               partition_value=self.src_ctl_validfrom)
#
#         logger.debug(f"query for getting data:\n{query}")
#
#         df = self.spark.sql(query)
#
#         return df
#
#     def _convert_date_from_str_to_date(self, date_str: str, date_format: str) -> datetime:
#         """
#         Преобразует дату из строки в нормальную дату (datetime.datetime)
#
#         Args:
#             date_str: Дата в формате строки
#             date_format: маска даты / формат даты
#
#         Returns: datetime.datetime
#
#         """
#         logger.debug(f"raw date type: {type(date_str)}")
#         # value_date = datetime.strptime(date_str, date_format).date()
#         value_date = datetime.strptime(date_str, date_format)
#         logger.debug(f"converted date type: {type(value_date)}")
#         logger.debug(f"converted date: {value_date}")
#
#         return value_date
#
#     def _get_src_ctl_validfrom(self) -> datetime:
#         """
#         см доку родительского класса
#         Returns:
#
#         """
#         if self._raw_value_date is None:
#             raise ValueError("Set params 'value_date' for PointDateMode")
#
#         src_ctl_validfrom = self._convert_date_from_str_to_date(date_str=self._raw_value_date,
#                                                                 date_format=self._value_date_format)
#
#         return src_ctl_validfrom


class SyntheticMode(WorkMode, QueryTemplates):

    def __init__(self, *args, **kwargs):
        # порядок вызова очень важен
        QueryTemplates.__init__(self)
        WorkMode.__init__(self, *args, **kwargs)

    def update_metadata_table(self, row_with_metadata: RowWithMetadata) -> None:
        """
        Ничего не делает

        нужно чтобы просто интерфейс соблюсти
        тк все остальные режими изменяют таблицу с метаинформацией
        а данные режим нет то вот так....

        Args:
            row_with_metadata:

        Returns:

        """
        logger.info("in synthetic mode metadata table not used")

        return None

    def get_data_to_kafka(self):
        """
        см документацию родительского класса
        """
        query = self.query_get_all_data.format(columns=self._fmt_columns_to_kafka,
                                               schema=self.schema,
                                               table=self.synthetic_table)

        logger.debug(f"query for getting data:\n{query}")

        df = self.spark.sql(query)

        return df

    def _get_src_ctl_validfrom(self) -> datetime:
        """
        Ничего не делает

        ради соблюдения интерфейса возвращает дату (datetime)
        но в данном режиме нет записи в таблицу с мета инфой
        и поэтому это значение нигде использоваться не будет
        можно любую дату вернуть
        Returns:

        """
        return datetime.now()


# class ArchiveMode(WorkMode, QueryTemplates):
#
#     def __init__(self, *args, **kwargs):
#         # порядок вызова очень важен
#         QueryTemplates.__init__(self)
#         WorkMode.__init__(self, *args, **kwargs)
#
#     def update_metadata_table(self, row_with_metadata: RowWithMetadata) -> None:
#         """
#         обновляет занчения в таблице с метаинформацией
#
#         1) Очищает все занчения что были раньше
#         2) Вставляет новую запись (поля из переданной структуры данных)
#
#         Args:
#             row_with_metadata: см доку структуры данныъ
#
#         Returns:
#
#         """
#         row = row_with_metadata
#
#         query_clear_metadata_table = self.query_truncate_table.format(schema=self.schema,
#                                                                       table=self.metadata_table)
#         logger.debug(f"query for clearing table:\n{query_clear_metadata_table}")
#         self.spark.sql(query_clear_metadata_table)
#
#         query = self.query_insert_row_to_metadata.format(schema=self.schema,
#                                                          table=self.metadata_table,
#                                                          datamart=row.datamart,
#                                                          datamart_finished_dttm=row.datamart_finished_dttm,
#                                                          mode=row.mode,
#                                                          src_ctl_validfrom=row.src_ctl_validfrom,
#                                                          ctl_validfrom=row.ctl_validfrom,
#                                                          ctl_loading_id=row.ctl_loading_id)
#
#         logger.debug(f"query for insert metadata value:\n{query}")
#         self.spark.sql(query)
#
#         return None
#
#     def get_data_to_kafka(self) -> DataFrame:
#         """
#         см документацию родительского класса
#         """
#         query = self.query_get_one_timestamp_partition.format(columns=self._fmt_columns_to_kafka,
#                                                               schema=self.schema,
#                                                               table=self.table,
#                                                               partition_col=self.partition_colum,
#                                                               partition_value=self.src_ctl_validfrom)
#
#         logger.debug(f"query for getting data:\n{query}")
#
#         df = self.spark.sql(query)
#
#         return df
#
#     def _get_src_ctl_validfrom(self) -> datetime:
#         """
#         возвращает максимальное дату на которую есть данные
#
#         Returns: datetime.date
#
#         """
#         logger.debug("start getting {partition_col}".format(partition_col=self.partition_colum))
#         query = self.query_max_date.format(field=self.partition_colum,
#                                            schema=self.schema,
#                                            table=self.table)
#         logger.debug(f"query for get max ctl_datecannge:\n{query}")
#
#         raw_max_ctl_datechange = self.spark.sql(query)
#
#         max_ctl_datechange = raw_max_ctl_datechange.collect()[0][0]
#
#         logger.debug("max_ctl_datechange: {date}".format(date=max_ctl_datechange))
#         if isinstance(max_ctl_datechange, datetime) is False:
#             # Пробуем привести к нужному типу, если это возможно
#             if isinstance(max_ctl_datechange, date):
#                 _year = max_ctl_datechange.year
#                 _month = max_ctl_datechange.month
#                 _day = max_ctl_datechange.day
#                 max_ctl_datechange = datetime(_year, _month, _day)
#             else:
#                 real_type = type(max_ctl_datechange)
#                 raise ValueError(f"max_ctl_datechange must be datetime, now {real_type} type")
#
#         return max_ctl_datechange


def select_mode(increment_mode: str,
                point_date_mode: str,
                synthetic_mode: str,
                archive_mode: str,
                ) -> Type[SyntheticMode]:
    """
    Рализован паттерн фабричный метод

    В зависимости от входных параметров (режима запуска)
    возвращает различные классы, в которых нужная логика и реализованна
    Но все эти классы имеют общий интерфейс

    Args:
        increment_mode: фалг запуска в инкрементальном режиме (см доку модуля)
        point_date_mode: дата или флаг запуска в режиме на определенную дату (см доку модуля)
        synthetic_mode: флаг запуска в тестовом он же синтетический режиме (см доку модуля)
        archive_mode: флаг запуска в архивном режиме (см доку модуля)

    Returns:
        класс, с реализованной локигой и интерфейсом от WorkMode

    """
    work_mode_class = None

    # параметры передаются как строки потому что не всегдя в них флаг хранится
    # например this_date_mode равен 0 (не этот режим) или дате("2020-11-29")
    # if increment_mode == '1':
    #     if work_mode_class is not None:
    #         raise ValueError("Set one working mode")
    #     work_mode_class = IncrementMode
    #
    # if archive_mode == '1':
    #     if work_mode_class is not None:
    #         raise ValueError("Set one working mode")
    #     work_mode_class = ArchiveMode

    if synthetic_mode == '1':
        if work_mode_class is not None:
            raise ValueError("Set one working mode")
        work_mode_class = SyntheticMode

    # if point_date_mode != '0':
    #     if work_mode_class is not None:
    #         raise ValueError("Set one working mode")
    #     work_mode_class = PointDateMode

    if work_mode_class is None:
        raise ValueError("Set one working mode")

    return work_mode_class
