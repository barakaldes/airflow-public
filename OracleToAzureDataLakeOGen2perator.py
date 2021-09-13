import os
from datetime import datetime
from tempfile import TemporaryDirectory
from typing import Any, Optional, Union

import cx_Oracle
import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.decorators import apply_defaults


class OracleToAzureDataLakeGen2Operator(BaseOperator):
    """
    Moves data from Oracle to Azure Data Lake. The operator runs the query against
    Oracle and stores the file locally before loading it into Azure Data Lake.


    :param filename: file name to be used by the csv file.
    :type filename: str
    :param azure_data_lake_conn_id: destination azure data lake connection.
    :type azure_data_lake_conn_id: str
    :param azure_data_lake_path: destination path in azure data lake to put the file.
    :type azure_data_lake_path: str
    :param oracle_conn_id: source Oracle connection.
    :type oracle_conn_id: str
    :param sql: SQL query to execute against the Oracle database. (templated)
    :type sql: str
    :param sql_params: Parameters to use in sql query. (templated)
    :type sql_params: Optional[dict]
    :param delimiter: field delimiter in the file.
    :type delimiter: str
    :param encoding: encoding type for the file.
    :type encoding: str
    :param quotechar: Character to use in quoting.
    :type quotechar: str
    :param quoting: Quoting strategy. See unicodecsv quoting for more information.
    :type quoting: str
    """

    template_fields = ('filename', 'sql', 'sql_params')
    template_fields_renderers = {"sql_params": "py"}
    ui_color = '#e08c8c'

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
            self,
            *,
            filename: str,
            azure_data_lake_conn_id: str,
            azure_data_lake_container: str,
            azure_data_lake_path: str,
            oracle_conn_id: str,
            sql: str,
            sql_params: Optional[dict] = None,
            delimiter: str = ";",
            encoding: str = "utf-8",
            quotechar: str = '"',
            quoting: str = csv.QUOTE_MINIMAL,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if sql_params is None:
            sql_params = {}
        self.filename = filename
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.sql_params = sql_params
        self.azure_data_lake_conn_id = azure_data_lake_conn_id
        self.azure_data_lake_container = azure_data_lake_container
        self.azure_data_lake_path = azure_data_lake_path
        self.delimiter = delimiter
        self.encoding = encoding
        self.quotechar = quotechar
        self.quoting = quoting

    def _write_temp_file(self, cursor: Any, path_to_save: Union[str, bytes, int]) -> None:
        with open(path_to_save, 'wb') as csvfile:
            csv_writer = csv.writer(
                csvfile,
                delimiter=self.delimiter,
                encoding=self.encoding,
                quotechar=self.quotechar,
                quoting=self.quoting,
            )
            csv_writer.writerow(map(lambda field: field[0], cursor.description))
            csv_writer.writerows(cursor)
            csvfile.flush()

    def execute(self, context: dict) -> None:

        # cx_Oracle.init_oracle_client(lib_dir=r"/opt/oracle/instantclient_21_1")

        oracle_hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        azure_data_lake_hook = WasbHook(wasb_conn_id=self.azure_data_lake_conn_id)

        execution_date = datetime.strptime(context.get("ds"), '%Y-%m-%d')
        execution_date_with_spanish_format = execution_date.strftime("%d/%m/%Y")

        self.sql = self.sql.replace("[DATE_TO]", execution_date_with_spanish_format)

        print("CONSULTA " + self.sql)

        self.log.info("Dumping Oracle query results to local file")





        from airflow.models.connection import Connection
        conn = Connection.get_connection_from_secrets("ORACLE-TO-AZURE-DATALAKE__ORACLE_CONNECTION")

        conn_config = {'user': conn.login, 'password': conn.password}
        dsn = conn.extra_dejson.get('dsn')
        sid = conn.extra_dejson.get('sid')
        mod = conn.extra_dejson.get('module')

        service_name = conn.extra_dejson.get('service_name')
        port = conn.port if conn.port else 1521
        if dsn and sid and not service_name:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, sid)
        elif dsn and service_name and not sid:
            conn_config['dsn'] = cx_Oracle.makedsn(dsn, port, service_name=service_name)
        else:
            conn_config['dsn'] = conn.host

        if 'encoding' in conn.extra_dejson:
            conn_config['encoding'] = conn.extra_dejson.get('encoding')
            # if `encoding` is specific but `nencoding` is not
            # `nencoding` should use same values as `encoding` to set encoding, inspired by
            # https://github.com/oracle/python-cx_Oracle/issues/157#issuecomment-371877993
            if 'nencoding' not in conn.extra_dejson:
                conn_config['nencoding'] = conn.extra_dejson.get('encoding')
        if 'nencoding' in conn.extra_dejson:
            conn_config['nencoding'] = conn.extra_dejson.get('nencoding')
        if 'threaded' in conn.extra_dejson:
            conn_config['threaded'] = conn.extra_dejson.get('threaded')
        if 'events' in conn.extra_dejson:
            conn_config['events'] = conn.extra_dejson.get('events')

        mode = conn.extra_dejson.get('mode', '').lower()
        if mode == 'sysdba':
            conn_config['mode'] = cx_Oracle.SYSDBA
        elif mode == 'sysasm':
            conn_config['mode'] = cx_Oracle.SYSASM
        elif mode == 'sysoper':
            conn_config['mode'] = cx_Oracle.SYSOPER
        elif mode == 'sysbkp':
            conn_config['mode'] = cx_Oracle.SYSBKP
        elif mode == 'sysdgd':
            conn_config['mode'] = cx_Oracle.SYSDGD
        elif mode == 'syskmt':
            conn_config['mode'] = cx_Oracle.SYSKMT
        elif mode == 'sysrac':
            conn_config['mode'] = cx_Oracle.SYSRAC

        purity = conn.extra_dejson.get('purity', '').lower()
        if purity == 'new':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_NEW
        elif purity == 'self':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_SELF
        elif purity == 'default':
            conn_config['purity'] = cx_Oracle.ATTR_PURITY_DEFAULT

        conn = cx_Oracle.connect(**conn_config)

        if mod is not None:
            conn.module = mod






        #connection = cx_Oracle.connect(user="ADMIN", password="aBc123-:,XyZ", dsn="test002db_high")

        # conn = oracle_hook.get_conn()
        # cursor = conn.cursor()  # type: ignore[attr-defined]
        # cursor.execute(self.sql, self.sql_params)
        #
        # with TemporaryDirectory(prefix='airflow_oracle_to_azure_op_') as temp:
        #     self._write_temp_file(cursor, os.path.join(temp, self.filename))
        #     self.log.info("Uploading local file to Azure Data Lake")
        #     final_path = self.azure_data_lake_path + "/" + execution_date_with_spanish_format.replace("/", "_") + "/" + self.filename
        #     azure_data_lake_hook.load_file(
        #         os.path.join(temp, self.filename), self.azure_data_lake_container, final_path, overwrite="true"
        #     )
        # cursor.close()
        # conn.close()  # type: ignore[attr-defined]
