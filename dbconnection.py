# -*- coding: utf-8 -*-
import mysql.connector
from logger import LogMsg


class DBConnectionError(Exception):
    pass


class CredentialsError(Exception):
    pass


class SQLError(Exception):
    pass


class DBConnection():
    """
    Класс для создания подключения к базе данных.
    Поддерживает работу с диспетчером контекста.
    """

    def __init__(self, config: dict):
        self.config = config

    def __enter__(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
            return self.cursor
        except mysql.connector.errors.InterfaceError as err:
            raise DBConnectionError(err)
        except mysql.connector.errors.ProgrammingError as err:
            raise CredentialsError(err)

    def __exit__(self, exc_type, exc_value, exc_trace):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        if exc_type is mysql.connector.errors.ProgrammingError:
            raise SQLError(exc_value)
        elif exc_type:
            raise exc_type(exc_value)
