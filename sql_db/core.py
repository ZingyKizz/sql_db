import pandas as pd
import six
from sqlalchemy import create_engine
from abc import ABC, abstractmethod


class SQLSession(ABC):
    def __init__(self, *, server, port, db, user, password, silent):
        """
        Parameters
        ----------
        :type server: str
        :type port: str
        :type db: str
        :type user: str
        :type password : str
        :type silent: bool
        Returns
        -------
        Session object
        """
        if not (
                isinstance(server, six.string_types)
                and isinstance(db, six.string_types)
        ):
            raise TypeError('server and db arguments should be strings')
        if not (
                isinstance(user, six.string_types)
                and isinstance(password, six.string_types)
        ):
            raise TypeError('user and password arguments should be strings')

        self.server = server
        self.port = port
        self.db = db
        self.user = user
        self.password = password

        self.engine = self.make_engine()
        self.con = self.engine.connect()
        self.is_connected = True
        if not silent:
            self.print_message()
        self.is_silent = silent

    def print_message(self, symbol='-'):
        fill_length = max(len(f'Server: {self.server}'), len(f'Database: {self.db}'), len(f'User: {self.user}'))
        fill = f'{fill_length * symbol}'
        message = f'Connected to\n{fill}\nServer: {self.server}\nDatabase: {self.db}\nUser: {self.user}\n{fill}'
        print(message)

    @abstractmethod
    def make_engine(self):
        raise NotImplementedError

    @abstractmethod
    def select_statement(self, query):
        """Returns result of select-query as DataFrame
        Parameters
        ----------
        :type query: str
        Returns
        -------
        pd.DataFrame
        """
        raise NotImplementedError

    @abstractmethod
    def exec_sp(self, sp_name, params):
        """
        Executes stored procedure with specified list of params
        Parameters
        ----------
        :type sp_name: str
        :type params: list
        Returns
        -------
        None
        """
        raise NotImplementedError

    def df_to_db(self, df, table_fullname, index=False, if_exists='fail', **kwargs):
        """Writes DataFrame to database
        Parameters
        ----------
        :type df: pd.DataFrame
        :type table_fullname: str
        :type index: bool
        :type if_exists: str
        :param kwargs: additional parameters for pd.to_sql() method
        Returns
        -------
        None
        """
        if not isinstance(table_fullname, six.string_types):
            raise TypeError('table_fullname argument should be a string')
        try:
            schema_name, table_name = table_fullname.split('.')
        except ValueError as e:
            raise Exception('table_fullname argument should be like "schema_name.table_name"') from e
        df.to_sql(name=table_name, index=index, schema=schema_name, con=self.con,
                  if_exists=if_exists, **kwargs)

    def close(self):
        """Closes connection"""
        if self.is_connected:
            self.con.close()
            self.is_connected = False
            message_ = 'Connection closed'
        else:
            message_ = 'Connection is already closed'
        if not self.is_silent:
            fill_symbol = '-'
            fill_length = len(message_)
            message = f'{fill_symbol * fill_length}\n{message_}\n{fill_symbol * fill_length}'
            print(message)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class MSSQLSession(SQLSession):
    """Provide connection to Microsoft SQL Server databases"""
    def make_engine(self):
        engine = create_engine(
            f'mssql+pyodbc://{self.user}:{self.password}'
            f'@{self.server}:{self.port}/{self.db}?driver=ODBC+Driver+13+for+SQL+Server',
            fast_executemany=True
        )
        return engine

    def select_statement(self, query):
        if not isinstance(query, six.string_types):
            raise TypeError('query argument should be a string')
        return pd.read_sql('set nocount on; ' + query, con=self.con)

    def exec_sp(self, sp_name, params=None):
        if not isinstance(sp_name, six.string_types):
            raise TypeError('query argument should be a string')

        if params is None:
            params = []
        elif not isinstance(params, list):
            raise TypeError('params argument should be a list')

        params_template = ', '.join(['?'] * len(params))
        cursor = self.engine.raw_connection().cursor()
        cursor.execute(f'set nocount on; exec {sp_name} {params_template}', params)
        cursor.commit()


class PostgreSQLSession(SQLSession):
    """Provide connection to PostgreSQL databases"""
    def make_engine(self):
        engine = create_engine(f'postgres://{self.user}:{self.password}@{self.server}:{self.port}/{self.db}')
        return engine

    def select_statement(self, query):
        if not isinstance(query, six.string_types):
            raise TypeError('query argument should be a string')
        return pd.read_sql(query, con=self.con)

    def exec_sp(self, sp_name, params):
        raise NotImplementedError

