import yaml
import pymysql
import pandas as pd
from typing import List, Tuple, Union


class Database:
    """
    Creates a mysql instance
    """

    def __init__(self, connection_name, current_table=None):
        self.connection_name = connection_name
        self.creds = self.__read_config()
        self._current_table = current_table
        self.__make_connection(self.creds)

    def __read_config(self):
        with open('db_config.yaml') as config:
            creds = yaml.safe_load(config)
        creds = creds[self.connection_name]
        return creds

    def __make_connection(self, creds):
        # Not sure yet how to pass in arbitrary number of arguments from db_config as parameters
        self.con = pymysql.connect(host=creds['host'],
                              port=creds['port'],
                              user=creds['user'],
                              passwd=creds['passwd'],
                              db=creds['db'],
                              charset=creds['charset'],
                              autocommit=creds['autocommit'])
        self.cursor = self.con.cursor()
        self.cursor.execute("USE " + creds['db'] + ";")

    def get_table_columns(self, current_table: str=None) -> tuple:
        if current_table is None:
            current_table = self._current_table
        query = "SELECT `COLUMN_NAME` \
                FROM `INFORMATION_SCHEMA`.`COLUMNS` \
                WHERE `TABLE_SCHEMA` = '" + self.creds['db'] + "' \
                AND `TABLE_NAME` = '" + current_table + "';"
        self.cursor.execute(query)

        # Convert returned columns to more easily readable names
        result = list(self.cursor.fetchall())
        result = tuple([val[0].replace(",", "") for val in result])

        return result

    def __preprocess_table_values(self, data, current_table: str=None):
        """Reformats the data being inserted into an organized, sql-ready format"""
        if current_table is None:
            current_table = self._current_table

        table_cols = self.get_table_columns(current_table)

        # Parse to column names
        if isinstance(data, pd.DataFrame):
            col_names = list(data.columns)
        elif isinstance(data, tuple) or isinstance(data, list):
            assert len(data) == 2
            col_names = data[0]
        else:
            raise TypeError("Unable to parse columns from given data. "
                            "Data must be of type: pandas.DataFrame or of tuples/lists of length: 2]")

        # Convert column names to snake case
        import re
        for index, name in enumerate(col_names):
            col_names[index] = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower().replace("__", "_")

        # Check that column names are all in the table
        for col in col_names:
            if col not in table_cols:
                raise ValueError("Column label: '" + col + "' not found in table columns: " + str(table_cols))

        col_names = str(tuple(col_names)).replace("'", "")

        # Parse the values
        if isinstance(data, pd.DataFrame):
            values = tuple(map(tuple, data.values.copy()))
            if len(data) == 1:
                values = str(values)[:-1]
        elif isinstance(data, tuple) or isinstance(data, list):
            if len(data) == 2:
                # Make sure the length of data being inserted matches
                try:
                    if isinstance(data[0], tuple) or isinstance(data[0], list) and len(data[0]) > 0:
                        insert_length = len(data[0])
                        # Correctly process whether it's only one row being inserted or multiple rows
                        if isinstance(data[1][0], list) or isinstance(data[1][0], tuple):
                            for row in data[1]:
                                assert len(row) == insert_length
                            values = data[1]
                        else:
                            assert len(data[1]) == insert_length
                            values = "(" + str(data[1]) + ")"
                except TypeError:
                    print("Unable to parse columns from given data. "
                          "Length of data to insert does not match length of the Insert columns:")
                    print(data)
        else:
            raise TypeError(
                "Unable to parse row values from given data. "
                "Data must be of type: pandas.DataFrame or follow structure: "
                "((column name, column name), ((value, value), (value, value))")

        values = str(values)[1:-1]


        return col_names, values


    def insert_data(self, data, current_table: str=None, update_duplicates: bool=False):
        if current_table is None:
            current_table = self._current_table
        # Assumes the dataframe column names match the table columns when converted to snake case
        query_values = self.__preprocess_table_values(data, current_table)
        col_names = query_values[0]
        values = query_values[1]

        insert_query = "INSERT INTO " + current_table + " " + col_names + " VALUES " + values + ";"

        # Upsert handling
        if update_duplicates:
            col_names = tuple(col_names[1:-1].split(", "))
            insert_query = insert_query[:-1]
            upsert = " ON DUPLICATE KEY UPDATE "
            for col in col_names:
                upsert = upsert + col + " = " + col + ","
            upsert = upsert[:-1] + ";"
            insert_query = insert_query + upsert

        print(insert_query)
        self.cursor.execute(insert_query)
        print("Data successfully inserted into " + current_table)

    def __append_where_and_having(self, cond_type: str, cond_list: list = None):
        if cond_list:
            conds = " " + cond_type.upper() + " "
            for arg_num, arg in enumerate(cond_list):
                conds = conds + arg
                if arg_num != len(cond_list) - 1:
                    conds = conds + " AND "
        else:
            conds = ''

        return conds

    def select_query(self, current_table=None, columns: list=['*'], where: list=None,
                     group_by: list=None, having: list=None, limit: int=None):
        """Basic WHERE query. JOINs are too complicated for the moment, need to see if I can add them in later"""
        if current_table is None:
            current_table = self._current_table

        base_query = "SELECT " + str(columns)[1: -1] + " FROM " + current_table
        base_query = base_query.replace("'", "")

        # TODO: add JOIN functionality

        where_conds = self.__append_where_and_having('where', where)

        if group_by:
            group_by_cond = ' GROUP BY ' + str(group_by)[1:-1].replace("'", "")

        having_conds = self.__append_where_and_having('having', having)

        limit = ' LIMIT ' + str(limit) if limit else ''

        end = ";"

        full_query = base_query + where_conds + group_by_cond + having_conds + limit + end
        print(full_query)

        self.cursor.execute(full_query)
        result = pd.DataFrame(self.cursor.fetchall())
        result.columns = [x[0] for x in self.cursor.description]
        return result

    def show_tables(self):
        self.cursor.execute("SHOW TABLES;")
        return pd.DataFrame(self.cursor.fetchall())

    def ad_hoc_query(self, query: str):
        self.cursor.execute(query)
        result = pd.DataFrame(self.cursor.fetchall())
        return result

    def drop_table(self, table: str):
        self.cursor.execute("DROP TABLE IF EXISTS " + table + ";")
        print(table + " dropped successfully")

    @property
    def current_table(self):
        if self._current_table:
            return self._current_table
        else:
            raise ValueError("No table selected")

    @current_table.setter
    def current_table(self, new_table_name: str):
        # Make sure that the new table name is in the database
        self.cursor.execute("SELECT table_name \
                            FROM `INFORMATION_SCHEMA`.`TABLES` \
                            WHERE `TABLE_SCHEMA` = '" + self.creds['db'] + "' \
                            AND `TABLE_NAME` = '" + new_table_name + "';")
        db_table = self.cursor.fetchone()[0]
        if new_table_name != db_table:
            raise ValueError("New table does not exist in the database")
        self._current_table = new_table_name

    @classmethod
    def list_connections(self):
        """
        Open db_config.yaml and list the possible connections, as well as connection notes
        """
        with open('db_config.yaml') as connections:
            c = yaml.safe_load(connections)
            for name in c:
                print("Connection name: " + name)
                print("     Connection notes: " + c[name]['notes'])

    def commit(self):
        self.con.commit()

    def close_connection(self):
        self.cursor.close()
        self.con.close()

