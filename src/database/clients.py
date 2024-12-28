import logging
from pydantic import BaseModel
from typing import Dict, List
import pandas as pd

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as Connection

class Client:
    """Base Client class.
    
    All Client classes should be inherited from this class"""
    def __init__(self,
                database: str,
                host: str,
                user: str,
                password: str,
                port: str,
                schema: BaseModel):
        """Initializes a Client instance for communicating with a database.

        This constructor sets up the necessary parameters to establish a connection 
        with the specified database. It prepares the client with the provided connection 
        details, which are essential for performing database operations.

        Parameters
        ----------
        database : str
            The name of the database to connect to.
        host : str
            The hostname or IP address of the database server.
        user : str
            The username used to authenticate with the database.
        password : str
            The password associated with the provided username.
        port : str
            The port number on which the database server is listening.
        schema : BaseModel
            A Pydantic model representing the schema for data that will be used in 
            interactions with the database.
        """

        
        self.logger = logging.getLogger(__name__)
        self.connection: Connection = None
        self.schema = schema
        try:
            self.connection = psycopg2.connect(
                database=database,
                host=host,
                user=user,
                password=password,
                port=port,
            )
            
            self.connection.autocommit = False
            self.logger.info(f"Successfully connected to the database '{database}'")

        except Exception:
            self.logger.error(f"Could not establish connection with database '{database}'", exc_info=True)
            

    def is_table_empty(self, table_name: str) -> bool:
        """Checks if specified table empty.

        Returns:
        bool
            True if specified table empty, else False
        """
        with self.connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM %s LIMIT 1);" % table_name)
            is_empty = not cursor.fetchone()[0]
        return is_empty


    def fetch_data_to_df(self, table_name: str) -> pd.DataFrame:
        """
        Fetch all data from the specified table and return it as a pandas DataFrame.

        Parameters:
        table_name : str
            Name of the table to fetch data from.

        Returns:
        pd.DataFrame
            DataFrame containing all rows from the specified table.
        """

        select_query = "SELECT * FROM %s;" % table_name
        
        with self.connection.cursor() as cursor:
            cursor.execute(select_query)
            rows = cursor.fetchall()

        column_names = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=column_names)
        
        return df

    def insert_df_to_table(self, df: pd.DataFrame, table_name: str) -> None:
        """Inserts specified pandas dataframe to specified table name in the database

        Parameters
        ----------
        df : pd.DataFrame
            Input pandas dataframe that should be inserted
        table_name : str
            Table name where dataframe should be inserted
        """
        # Prepare the SQL insert statement
        columns = df.columns.tolist()
        
        # Create an insert query with placeholders for each column
        insert_query = "INSERT INTO {} ({}) VALUES ({})".format(
            table_name,
            ', '.join(columns),
            ', '.join(["%s"] * len(columns))
        )
        
        # Prepare data for insertion
        values = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        # Use executemany to insert all rows at once
        with self.connection.cursor() as cursor:
            execute_batch(cursor, insert_query, values)
            self.connection.commit()
    
    def clear_table(self, table_name: str) -> None:
        """Clears table by specified table name

        Parameters
        ----------
        table_name : str
            Table name that needs to be cleared
        """
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM %s;" % table_name)
            self.connection.commit()
    
    def insert_from_table_to_table(self, src_table_name: str, dest_table_name, mapping: Dict[str, str]) -> None:
        query_template = """
            INSERT INTO {dest_table_name} ({dest_cols_string})
            SELECT {src_cols_string}
            FROM {src_table_name} src
            WHERE NOT EXISTS (
            SELECT 1 
            FROM {dest_table_name} dest
            WHERE {where_string}
        );
        """

        src_cols_string = ", ".join(list(mapping.keys()))
        dest_cols_string = ", ".join(list(mapping.values()))
        
        where_list = []
        for src_col, dest_col in mapping.items():
            where_list.append(f"dest.{dest_col} = src.{src_col}")
        where_string = " AND ".join(where_list)

        query = query_template.format(dest_table_name=dest_table_name,
                                      dest_cols_string=dest_cols_string,
                                      src_table_name=src_table_name,
                                      src_cols_string=src_cols_string,
                                      where_string=where_string)
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()


class BankDBClient(Client):
    """Communicates with bank database.
    
    e.g. recieves client related information from bank database"""

    def __init__(self,
                database: str,
                host: str,
                user: str,
                password: str,
                port: str,
                schema: BaseModel):
        """Initializes a BankDBClient instance for communicating with a bank database.

        This constructor sets up the necessary parameters to establish a connection 
        with the specified bank database. It inherits from the base `Client` class 
        and initializes it with the provided database connection details.

        Parameters
        ----------
        database : str
            The name of the database to connect to.
        host : str
            The hostname or IP address of the database server.
        user : str
            The username used to authenticate with the database.
        password : str
            The password associated with the provided username.
        port : str
            The port number on which the database server is listening.
        schema : BaseModel
            A Pydantic model representing the schema for client-related information 
            that will be used in interactions with the bank database.
        """

        super().__init__(database=database,
                         host=host,
                         user=user,
                         password=password,
                         port=port,
                         schema=schema)


class DWHClient(Client):
    """Communicates with data warehouse database."""

    def __init__(self,
                database: str,
                host: str,
                user: str,
                password: str,
                port: str,
                schema: BaseModel,
                scd2_config: Dict[str, Dict[str, str]] = None,
                fact_mapping: Dict[str, Dict[str, str]]= None):
        """Initializes a DWHClient instance for communicating with a data warehouse database.

        This constructor sets up the necessary parameters to establish a connection 
        with the specified data warehouse database. It inherits from the base `Client` 
        class and initializes it with the provided database connection details.

        Parameters
        ----------
        database : str
            The name of the data warehouse database to connect to.
        host : str
            The hostname or IP address of the data warehouse server.
        user : str
            The username used to authenticate with the data warehouse.
        password : str
            The password associated with the provided username.
        port : str
            The port number on which the data warehouse server is listening.
        schema : BaseModel
            A Pydantic model representing the schema for data that will be used in 
            interactions with the data warehouse.
        """

        super().__init__(database=database,
                         host=host,
                         user=user,
                         password=password,
                         port=port,
                         schema=schema)
        self.scd2_config = scd2_config
        self.fact_mapping = fact_mapping
    
    def create_schema(self, ddl_pattern_filepath: str) -> None:
        """Creates empty database schema according to specified DDL script.

        Parameters
        ----------
        ddl_pattern_filepath : str
            The path to the SQL pattern file containing the DDL commands for creating data warehouse schema.
        """

        with open(ddl_pattern_filepath, "r") as sql_file:
            sql_script = sql_file.read()
        
        # Replace table names patterns with actual table names
        sql_script = sql_script.format(
            DIM_terminals=self.schema.DIM.terminals,
            DIM_clients=self.schema.DIM.clients,
            DIM_accounts=self.schema.DIM.accounts,
            DIM_cards=self.schema.DIM.cards,
            FACT_transactions=self.schema.FACT.transactions,
            FACT_blacklist=self.schema.FACT.blacklist,
            REP_fraud=self.schema.REP.fraud,
            STG_transactions=self.schema.STG.transactions,
            STG_terminals=self.schema.STG.terminals,
            STG_blacklist=self.schema.STG.blacklist,
            STG_clients=self.schema.STG.clients,
            STG_accounts=self.schema.STG.accounts,
            STG_cards=self.schema.STG.cards,
        )
            
        with self.connection.cursor() as cursor:
            cursor.execute(sql_script)
            self.connection.commit()

    def insert_to_stg_table(self, field_name: str, data: pd.DataFrame) -> None:
        """Inserts data to staging table by field name

        Parameters
        ----------
        field_name : str
            Pydantic table schema field name
        data : pd.DataFrame
            Pandas dataframe to insert
        """
        if hasattr(self.schema.STG, field_name):
            stg_table_name = self.schema.STG.__getattribute__(field_name)
            self.clear_table(stg_table_name)
            self.insert_df_to_table(data, stg_table_name)
        else:
            raise AttributeError(f"No table name for {field_name} field name found in staging tables")


    def insert_bank_tables(self, bank_client: BankDBClient) -> None:
        """Inserts data to bank tables.

        e.g. accounts, clients, cards.

        Parameters
        ----------
        bank_client : BankDBClient
            Bank database client object.
        """
        for dim_field_name, _ in self.schema.DIM:
            
            # if self.is_table_empty(dim_table_name):

            if hasattr(bank_client.schema, dim_field_name):

                # 1. Load bank data to corresponding staging tables

                # Get data from corresponding bank table
                bank_table_name = bank_client.schema.__getattribute__(dim_field_name)
                data = bank_client.fetch_data_to_df(bank_table_name)

                # Load raw data to corresponding staging table
                self.insert_to_stg_table(dim_field_name, data)

                # 2. Insert data to DWH dimension tables from staging tables

                scd2_config = self.scd2_config.get(dim_field_name)
                if scd2_config is not None:
                    self.insert_from_stg_table_to_dim_table(dim_field_name, **scd2_config)
        

    def insert_incoming_tables(self, incoming_data: Dict[str, pd.DataFrame]) -> None:
        """Inserts incoming data to corresponding tables.

        Parameters
        ----------
        incoming_data : Dict[str, pd.DataFrame]
            A dictionary maps table names to their corresponding 
            pandas DataFrames containing the tabular data.
        """
        for field_name, data in incoming_data.items():

            # 1. Load incoming data to corresponding staging tables
            self.insert_to_stg_table(field_name, data)
            
            # 2. Insert data to DWH dimension tables from staging tables
            scd2_config = self.scd2_config.get(field_name)
            if scd2_config is not None:
                self.insert_from_stg_table_to_dim_table(field_name, **scd2_config)
            
            # 3. Insert data to DWH fact tables from staging tables
            fact_mapping = self.fact_mapping.get(field_name)
            if fact_mapping is not None:
                stg_table_name, fact_table_name = None, None
                if hasattr(self.schema.STG, field_name):
                    stg_table_name = self.schema.STG.__getattribute__(field_name)
                if hasattr(self.schema.FACT, field_name):
                    fact_table_name = self.schema.FACT.__getattribute__(field_name)
                if stg_table_name is not None and fact_table_name is not None:
                    self.insert_from_table_to_table(stg_table_name, fact_table_name, fact_mapping)

    
    def insert_from_stg_table_to_dim_table(self, field_name: str,
                                           mapping: Dict[str, str],
                                           date_col: str,
                                           stg_pk: str,
                                           dim_pk: str) -> None:
        """Inserts data in dimension table in SCD2 format from staging table.

        Parameters
        ----------
        field_name : str
            Pydantic table schema field name
        mapping : Dict[str, str]
            Maps corresponding columns names of staging and dimension table
        date_col : str
            Date column name which is used to update effective from/to columns
        stg_pk : str
            Staging table primary key column name. Used for join with `dim_pk`
        dim_pk : str
            Dimension table primary key column name. Used for join with `stg_pk`
        """
        stg_table_name = None
        dim_table_name = None
        query_template = """
            UPDATE {dim_table_name}
            SET effective_to = stg."{date_col}",
                deleted_flg = True
            FROM {stg_table_name} stg
            WHERE {dim_table_name}.{dim_pk} = stg.{stg_pk}
            AND ({differ_string_update})
            AND {dim_table_name}.deleted_flg = False;

            INSERT INTO {dim_table_name} ({dim_cols_string}, effective_from, effective_to, deleted_flg)
            SELECT {stg_cols_string}, '3000-01-01', False
            FROM {stg_table_name} stg
            LEFT JOIN {dim_table_name} dim
            ON stg.{stg_pk} = dim.{dim_pk} AND dim.deleted_flg = False
            WHERE dim.{dim_pk} IS NULL OR ({differ_string_insert});
        """
        if hasattr(self.schema.STG, field_name):
            stg_table_name = self.schema.STG.__getattribute__(field_name)
        if hasattr(self.schema.DIM, field_name):
            dim_table_name = self.schema.DIM.__getattribute__(field_name)

        if stg_table_name is not None and dim_table_name is not None:

            dim_cols_string = ", ".join(list(mapping.values()))
            stg_cols_string = ", ".join(list(map(lambda x: f"stg.{x}", mapping.keys())) + [f"COALESCE(stg.\"{date_col}\", '1900-01-01')"])

            differ_list_update = []
            differ_list_insert = []
            for stg_col, dim_col in mapping.items():
                differ_list_update.append(f"{dim_table_name}.{dim_col}" + " <> " + f"stg.{stg_col}")
                differ_list_insert.append(f"dim.{dim_col}" + " <> " + f"stg.{stg_col}")
            differ_string_update = " OR ".join(differ_list_update)
            differ_string_insert = " OR ".join(differ_list_insert)

            query = query_template.format(
                dim_table_name=dim_table_name,
                stg_table_name=stg_table_name,
                dim_pk=dim_pk,
                stg_pk=stg_pk,
                date_col=date_col,
                dim_cols_string=dim_cols_string,
                stg_cols_string=stg_cols_string,
                differ_string_update=differ_string_update,
                differ_string_insert=differ_string_insert
            )

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()


