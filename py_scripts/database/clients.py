import logging
from pydantic import BaseModel
from typing import Dict
import pandas as pd
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.extensions import connection as Connection

from py_scripts.database.models import DWHSchema, BankSchema

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
                schema: BankSchema):
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
                schema: DWHSchema,
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
        self.max_dt = "3000-01-01"
        self.min_dt = "1900-01-01"
    
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
            META=self.schema.META.meta,
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


    def update_staging_timestamp_in_meta_table(self, upd_date: datetime, field_name: str) -> None:
        """Updates timestamp for staging table.

        Parameters
        ----------
        upd_date : datetime
            Update date value.
        field_name: str
            Pydantic field name of staging table
        """
        query_template = """
        UPDATE {meta_table_name}
        SET max_update_dt = to_timestamp('{upd_timestamp}', 'YYYY-MM-DD')
        WHERE table_name = '{stg_table_name}';
        """
        if hasattr(self.schema.STG, field_name):
            stg_table_name = self.schema.STG.__getattribute__(field_name)
            query = query_template.format(
                meta_table_name=self.schema.META.meta,
                stg_table_name=stg_table_name,
                upd_timestamp=upd_date.strftime("%Y-%m-%d")
            )
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
            

    def insert_incoming_tables(self, incoming_data: Dict[str, pd.DataFrame], date: datetime) -> None:
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

            # 1.1 Update date of incoming data in meta table
            self.update_staging_timestamp_in_meta_table(date, field_name)
            
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
            SELECT {stg_cols_string}, '{max_dt}', False
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
            stg_cols_string = ", ".join(list(map(lambda x: f"stg.{x}", mapping.keys())) + [f"COALESCE(stg.\"{date_col}\", '{self.min_dt}')"])

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
                differ_string_insert=differ_string_insert,
                max_dt=self.max_dt
            )

            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
    
    def report_frauds(self, report_date: datetime = None) -> None:
        """Manages frauds reporting functions calling
        
        Parameters
        ----------
        report_date : datetime
            Report date, default None
        """
        self.report_blacklist_fraud(report_date)
        self.report_invalid_contract_fraud(report_date)
        self.report_transactions_in_different_cities_fraud(report_date)
        self.report_amount_guessing_fraud(report_date)

    def report_blacklist_fraud(self, report_date: datetime = None) -> None:
        query_template = """
        INSERT INTO {rep_fraud_table_name} (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            t.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Заблокированный или просроченный паспорт' AS event_type,
            CURRENT_DATE AS report_dt
        FROM {fact_transactions_table_name} t
        JOIN {dim_cards_table_name} c
            ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN public.maka_dwh_dim_accounts_hist a
            ON c.account_num = a.account_num AND c.deleted_flg = False
        JOIN {dim_clients_table_name} cl
            ON a.client = cl.client_id AND c.deleted_flg = False
        JOIN {fact_blacklist_table_name} p
            ON cl.passport_num = p.passport_num
        WHERE (p.entry_dt <= t.trans_date OR cl.passport_valid_to <= t.trans_date)
        AND t.trans_date >= {date_string};
        """
        if report_date is not None:
            date_string = f"'{report_date.strftime('%Y-%m-%d')}'"
        else:
            date_string = """
            (
                SELECT MAX(max_update_dt) 
                FROM {meta_table_name} 
                WHERE table_name = '{date_ref_table_name}'
            )
            """.format(
                meta_table_name=self.schema.META.meta,
                date_ref_table_name=self.schema.STG.transactions
                )
            
        query = query_template.format(
            rep_fraud_table_name=self.schema.REP.fraud,
            fact_transactions_table_name=self.schema.FACT.transactions,
            dim_cards_table_name=self.schema.DIM.cards,
            dim_clients_table_name=self.schema.DIM.clients,
            fact_blacklist_table_name=self.schema.FACT.blacklist,
            date_string=date_string)
        
        self.logger.info("Checking blacklist frauds ...")
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        self.logger.info("Complete")

    def report_invalid_contract_fraud(self, report_date: datetime = None) -> None:
        query_template = """
        INSERT INTO {rep_fraud_table_name} (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            t.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Недействующий договор' AS event_type,
            CURRENT_DATE AS report_dt
        FROM {fact_transactions_table_name} t
        JOIN {dim_cards_table_name} c
            ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN {dim_accounts_table_name} a
            ON c.account_num = a.account_num AND a.deleted_flg = False
        JOIN {dim_clients_table_name} cl
            ON a.client = cl.client_id AND cl.deleted_flg = False
        WHERE a.valid_to <= t.trans_date
        AND t.trans_date >= {date_string};
        """
        if report_date is not None:
            date_string = f"'{report_date.strftime('%Y-%m-%d')}'"
        else:
            date_string = """
            (
                SELECT MAX(max_update_dt) 
                FROM {meta_table_name} 
                WHERE table_name = '{date_ref_table_name}'
            )
            """.format(
                meta_table_name=self.schema.META.meta,
                date_ref_table_name=self.schema.STG.transactions
                                          )
            
        query = query_template.format(
            rep_fraud_table_name=self.schema.REP.fraud,
            fact_transactions_table_name=self.schema.FACT.transactions,
            dim_cards_table_name=self.schema.DIM.cards,
            dim_accounts_table_name=self.schema.DIM.accounts,
            dim_clients_table_name=self.schema.DIM.clients,
            date_string=date_string)
        
        self.logger.info("Checking invalid contract frauds ...")
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        self.logger.info("Complete")

    def report_transactions_in_different_cities_fraud(self, report_date: datetime = None) -> None:
        query_template = """
        WITH unique_cards AS (
            SELECT 
                a.client AS client_id,
                c.cards_num
            FROM {dim_cards_table_name} c
            JOIN {dim_accounts_table_name} a ON c.account_num = a.account_num AND a.deleted_flg = False
            JOIN {dim_clients_table_name} cl ON a.client = cl.client_id AND cl.deleted_flg = False
            GROUP BY a.client, c.cards_num
        ),
        filtered_transactions AS (
            SELECT 
                t.trans_date,
                t.card_num,
                term.terminal_city,
                cl.passport_num,
                CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
                cl.phone
            FROM {fact_transactions_table_name} t
            JOIN {dim_terminals_table_name} term ON t.terminal = term.terminal_id AND term.deleted_flg = False
            JOIN unique_cards uc ON TRIM(t.card_num) = TRIM(uc.cards_num)
            JOIN {dim_cards_table_name} c ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
            JOIN {dim_accounts_table_name} a ON c.account_num = a.account_num AND a.deleted_flg = False
            JOIN {dim_clients_table_name} cl ON a.client = cl.client_id AND cl.deleted_flg = FALSE
        )
        INSERT INTO {rep_fraud_table_name} (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT DISTINCT 
            t1.trans_date AS event_dt,
            t1.passport_num AS passport,
            t1.fio,
            t1.phone,
            'Операции в разных городах за короткое время' AS event_type,
            CURRENT_DATE as report_dt
        FROM filtered_transactions t1
        JOIN filtered_transactions t2
            ON t1.passport_num = t2.passport_num
            AND t1.terminal_city != t2.terminal_city 
            AND ABS(EXTRACT(EPOCH FROM t2.trans_date) - EXTRACT(EPOCH FROM t1.trans_date)) <= 3600
        WHERE t1.trans_date >= {date_string};
        """
        if report_date is not None:
            date_string = f"'{report_date.strftime('%Y-%m-%d')}'"
        else:
            date_string = """
            (
                SELECT MAX(max_update_dt) 
                FROM {meta_table_name} 
                WHERE table_name = '{date_ref_table_name}'
            )
            """.format(
                meta_table_name=self.schema.META.meta,
                date_ref_table_name=self.schema.STG.transactions
                                          )
            
        query = query_template.format(
            rep_fraud_table_name=self.schema.REP.fraud,
            fact_transactions_table_name=self.schema.FACT.transactions,
            dim_terminals_table_name=self.schema.DIM.terminals,
            dim_cards_table_name=self.schema.DIM.cards,
            dim_accounts_table_name=self.schema.DIM.accounts,
            dim_clients_table_name=self.schema.DIM.clients,
            date_string=date_string)
        
        self.logger.info("Checking transaction in different cities frauds ...")
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        self.logger.info("Complete")

    def report_amount_guessing_fraud(self, report_date: datetime = None) -> None:
        query_template = """
        WITH RECURSIVE ordered_transactions AS (
            SELECT 
                TRIM(t.card_num) AS card_num, 
                t.trans_date, 
                t.amt, 
                t.oper_result,
                ROW_NUMBER() OVER (PARTITION BY TRIM(t.card_num) ORDER BY t.trans_date) AS rn
            FROM {fact_transactions_table_name} t
            JOIN {dim_cards_table_name} c
                ON TRIM(t.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
            WHERE t.trans_date >= {date_string}
        ), 
        suspicious_sequences AS (
            SELECT 
                card_num, 
                trans_date AS start_date,
                trans_date AS end_date,
                amt,
                oper_result,
                rn,
                1 AS sequence_length,
                CASE WHEN oper_result = 'REJECT' THEN 1 ELSE 0 END AS reject_count
            FROM ordered_transactions
            UNION ALL
            SELECT 
                t.card_num, 
                s.start_date,
                t.trans_date,
                t.amt,
                t.oper_result,
                t.rn,
                s.sequence_length + 1,
                s.reject_count + CASE WHEN t.oper_result = 'REJECT' THEN 1 ELSE 0 END
            FROM ordered_transactions t
            JOIN suspicious_sequences s
                ON t.card_num = s.card_num AND t.rn = s.rn + 1
            WHERE t.trans_date - s.start_date <= INTERVAL '20 MINUTES'
                AND t.amt < s.amt
        ),
        final_suspicious_transactions AS (
            SELECT DISTINCT
                card_num,
                start_date,
                end_date
            FROM suspicious_sequences
            WHERE sequence_length >= 4
                AND reject_count >= 3
                AND oper_result = 'SUCCESS'
        ),
        filtered_suspicious_transactions AS (
            SELECT 
                o.card_num, 
                o.trans_date, 
                o.oper_result,
                ROW_NUMBER() OVER (PARTITION BY o.card_num, f.start_date ORDER BY o.trans_date DESC) AS row_desc
            FROM ordered_transactions o
            JOIN final_suspicious_transactions f
                ON o.card_num = f.card_num
            WHERE o.trans_date BETWEEN f.start_date AND f.end_date
        ),
        distinct_suspicious_transactions AS (
            SELECT 
                card_num,
                trans_date,
                oper_result
            FROM filtered_suspicious_transactions
            WHERE oper_result = 'SUCCESS' AND row_desc = 1
            UNION
            SELECT 
                card_num,
                trans_date,
                oper_result
            FROM filtered_suspicious_transactions
            WHERE oper_result = 'REJECT'
        )
        INSERT INTO {rep_fraud_table_name} (event_dt, passport, fio, phone, event_type, report_dt)
        SELECT 
            dst.trans_date AS event_dt,
            cl.passport_num AS passport,
            CONCAT(cl.last_name, ' ', cl.first_name, ' ', cl.patronymic) AS fio,
            cl.phone AS phone,
            'Попытка подбора суммы' AS event_type,
            CURRENT_DATE AS report_dt
        FROM distinct_suspicious_transactions dst
        JOIN {dim_cards_table_name} c
            ON TRIM(dst.card_num) = TRIM(c.cards_num) AND c.deleted_flg = False
        JOIN {dim_accounts_table_name} a
            ON c.account_num = a.account_num AND a.deleted_flg = False
        JOIN {dim_clients_table_name} cl
            ON a.client = cl.client_id AND cl.deleted_flg = False
        ORDER BY dst.trans_date;
        """
        if report_date is not None:
            date_string = f"'{report_date.strftime('%Y-%m-%d')}'"
        else:
            date_string = """
            (
                SELECT MAX(max_update_dt) 
                FROM {meta_table_name} 
                WHERE table_name = '{date_ref_table_name}'
            )
            """.format(
                meta_table_name=self.schema.META.meta,
                date_ref_table_name=self.schema.STG.transactions
                                          )
        
        query = query_template.format(
            rep_fraud_table_name=self.schema.REP.fraud,
            fact_transactions_table_name=self.schema.FACT.transactions,
            dim_cards_table_name=self.schema.DIM.cards,
            dim_accounts_table_name=self.schema.DIM.accounts,
            dim_clients_table_name=self.schema.DIM.clients,
            date_string=date_string)
        
        self.logger.info("Checking amount guessing frauds ...")
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
        self.logger.info("Complete")
