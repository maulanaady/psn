from __future__ import annotations
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import mysql.connector
import paramiko
import logging
from airflow.hooks.base import BaseHook
from airflow.models import Variable


class ConnectionHook:
    @staticmethod
    def get_maria_connection(conn_id, db=None):
        try:
            connection = BaseHook.get_connection(conn_id)
            database = db if db else "information_schema"
            return mysql.connector.connect(
                user=connection.login,
                password=connection.password,
                host=connection.host,
                database=database,
            )
        except mysql.connector.Error as e:
            logging.error("MariaDB connection error: %s", e)
            raise RuntimeError(f"Database connection failed: {e}") from e

    @staticmethod
    def get_mariaAlchemy_engine(conn_id, db=None):
        try:
            connection = BaseHook.get_connection(conn_id)
            database = db if db else connection.schema
            connection_url = (
                f"mysql+pymysql://{connection.login}:{connection.password}@"
                f"{connection.host}:{connection.port}/{database}"
            )
            return create_engine(connection_url)
        except SQLAlchemyError as e:
            logging.error("MariaDB SQLAlchemy engine creation error: %s", e)
            raise RuntimeError(f"Database connection failed: {e}") from e

    @staticmethod
    def get_pg_connection(conn_id, uri=False):
        try:
            connection = BaseHook.get_connection(conn_id)
            if uri:
                uri_str = Variable.get("postgres_ha", default_var="")
                if not uri_str:
                    raise ValueError("Postgres HA URI not found in Airflow variables.")
                uri = uri_str.format(
                    username=connection.login,
                    password=connection.password,
                    db=connection.schema,
                )
                return psycopg2.connect(uri)
            return psycopg2.connect(
                user=connection.login,
                password=connection.password,
                host=connection.host,
                database=connection.schema,
            )
        except psycopg2.Error as e:
            logging.error("PostgreSQL connection error: %s", e)
            raise RuntimeError(f"Database connection failed: {e}") from e

    @staticmethod
    def get_pgAlchemy_engine(conn_id, db=None):
        try:
            connection = BaseHook.get_connection(conn_id)
            database = db if db else connection.schema
            connection_url = (
                f"postgresql+psycopg2://{connection.login}:{connection.password}@"
                f"{connection.host}/{database}"
            )
            return create_engine(connection_url)
        except SQLAlchemyError as e:
            logging.error("PostgreSQL SQLAlchemy engine creation error: %s", e)
            raise RuntimeError(f"Database connection failed: {e}") from e

    @staticmethod
    def get_ssh_connection(conn_id):
        try:
            connection = BaseHook.get_connection(conn_id)
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            key_mapping = Variable.get(
                "ssh_key_mapping", deserialize_json=True, default_var={}
            )
            key = key_mapping.get(conn_id)
            ssh_client.connect(
                hostname=connection.host,
                port=connection.port,
                username=connection.login,
                password=connection.password,
                key_filename=key,
                timeout=30,
                banner_timeout=30,
                auth_timeout=30,
            )
            return ssh_client
        except paramiko.SSHException as e:
            logging.error("SSH connection error for %s: %s", conn_id, e)
            raise RuntimeError(f"Remote connection failed: {e}") from e
