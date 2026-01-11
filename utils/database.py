"""
Database Connector
Supports SQLite (dev) and Databricks (production)
"""

import os
import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnector(ABC):
    """Abstract base class for database connections"""
    
    @abstractmethod
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL and return results as DataFrame"""
        pass
    
    @abstractmethod
    def get_schema_info(self) -> Dict[str, Any]:
        """Get schema information for all tables"""
        pass
    
    @abstractmethod
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get column information for a table"""
        pass


class SQLiteConnector(DatabaseConnector):
    """SQLite connector for development"""
    
    def __init__(self, db_path: str = "data/retail_lakehouse.db"):
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}")
        logger.info(f"Connected to SQLite: {db_path}")
    
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(text(sql), conn)
            return result
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get schema information for all tables"""
        schema = {}
        
        # Get all tables
        tables_query = """
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
        """
        
        with self.engine.connect() as conn:
            tables = pd.read_sql(text(tables_query), conn)
            
            for table_name in tables['name']:
                columns = self.get_table_columns(table_name)
                
                # Get row count
                count_result = pd.read_sql(
                    text(f"SELECT COUNT(*) as cnt FROM {table_name}"), 
                    conn
                )
                row_count = count_result['cnt'].iloc[0]
                
                schema[table_name] = {
                    "columns": columns,
                    "row_count": row_count
                }
        
        return schema
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get column information for a table"""
        query = f"PRAGMA table_info({table_name})"
        
        with self.engine.connect() as conn:
            result = pd.read_sql(text(query), conn)
        
        columns = []
        for _, row in result.iterrows():
            columns.append({
                "name": row['name'],
                "type": row['type'],
                "nullable": not row['notnull'],
                "primary_key": bool(row['pk'])
            })
        
        return columns
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample rows from a table"""
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)


class DatabricksConnector(DatabaseConnector):
    """Databricks connector for production"""
    
    def __init__(
        self,
        host: str,
        token: str,
        http_path: str,
        catalog: str = "retail",
        schema: str = "analytics"
    ):
        self.host = host
        self.catalog = catalog
        self.schema = schema
        
        try:
            from databricks import sql as databricks_sql
            self.connection = databricks_sql.connect(
                server_hostname=host,
                http_path=http_path,
                access_token=token
            )
            logger.info(f"Connected to Databricks: {host}")
        except ImportError:
            raise ImportError("databricks-sql-connector not installed")
    
    def execute_query(self, sql: str) -> pd.DataFrame:
        """Execute SQL query and return DataFrame"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=columns)
        finally:
            cursor.close()
    
    def get_schema_info(self) -> Dict[str, Any]:
        """Get schema information from Unity Catalog"""
        schema = {}
        
        # List tables in schema
        tables_query = f"""
            SELECT table_name 
            FROM {self.catalog}.information_schema.tables
            WHERE table_schema = '{self.schema}'
        """
        
        tables = self.execute_query(tables_query)
        
        for table_name in tables['table_name']:
            columns = self.get_table_columns(table_name)
            
            # Get row count (approximate for large tables)
            count_query = f"""
                SELECT COUNT(*) as cnt 
                FROM {self.catalog}.{self.schema}.{table_name}
            """
            count_result = self.execute_query(count_query)
            
            schema[table_name] = {
                "columns": columns,
                "row_count": count_result['cnt'].iloc[0]
            }
        
        return schema
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get column information from Unity Catalog"""
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM {self.catalog}.information_schema.columns
            WHERE table_schema = '{self.schema}'
              AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        
        result = self.execute_query(query)
        
        columns = []
        for _, row in result.iterrows():
            columns.append({
                "name": row['column_name'],
                "type": row['data_type'],
                "nullable": row['is_nullable'] == 'YES'
            })
        
        return columns


def get_connector() -> DatabaseConnector:
    """Factory function to get appropriate database connector"""
    db_type = os.getenv("DATABASE_TYPE", "sqlite")
    
    if db_type == "sqlite":
        db_path = os.getenv("DATABASE_PATH", "data/retail_lakehouse.db")
        return SQLiteConnector(db_path)
    
    elif db_type == "databricks":
        return DatabricksConnector(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_TOKEN"],
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            catalog=os.getenv("DATABRICKS_CATALOG", "retail"),
            schema=os.getenv("DATABRICKS_SCHEMA", "analytics")
        )
    
    else:
        raise ValueError(f"Unknown database type: {db_type}")
