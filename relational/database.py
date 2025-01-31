import os
from typing import Dict, List, Any, Optional
from .table import Table, Column

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.tables: Dict[str, Table] = {}
        
        # Create database directory if it doesn't exist
        os.makedirs(db_path, exist_ok=True)

    def create_table(self, name: str, columns: List[Column]):
        """Create a new table in the database"""
        if name in self.tables:
            raise ValueError(f"Table {name} already exists")

        # Create table directory
        table_path = os.path.join(self.db_path, name)
        os.makedirs(table_path, exist_ok=True)

        # Initialize table
        table = Table(name, columns, table_path)
        self.tables[name] = table
        return table

    def get_table(self, name: str) -> Optional[Table]:
        """Get a table by name"""
        return self.tables.get(name)

    def drop_table(self, name: str):
        """Drop a table from the database"""
        if name not in self.tables:
            raise ValueError(f"Table {name} does not exist")
        
        # Remove table from memory
        del self.tables[name]
        
        # Remove table files
        table_path = os.path.join(self.db_path, name)
        if os.path.exists(table_path):
            import shutil
            shutil.rmtree(table_path)

    def list_tables(self) -> List[str]:
        """List all tables in the database"""
        return list(self.tables.keys())
