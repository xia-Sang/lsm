from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from .bplus_tree import BPlusTree
from lsm.lsm import LSMTree

@dataclass
class Column:
    name: str
    type: str
    primary_key: bool = False
    nullable: bool = True

class Table:
    def __init__(self, name: str, columns: List[Column], lsm_path: str):
        self.name = name
        self.columns = {col.name: col for col in columns}
        self.primary_key_col = next((col for col in columns if col.primary_key), None)
        
        # Initialize storage
        self.lsm_store = LSMTree(lsm_path)
        self.index = BPlusTree(order=4)  # B+ tree index for primary key
        
        # Load existing data into index
        self._load_index()

    def _load_index(self):
        """Load existing data from LSM tree into B+ tree index"""
        # Use range_scan with minimum and maximum possible keys
        for key, value in self.lsm_store.range_scan("\0", "\xff"):
            if isinstance(value, dict):  # Ensure value is a valid row
                self.index.insert(key, value)

    def insert(self, row: Dict[str, Any]):
        """Insert a new row into the table"""
        # Validate row structure
        for col_name, value in row.items():
            if col_name not in self.columns:
                raise ValueError(f"Unknown column: {col_name}")
            
            if not self.columns[col_name].nullable and value is None:
                raise ValueError(f"Column {col_name} cannot be null")

        # Get primary key value
        if not self.primary_key_col:
            raise ValueError("No primary key defined for table")
            
        pk_value = row.get(self.primary_key_col.name)
        if pk_value is None:
            raise ValueError("Primary key value cannot be null")

        # Store in LSM tree
        self.lsm_store.put(pk_value, row)
        
        # Update index
        self.index.insert(pk_value, row)

    def get(self, primary_key: Any) -> Optional[Dict[str, Any]]:
        """Retrieve a row by its primary key"""
        return self.index.search(primary_key)

    def scan(self, start_key: Any = None, end_key: Any = None) -> List[Dict[str, Any]]:
        """Scan table for rows within key range"""
        if start_key is None or end_key is None:
            # Full table scan from LSM
            return [value for _, value in self.lsm_store.scan()]
        
        # Range scan using index
        return [row for _, row in self.index.range_search(start_key, end_key)]

    def update(self, primary_key: Any, new_values: Dict[str, Any]):
        """Update a row by its primary key"""
        existing_row = self.get(primary_key)
        if not existing_row:
            raise ValueError(f"No row found with primary key: {primary_key}")

        # Update values
        updated_row = existing_row.copy()
        updated_row.update(new_values)

        # Store updated row
        self.lsm_store.put(primary_key, updated_row)
        self.index.insert(primary_key, updated_row)

    def delete(self, primary_key: Any):
        """Delete a row by its primary key"""
        # Remove from LSM tree
        self.lsm_store.delete(primary_key)
        
        # Note: We should also implement deletion in B+ tree
        # For now, we'll rebuild the index
        self._load_index()
