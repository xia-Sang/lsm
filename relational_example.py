import os
import sys

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from relational.database import Database
from relational.table import Column

def main():
    # Initialize database
    db = Database("./data/mydb")

    # Create users table
    users_table = db.create_table("users", [
        Column("id", "int", primary_key=True),
        Column("name", "str"),
        Column("email", "str", nullable=False),
    ])

    # Insert some data
    users_table.insert({
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com"
    })

    users_table.insert({
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com"
    })

    # Retrieve by primary key
    user = users_table.get(1)
    print(f"Found user: {user}")

    # Scan range
    users = users_table.scan(start_key=1, end_key=2)
    print(f"Users in range: {users}")

    # Update user
    users_table.update(1, {"name": "John Smith"})
    updated_user = users_table.get(1)
    print(f"Updated user: {updated_user}")

    # Delete user
    users_table.delete(2)
    deleted_user = users_table.get(2)
    print(f"Deleted user (should be None): {deleted_user}")

if __name__ == "__main__":
    main()
