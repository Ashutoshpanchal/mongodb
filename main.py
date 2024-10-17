from sqlalchemy import create_engine, text
from pymongo import MongoClient
import pandas as pd

# Connect to MongoDB
mongo_client = MongoClient("mongodb://username:password@localhost:27017/")
mongo_db = mongo_client["mydatabase"]
collection = mongo_db["mycollection"]
username = "admin"
password = "secretpassword"
# Create a SQLAlchemy engine for MongoDB
mongo_engine = create_engine(f'mongodb://{username}:{password}@localhost:27017/mydatabase')

# Insert sample data into MongoDB
sample_data = [
    {"name": "John", "age": 30, "department": "HR"},
    {"name": "Jane", "age": 25, "department": "IT"},
    {"name": "Bob", "age": 35, "department": "Finance"},
    {"name": "Alice", "age": 28, "department": "HR"}
]
collection.insert_many(sample_data)

# Example 1: Simple SELECT query
query = text("SELECT name, age FROM mycollection WHERE age > 25")
result = pd.read_sql(query, mongo_engine)
print("Example 1 - Simple SELECT:")
print(result)

# Example 2: Aggregation (COUNT)
query = text("SELECT department, COUNT(*) as count FROM mycollection GROUP BY department")
result = pd.read_sql(query, mongo_engine)
print("\nExample 2 - Aggregation:")
print(result)

# Example 3: JOIN (Note: MongoDB doesn't support JOINs natively, this is simulated)
# First, let's add another collection for departments
dept_collection = mongo_db["departments"]
dept_data = [
    {"name": "HR", "location": "New York"},
    {"name": "IT", "location": "San Francisco"},
    {"name": "Finance", "location": "Chicago"}
]
dept_collection.insert_many(dept_data)

# Now, let's perform a "JOIN" operation
query = text("""
    SELECT e.name, e.age, e.department, d.location
    FROM mycollection e
    JOIN departments d ON e.department = d.name
""")
result = pd.read_sql(query, mongo_engine)
print("\nExample 3 - Simulated JOIN:")
print(result)

# Clean up
collection.delete_many({})
dept_collection.delete_many({})
mongo_client.close()
