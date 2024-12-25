from sqlalchemy import create_engine, text
from pymongo import MongoClient
import pandas as pd
from pymongo.errors import OperationFailure

# Connect to MongoDB
username = "admin"
password = "secretpassword"
mongo_client = MongoClient(f"mongodb://{username}:{password}@localhost:27017/")
mongo_db = mongo_client["new_database"]

# Get the employees collection
employees_collection = mongo_db["employees"]

def merge_employee_records():
    pipeline = [
        {
            "$group": {
                "_id": "$employee_id",
                "merged_doc": {
                    "$mergeObjects": "$$ROOT"
                }
            }
        },
        {
            "$replaceRoot": {
                "newRoot": "$merged_doc"
            }
        }
    ]
    
    merged_records = list(employees_collection.aggregate(pipeline))
    
    # Clear the collection
    employees_collection.delete_many({})
    
    # Insert merged records
    if merged_records:
        employees_collection.insert_many(merged_records)
    
    print("Employee records merged successfully.")

def update_schema_validator(collection):
    # Get all unique fields from existing documents
    all_fields = set()
    for doc in collection.find({}, {"_id": 0}):
        all_fields.update(doc.keys())

    # Create a validator that allows all existing fields and any new fields
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "additionalProperties": True,
            "properties": {field: {} for field in all_fields}
        }
    }

    try:
        collection.database.command({
            "collMod": collection.name,
            "validator": validator,
            "validationLevel": "moderate"
        })
        print("Schema validator updated successfully.")
    except OperationFailure as e:
        print(f"Failed to update schema validator: {e}")

def add_new_field_to_all_records(collection, new_field, default_value=None):
    collection.update_many(
        {new_field: {"$exists": False}},
        {"$set": {new_field: default_value}}
    )
    print(f"Added new field '{new_field}' to all records with default value: {default_value}")

def add_employee(employee_data):
    # Check for new fields
    existing_fields = set(employees_collection.find_one({}, {"_id": 0}).keys())
    new_fields = set(employee_data.keys()) - existing_fields

    # Add new fields to all existing records
    for new_field in new_fields:
        add_new_field_to_all_records(employees_collection, new_field, None)

    # Update or insert the employee
    employees_collection.update_one(
        {"employee_id": employee_data["employee_id"]},
        {"$set": employee_data},
        upsert=True
    )

    # Update the schema validator
    update_schema_validator(employees_collection)

    print(f"Employee added/updated: {employee_data}")

# Merge duplicate records
merge_employee_records()

# Update the schema validator
update_schema_validator(employees_collection)

# Example usage: Adding a new employee with new fields
new_employee = {
    "employee_id": 7,
    "name": "Alice Johnson",
    "department": "Marketing",
    "salary": 58000,
    "phone_number": "1234567895",
    "email": "alice.johnson@company.com",
    "start_date": "2023-01-15",
    "end_date": "2023-01-15"
}

add_employee(new_employee)

# Adding another employee with a new field
another_employee = {
    "employee_id": 8,
    "name": "Bob Wilson",
    "department": "Sales",
    "salary": 62000,
    "phone_number": "1234567896",
    "email": "bob.wilson@company.com",
    "start_date": "2023-02-01",
    "performance_rating": 4.5  # New field
}

add_employee(another_employee)

# Verify the updated data
print("\nVerifying data:")
for employee in employees_collection.find():
    print(employee)
