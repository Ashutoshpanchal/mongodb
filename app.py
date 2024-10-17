from flask import Flask
from flask_mongoalchemy import MongoAlchemy
import pandas as pd

app = Flask(__name__)
username = "admin"
password = "secretpassword"
app.config['MONGOALCHEMY_DATABASE'] = 'mydatabase'
app.config['MONGOALCHEMY_CONNECTION_STRING'] = f'mongodb://{username}:{password}@localhost:27017/'

db = MongoAlchemy(app)

# Define models
class Employee(db.Document):
    name = db.StringField()
    age = db.IntField()
    department = db.StringField()

class Department(db.Document):
    name = db.StringField()
    location = db.StringField()

# Insert sample data
def insert_sample_data():
    # Employee.query.remove()
    # Department.query.remove()

    employees = [
        Employee(name="John", age=30, department="HR"),
        Employee(name="Jane", age=25, department="IT"),
        Employee(name="Bob", age=35, department="Finance"),
        Employee(name="Alice", age=28, department="HR")
    ]
    for emp in employees:
        emp.save()

    departments = [
        Department(name="HR", location="New York"),
        Department(name="IT", location="San Francisco"),
        Department(name="Finance", location="Chicago")
    ]
    for dept in departments:
        dept.save()

# Example queries
def run_queries():
    # Example 1: Simple SELECT query
    print("Example 1 - Simple SELECT:")
    result = Employee.query.filter(Employee.age > 25).all()
    df = pd.DataFrame([{'name': e.name, 'age': e.age} for e in result])
    print("Example 1 - Simple SELECT:")
    print(df)

    # # Example 2: Aggregation (COUNT)
    # result = Employee.query.group_by(Employee.department).all()
    # df = pd.DataFrame([{'department': group.department, 'count': len(group.items)} for group in result])
    # print("\nExample 2 - Aggregation:")
    # print(df)

    # Example 3: JOIN (Note: MongoAlchemy doesn't support JOINs directly, so we'll simulate it)
    employees = Employee.query.all()
    departments = Department.query.all()
    
    joined_data = []
    for emp in employees:
        dept = next((d for d in departments if d.name == emp.department), None)
        if dept:
            joined_data.append({
                'name': emp.name,
                'age': emp.age,
                'department': emp.department,
                'location': dept.location
            })
    
    df = pd.DataFrame(joined_data)
    print("\nExample 3 - Simulated JOIN:")
    print(df)

if __name__ == '__main__':
    with app.app_context():
        insert_sample_data()
        run_queries()