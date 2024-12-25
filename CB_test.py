from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException
import uuid

# Connect to your Couchbase cluster
cluster = Cluster('couchbase://192.168.1.15', ClusterOptions(
    PasswordAuthenticator('admin', '1234567')
))

# Open a bucket (assuming you have a bucket named 'Testing')
bucket = cluster.bucket('Testing')
collection = bucket.default_collection()

# Sample data for four different tables/document types
users = [
    {"type": "user", "id": "user1", "name": "John Doe", "email": "john@example.com", "age": 30},
    {"type": "user", "id": "user2", "name": "Jane Smith", "email": "jane@example.com", "age": 28}
]

products = [
    {"type": "product", "id": "prod1", "name": "Laptop", "price": 999.99, "stock": 50},
    {"type": "product", "id": "prod2", "name": "Smartphone", "price": 599.99, "stock": 100}
]

orders = [
    {"type": "order", "id": "order1", "user_id": "user1", "product_id": "prod1", "quantity": 2, "total": 1999.98},
    {"type": "order", "id": "order2", "user_id": "user2", "product_id": "prod2", "quantity": 1, "total": 599.99}
]

reviews = [
    {"type": "review", "id": "review1", "user_id": "user1", "product_id": "prod1", "rating": 5, "comment": "Great laptop!"},
    {"type": "review", "id": "review2", "user_id": "user2", "product_id": "prod2", "rating": 4, "comment": "Good phone, but battery life could be better"}
]

# Function to insert documents
def insert_documents(docs):
    for doc in docs:
        try:
            key = f"{doc['type']}::{doc['id']}"
            collection.upsert(key, doc)
            print(f"Inserted document with key: {key}")
        except CouchbaseException as e:
            print(f"An error occurred: {e}")

# Insert sample data
insert_documents(users)
insert_documents(products)
insert_documents(orders)
insert_documents(reviews)

# Create a new document type that combines all the information
combined_data = []
for order in orders:
    user = next(u for u in users if u['id'] == order['user_id'])
    product = next(p for p in products if p['id'] == order['product_id'])
    review = next((r for r in reviews if r['user_id'] == order['user_id'] and r['product_id'] == order['product_id']), None)
    
    combined_doc = {
        "type": "combined",
        "order_id": order['id'],
        "user_name": user['name'],
        "user_email": user['email'],
        "product_name": product['name'],
        "product_price": product['price'],
        "order_quantity": order['quantity'],
        "order_total": order['total'],
        "review_rating": review['rating'] if review else None,
        "review_comment": review['comment'] if review else None
    }
    combined_data.append(combined_doc)

# Insert combined documents
insert_documents(combined_data)

print("Sample data insertion complete.")

# Query to retrieve all combined documents
bucket_name = 'Testing'
scope = bucket.scope("_default")

query = f"""
SELECT *
FROM `{bucket_name}`.`_default`.`_default`
WHERE type = 'combined'
"""

print("Executing query:", query)

# Execute the query
result = scope.query(query)

# Print the results
for row in result:
    print(row)
