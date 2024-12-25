from pymongo import MongoClient
from datetime import datetime


username = "admin"
password = "secretpassword"
mongo_client = MongoClient(f"mongodb://{username}:{password}@localhost:27017/")
# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['new_database']

# Define the collections
sales_voucher_details = db['sales_voucher_details']
customer_master_view = db['customer_master_view']
item_master_view = db['item_master_view']

# Define the date range
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 10, 17)

# MongoDB aggregation pipeline
pipeline = [
    {
        "$match": {
            "document_type": {"$in": ["SalesReturn", "Sales", "DebitNote", "CreditNote"]},
            "billdate": {"$gte": start_date, "$lte": end_date}
        }
    },
    {
        "$addFields": {
            "customer_id": {
                "$cond": [
                    {"$eq": ["$customer_id", "Not Defined"]},
                    {"$cond": [
                        {"$eq": ["$bill_to_id", "Not Defined"]},
                        {"$cond": [
                            {"$eq": ["$bill_ship_id", "Not Defined"]},
                            "Not Defined",
                            "$bill_ship_id"
                        ]},
                        "$bill_to_id"
                    ]},
                    "$customer_id"
                ]
            }
        }
    },
    {
        "$lookup": {
            "from": "customer_master_view",
            "let": {"guid": "$guid", "customer_id": "$customer_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$guid", "$$guid"]},
                                {"$eq": ["$customer_id", "$$customer_id"]}
                            ]
                        }
                    }
                }
            ],
            "as": "customer_info"
        }
    },
    {"$unwind": "$customer_info"},
    {
        "$lookup": {
            "from": "item_master_view",
            "let": {"guid": "$guid", "item_id": "$item_id"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$guid", "$$guid"]},
                                {"$eq": ["$item_id", "$$item_id"]}
                            ]
                        }
                    }
                }
            ],
            "as": "item_info"
        }
    },
    {"$unwind": "$item_info"},
    {
        "$group": {
            "_id": {
                "batch__name": "$batch__name",
                "cg_3": "$customer_info.cg_3",
                "customer_type": "$customer_info.customer_type",
                "item_name": "$item_info.item_name",
                "company_name": "$company_name",
                "cg_4": "$customer_info.cg_4",
                "category": "$item_info.category",
                "gstno": "$customer_info.gstno",
                "item_alias": "$item_info.item_alias",
                "cg_1": "$customer_info.cg_1",
                "billdate": "$billdate",
                "document_type": "$document_type",
                "item__costcenter": "$item__costcenter",
                "customer_name": "$customer_info.customer_name",
                "mobileno": "$customer_info.mobileno",
                "ig_1": "$item_info.ig_1",
                "customer_alias": "$customer_info.customer_alias",
                "referenceno": "$referenceno",
                "voucher_type": "$voucher_type",
                "billno": "$billno",
                "phoneno": "$customer_info.phoneno",
                "godown__name": "$godown__name",
                "ig_2": "$item_info.ig_2",
                "cg_2": "$customer_info.cg_2"
            },
            "qty": {"$sum": "$qty"},
            "taxpaidvalue": {"$sum": "$taxpaidvalue"},
            "taxlessvalue": {"$sum": "$taxlessvalue"},
            "discount": {"$sum": "$discount"}
        }
    },
    {
        "$project": {
            "_id": 0,
            "batch__name": "$_id.batch__name",
            "cg_3": "$_id.cg_3",
            "customer_type": "$_id.customer_type",
            "item_name": "$_id.item_name",
            "company_name": "$_id.company_name",
            "cg_4": "$_id.cg_4",
            "category": "$_id.category",
            "gstno": "$_id.gstno",
            "item_alias": "$_id.item_alias",
            "cg_1": "$_id.cg_1",
            "billdate": "$_id.billdate",
            "document_type": "$_id.document_type",
            "item__costcenter": "$_id.item__costcenter",
            "customer_name": "$_id.customer_name",
            "mobileno": "$_id.mobileno",
            "ig_1": "$_id.ig_1",
            "customer_alias": "$_id.customer_alias",
            "referenceno": "$_id.referenceno",
            "voucher_type": "$_id.voucher_type",
            "billno": "$_id.billno",
            "phoneno": "$_id.phoneno",
            "godown__name": "$_id.godown__name",
            "ig_2": "$_id.ig_2",
            "cg_2": "$_id.cg_2",
            "qty": 1,
            "taxpaidvalue": {"$divide": ["$taxpaidvalue", 1]},
            "taxlessvalue": {"$divide": ["$taxlessvalue", 1]},
            "discount": 1,
            "avg_price": {
                "$cond": [
                    {"$eq": ["$qty", 0]},
                    0,
                    {"$divide": [{"$round": [{"$divide": ["$taxpaidvalue", 1]}, 0]}, "$qty"]}
                ]
            }
        }
    },
    {
        "$sort": {
            "customer_name": 1, "ig_1": 1, "cg_4": 1, "category": 1, "batch__name": 1,
            "customer_alias": 1, "document_type": 1, "billdate": 1, "cg_1": 1,
            "company_name": 1, "godown__name": 1, "billno": 1, "cg_2": 1,
            "customer_type": 1, "gstno": 1, "referenceno": 1, "item_name": 1,
            "voucher_type": 1, "item__costcenter": 1, "item_alias": 1, "ig_2": 1,
            "cg_3": 1, "mobileno": 1, "phoneno": 1
        }
    }
]

# Execute the aggregation pipeline
result = list(sales_voucher_details.aggregate(pipeline))

# Print the results
for doc in result:
    print(doc)
