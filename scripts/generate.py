import random
import pandas as pd
from faker import Faker

fake = Faker()

def generate_data() -> None:
    customers_data = {
        "customer_id": [],
        "name": [],
        "address": [],
        "phone": [],
        "email": [],
        "gender": [],
        "date_of_birth": []
    }

    products_data = {
        "product_id": [],
        "category": [],
        "price": [],
    }

    orders_data = {
        "order_id": [],
        "order_date": [],
        "customer_id": [],
        "product_id": [],
    }

    for _ in range(1000):
        customers_data["customer_id"].append(fake.random_int(min=100, max=10**10))
        customers_data["name"].append(fake.name())
        customers_data["address"].append(fake.address())
        customers_data["phone"].append(fake.phone_number())
        customers_data["email"].append(fake.email())
        customers_data["gender"].append(fake.random_element(elements=("male", "female")))
        customers_data["date_of_birth"].append(fake.date_of_birth())

        products_data["product_id"].append(fake.random_int(min=1, max=1000))
        products_data["category"].append(fake.random_element(elements=("Electronics", "Clothing", "Books", "Home", "Sports")))
        products_data["price"].append(fake.random_int(min=500, max=10000))

        orders_data["order_id"].append(fake.random_int(min=100, max=10**10))
        orders_data["order_date"].append(fake.date_time())
        orders_data["customer_id"].append(fake.random_choice(customers_data["customer_id"]))
        orders_data["product_id"].append(fake.random_choice(products_data["product_id"]))
    
    customers_df = pd.DataFrame(customers_data)
    products_df = pd.DataFrame(products_data)
    orders_df = pd.DataFrame(orders_data)

    customers_df.to_csv("datawarehouse/data/customers.csv", index=False)
    products_df.to_csv("datawarehouse/data/products.csv", index=False)
    orders_df.to_csv("datawarehouse/data/orders.csv", index=False)