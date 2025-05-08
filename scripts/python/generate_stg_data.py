import random
import pandas as pd
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_data(run_id, **context) -> None:

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

    f = open("logs/generate_stg_data_logs.txt", "a")
    f.write('--------------------------------------------------------')
    f.write(f"\nSTARTED DAG RUN {run_id}\n")
    f.write('--------------------------------------------------------')

    num_rows = 10**5 + fake.random_int(min=1000, max=12000) - fake.random_int(min=1000, max=12000)
    for i in range(1, num_rows):
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
        orders_data["customer_id"].append(fake.random_choices(customers_data["customer_id"])[0])
        orders_data["product_id"].append(fake.random_choices(products_data["product_id"])[0])

        if i % 1000 == 0 or i == num_rows-1:
            if i == num_rows-1: f.write(f"\n{datetime.now()}: generation finished, rows in total: {i} \n")
            else: f.write(f"\n{datetime.now()}: generated {i} rows")
    
    f.write('---------------------------------------------------------')
    f.write(f"\nDAG RUN {run_id} FINISHED\n")
    f.write('---------------------------------------------------------\n')
    f.close()

    customers_df = pd.DataFrame(customers_data)
    products_df = pd.DataFrame(products_data)
    orders_df = pd.DataFrame(orders_data)

    customers_df.to_csv("../data/generated/customers.csv", index=False)
    products_df.to_csv("../data/generated/products.csv", index=False)
    orders_df.to_csv("../data/generated/orders.csv", index=False)