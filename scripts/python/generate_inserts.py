import pandas as pd
from datetime import datetime

def generate_inserts(**kwargs) -> None:
    target_schema_name = kwargs['target_schema_name']
    target_table_name = kwargs['target_table_name']
    file_name = kwargs['file_name']

    df = pd.read_csv(f'/opt/data/generated/{file_name}')
    
    insert_queries = []
    for index, row in df.iterrows():
        insert_query = f"INSERT INTO {target_schema_name}.{target_table_name} VALUES ({row['order_id']}, '{row['order_date']}', {row['customer_id']}, {row['product_id']}, '{datetime.now()}');"
        insert_queries.append(insert_query)
    
    # Save queries to a file for the PostgresOperator to execute
    with open(f'/opt/scripts/sql/insert_{target_schema_name}__{target_table_name}.sql', 'w') as f:
        for query in insert_queries:
            f.write(f"{query}\n")

