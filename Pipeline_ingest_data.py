#!/usr/bin/env python
# coding: utf-8

# In[2]:


import sys
print(sys.executable)

year = 2021
month = 1

# In[3]:


import pandas as pd
from sqlalchemy import create_engine

pg_user = 'root'
pg_pass = 'root'
pg_host = 'localhost'
pg_port = '5432'
pg_database = 'ny_taxi'

# Read a sample of the data
prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
df = pd.read_csv(url, nrows=100)

# Display first rows
df.head()

# Check data types
df.dtypes

# Check data shape
df.shape


# In[4]:


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

df = pd.read_csv(
    prefix + 'yellow_tripdata_2021-01.csv.gz',
    nrows=100,
    dtype=dtype,
    parse_dates=parse_dates
)




engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_database}')


# In[10]:


print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# In[11]:


df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')


# In[12]:


df_iter = pd.read_csv(
    url,
    iterator=True,
    chunksize=100000
)


# In[13]:


for df_chunk in df_iter:
    print(len(df_chunk))


# In[14]:


df_chunk.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')


# In[15]:


first = True

for df_chunk in df_iter:

    if first:
        # Create table schema (no data)
        df_chunk.head(0).to_sql(
            name="yellow_taxi_data",
            con=engine,
            if_exists="replace"
        )
        first = False
        print("Table created")

    # Insert chunk
    df_chunk.to_sql(
        name="yellow_taxi_data",
        con=engine,
        if_exists="append"
    )

    print("Inserted:", len(df_chunk))


# In[16]:


from tqdm.auto import tqdm

for df_chunk in tqdm(df_iter):
    ...


# In[ ]:




