#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import sqlalchemy
import pyodbc
from sqlalchemy import create_engine


# In[35]:


print("Starting ETL process...")


# In[2]:


data= pd.read_csv(r'C:\Users\USER\Desktop\MOCK_DATA.csv')


# In[37]:


print(f"Extracted {len(df)} rows from CSV.")


# In[39]:


df= pd.DataFrame(data)


# In[41]:


df.info()


# In[43]:


df['discount%']=df['discount%'].fillna(2)


# In[45]:


df.info()


# In[47]:


df['order_date'] = pd.to_datetime(df['order_date'], dayfirst=True)


# In[49]:


df.info()


# In[51]:


df['total_price']= df['quantity']*df['unit_price']


# In[53]:


df.info()


# In[55]:


df.duplicated().any()


# In[57]:


server="DESKTOP-CU4E43M"
database="SalesProject"


# In[59]:


connection_string=f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+sql+Server&trusted_connection=yes'


# In[61]:


engine=create_engine(connection_string)
connection=engine.connect()


# In[63]:


print(connection)


# In[65]:


df.to_sql(
    name='Sales',         # Name of the table in SQL Server
    con=engine,                # The connection engine
    if_exists='replace',       # Options: 'replace', 'append', or 'fail'
    index=False,               # Don't write DataFrame index as a column
    schema='dbo'               # Default schema (optional, but good to specify)
)


# In[66]:


print("Loaded data into the database.")


# In[69]:


print("ETL process completed.")


# In[ ]:




