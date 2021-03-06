"""

pip install --upgrade snowflake-sqlalchemy
    (which also install the snowflake connector for python)
pip install --upgrade snowflake-connector-python
"""

import pandas as pd
import datetime
import snowflake.connector as sc
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

import sf_config

SF_USER = sf_config.sf_username
SF_PASS = sf_config.sf_pw
SF_ACCT = sf_config.sf_account
SF_ROLE = sf_config.sf_role 
SF_WAREHOUSE = sf_config.sf_warehouse 
SF_DB = sf_config.sf_db
SF_SCHEMA = sf_config.sf_schema

"""
import json
with open(r'C:/Users/chandlerwhipple/Videos/sf_config.json') as config_file:
    config = json.load(config_file)
"""

valid_tlds = ['com','couk','cojp','ca','es','it','fr','de','mx']


def run_snowflake(query: str, 
                  role: str = SF_ROLE, 
                  warehouse: str = SF_WAREHOUSE, 
                  db: str = SF_DB, 
                  schema: str = SF_SCHEMA):
    """
    Runs queries on snowflake. Used for copy/create/delete/update statements affecting the snowflake database. Does not return data.
    To query data from snowflake into a pandas dataframe use get_sf_data()
    """
    con = sc.connect(  #creates connection
            user = SF_USER,
            password= SF_PASS,
            account = SF_ACCT
            )
    
    cur = con.cursor() #creates cursor. You execute pretty much everything from the cursor
    
    #First set up connection. role->warehouse->database->schema
    cur.execute("USE role " + role)
    cur.execute("USE warehouse " + warehouse)
    cur.execute("USE DATABASE " + db)
    cur.execute("USE SCHEMA " + schema)
    
    #Execute query
    cur.execute(query)
           
    #close the cursor and then the connection when you're done.
    cur.close
    con.close
    
def get_sf_data(query: str, 
                  role: str = SF_ROLE, 
                  warehouse: str = SF_WAREHOUSE, 
                  db: str = SF_DB, 
                  schema: str = SF_SCHEMA) -> pd.DataFrame:
    """
    Use this to run a select statment and export that data to a pandas data frame.
    """

    url = URL(
        account = SF_ACCT,
        user = SF_USER,
        password = SF_PASS,
        database = db,
        schema = schema,
        warehouse = warehouse,
        role=role,
    )
    
    engine = create_engine(url)
    connection = engine.connect()
    
    df = pd.read_sql_query(query, connection)    
    
    connection.close()
    engine.dispose()
    
    return df

def multiple_snowflake_queries(queries: list, 
                  role: str = SF_ROLE, 
                  warehouse: str = SF_WAREHOUSE, 
                  db: str = SF_DB, 
                  schema: str = SF_SCHEMA) -> pd.DataFrame:
    """
    Use this to run a select statment and export that data to a pandas data frame.
    """


    url = URL(
        account = SF_ACCT,
        user = SF_USER,
        password = SF_PASS,
        database = db,
        schema = schema,
        warehouse = warehouse,
        role=role,
    )
    
    engine = create_engine(url)
    connection = engine.connect()
    out = {}
    for q in queries:
        df = pd.read_sql_query(q, connection)
        out.update({q:df})
    
    connection.close()
    engine.dispose()
    
    return out

def upload_to_snowflake(df: pd.DataFrame, table: str, 
                  role: str = SF_ROLE, 
                  warehouse: str = SF_WAREHOUSE, 
                  db: str = SF_DB, 
                  schema: str = SF_SCHEMA,
                  chunksize: int = 16384,
                  if_exists: str = 'append',
                  index=False, 
                  index_label=None,
                  dtype=None):

    url = URL(
        account = SF_ACCT,
        user = SF_USER,
        password = SF_PASS,
        database = SF_DB,
        schema = SF_SCHEMA,
        warehouse = SF_WAREHOUSE,
        role = SF_ROLE,
    )
    
    engine = create_engine(url)
    connection = engine.connect()
    
    df.to_sql(table, connection, index=index, if_exists= if_exists, chunksize = chunksize, index_label = index_label, dtype = dtype)   
    
    connection.close()
    engine.dispose()
    
    return    



###############################
    
def copy_into(table: str, file: str, delim: str = ',', 
              skip: int = 1, column_count_error: str = 'true',
              on_error: str = 'CONTINUE', force: str = 'TRUE') -> pd.DataFrame:
    
    skip = str(skip)
    
    query = f'''
                COPY INTO  {table}
                    FROM {file}  
                        CREDENTIALS=(AWS_KEY_ID='{config['aws_access_key_id']}' AWS_SECRET_KEY='{config['aws_secret_access_key']}')
                        FILE_FORMAT = (type = csv field_delimiter = '{delim}' skip_header = {skip}, error_on_column_count_mismatch= {column_count_error})
                        ON_ERROR = '{on_error}' FORCE = {force}
                ;
            '''
            
    output = get_sf_data(query)
    
    return output

def get_client_data(client_id: int, field: str) -> str:
    query = f'''select {field} from clients where id = '{str(client_id)}';'''
    df = get_sf_data(query)
    return df[field][0]


def get_client_list(tld: str):
    
    query = f'''SELECT id 
                FROM clients
                WHERE tld = '{tld}' 
                    AND client_active = 1 
                    AND not internal_client = 1 
                    AND NOT id in (527,553,555) 
                    AND deleted_at is null
                    and not client_type_id = 10
                ;
            '''
    
    df = get_snowflake_data(query)
    id_list = df.id
    return id_list

def get_tld_client_asins(tld: str):
    query = f'''SELECT cat.asin, cat.client_id
            FROM catalog cat JOIN clients c
            ON cat.client_id = c.id
            WHERE c.tld = '{tld}'
                AND c.client_active = 1
                AND c.deleted_at is null
                AND c.internal_client <> 1
        '''
    df = get_snowflake_data(query)
    return df

def get_fced_data(asins: str, tld: str, wb: str, wb_end: str) -> pd.DataFrame:
    query = f'''
                SELECT *
                    FROM full_calculated_engine_data_oprah
                    WHERE tld = '{tld}'
                        AND week_beginning BETWEEN '{wb}' AND '{wb_end}'
                        AND asin in ({asins})
                ;
            '''
    df = get_sf_data(query)
    return df


#### Tagging system queries (in development) ###########
def list_to_sql(t: list):
    t = t
    t = "','".join(t)
    t = f"'{t}'"
    return t

def sanitize_tags(t: list):
    valid_tags = list(get_sf_data('''select distinct tag from amz_pg_tags;''').tag)
    iv = list(filter(lambda x: x not in valid_tags, t))
    if len(iv) > 0:
        iv = list_to_sql(iv)
        raise Exception(f'The following tags are not valid: {iv}.')
    else:
        return True
    
def sanitize_tlds(t:list):
    iv = list(filter(lambda x: x not in valid_tlds, t))
    if len(iv) > 0:
        iv = list_to_sql(iv)
        raise Exception(f'The following tlds are not valid: {iv}.')
    else:
        return True
    
def sanitize_dates(t:list):
    for date_text in t:
        try:
            datetime.datetime.strptime(date_text, '%Y-%m-%d')
        except ValueError:
            raise ValueError(f'{date_text} is not a valid date, should be YYYY-MM-DD')
    return True
        
def sanitize_all(tags: list = [], tlds: list = [], dates: list = []):
    if len(tags) > 0: sanitize_tags(tags)
    if len(tlds) > 0: sanitize_tlds(tlds)
    if len(dates) > 0: sanitize_dates(dates)
    
def all_tags_query(tags: list, tld: str, wb: str, weeks: int = 1) -> pd.DataFrame:
    sanitize_all(tags,[tld],[wb])
    ntags = len(set(tags))
    tags = list_to_sql(tags)
    wb_start = str(datetime.datetime.strptime(wb, '%Y-%m-%d').date() \
                     - datetime.timedelta((weeks-1)*7))
    query = f'''
                SELECT fced.* 
                FROM full_calculated_engine_data_oprah fced
                    JOIN (SELECT asin FROM amz_pg_tags
                            WHERE tld = '{tld}'
                                AND tag in ({tags})
                            GROUP BY 1
                            HAVING count(distinct(tag)) = {ntags}
                         )
                    USING (asin)
                WHERE fced.tld = '{tld}'
                    AND fced.week_beginning BETWEEN '{wb_start}' and '{wb}'
                ;
            '''
    df = get_sf_data(query)
    return df

def any_tag_query(tags: list, tld: str, wb: str, weeks: int = 1) -> pd.DataFrame:
    sanitize_all(tags,[tld],[wb])
    tags = list_to_sql(tags)
    wb_start = str(datetime.datetime.strptime(wb, '%Y-%m-%d').date() \
                     - datetime.timedelta((weeks-1)*7))
    query = f'''
                SELECT fced.* 
                FROM full_calculated_engine_data_oprah fced
                    JOIN (SELECT distinct asin FROM amz_pg_tags
                            WHERE tld = '{tld}'
                                AND tag in ({tags})
                         )
                    USING (asin)
                WHERE fced.tld = '{tld}'
                    AND fced.week_beginning BETWEEN '{wb_start}' and '{wb}'
                ;
            '''
    df = get_sf_data(query)
    return df

def tag_comparison_query(tags: list, tld: str, wb: str, weeks: int = 1) -> pd.DataFrame:
    sanitize_all(tags,[tld],[wb])
    tags = list_to_sql(tags)
    wb_start = str(datetime.datetime.strptime(wb, '%Y-%m-%d').date() \
                     - datetime.timedelta((weeks-1)*7))
    query = f'''
                SELECT t.tag, fced.week_beginning, sum(fced.sales_1p), sum(fced.sales_3p), count(*)
                FROM full_calculated_engine_data_oprah fced
                    JOIN (SELECT asin, listagg(distinct(tag),'|') as tags FROM amz_pg_tags
                            WHERE tld = '{tld}'
                                AND tag in ({tags})
                         ) t
                    USING (asin)
                WHERE fced.tld = '{tld}'
                    AND fced.week_beginning BETWEEN '{wb_start}' and '{wb}'
                ;
            '''
    df = get_sf_data(query)
    return df

