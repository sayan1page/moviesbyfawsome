import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
import pymongo
from pymongo import MongoClient


f = open('/home/ec2-user/rest_api/dbconfig.txt')
config = {}
for line in f:
    fields = line.strip().split(' ')
    config[fields[0]] = fields[1].strip()
f.close()
    

def splitListToRows(row,row_accumulator,target_column,separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)

connection = None
df_vertices = None
df_edges = None
df_content = None


try:
    connection = MySQLdb.connect (host = config['hostname'], user = config['username'], passwd = config['passwd'], db = config['dbname'])
    df_vertices = pd.read_sql('SELECT * FROM rankv order by rank desc limit 10000', con=connection)
    df_edges = pd.read_sql('SELECT * FROM ranke order by rank desc limit 30000', con=connection)
    df_content = pd.read_sql('SELECT * FROM content_rec', con=connection)    
except:
    connection.close()
    exit()


df_vertices['Node'], df_vertices['device'] = df_vertices['identifier'].str.split('_',1).str

df_vertices = df_vertices.drop(["identifier"],1,inplace=False)

df_vertices['Node'] = df_vertices['Node'].str.strip()
df_content['Node'] = df_content['Node'].apply(str).str.strip() 

df_merged = pd.merge(df_vertices, df_content, left_on=['Node'], right_on=['Node'], how='inner')

new_rows = []
df_merged.apply(splitListToRows,axis=1,args = (new_rows,"Category Ids",","))
new_df_vertices = pd.DataFrame(new_rows)

new_df_vertices = new_df_vertices.drop(["Node"],1,inplace=False)

print new_df_vertices

df_edges['srcNode'], df_edges['device'] = df_edges['source'].str.split('_',1).str

df_edges = df_edges.drop(["source"], 1,inplace=False)

df_edges['srcNode'] = df_edges['srcNode'].str.strip()

df_merged = pd.merge(df_edges, df_content, left_on=['srcNode'], right_on=['Node'], how='inner')

df_merged = df_merged.drop(["srcNode","device"],1,inplace=False)

new_rows = []
df_merged.apply(splitListToRows,axis=1,args = (new_rows,"Category Ids",","))
new_df_edges = pd.DataFrame(new_rows)
new_df_edges = new_df_edges.drop(["Node"],1,inplace=False)
new_df_edges = new_df_edges.rename(columns={"Category Ids": "Source_Category"})

new_df_edges['Node'], new_df_edges['device'] = new_df_edges['dest'].str.split('_',1).str
new_df_edges = new_df_edges.drop(["dest"], 1,inplace=False)

new_df_edges['Node'] = new_df_edges['Node'].str.strip()
df_merged = pd.merge(new_df_edges, df_content, left_on=['Node'], right_on=['Node'], how='inner')

new_rows = []
df_merged.apply(splitListToRows,axis=1,args = (new_rows,"Category Ids",","))
new_df_edges = pd.DataFrame(new_rows)
new_df_edges = new_df_edges.drop(["Node"],1,inplace=False)
new_df_edges = new_df_edges.rename(columns={"Category Ids": "Dest_Category"})
print new_df_edges

engine = create_engine("mysql+mysqldb://"+config['username']+':'+config['passwd']+"@"+config['hostname']+"/" + config['dbname'])
new_df_vertices.to_sql(con=engine, name='converted_vertice', if_exists='replace',index=False)
new_df_edges.to_sql(con=engine, name='converted_edges', if_exists='replace',index=False)


connection.close()
