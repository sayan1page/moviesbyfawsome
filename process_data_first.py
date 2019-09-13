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
    
f = open('/home/ec2-user/rest_api/mongoconfig.txt')
mongoconfig = {}
for line in f:
    fields = line.strip().split('=')
    mongoconfig[fields[0]] = fields[1].strip()
f.close()
    
connection = None
df_score = None
df_content = None

try:
    connection = MySQLdb.connect (host = config['hostname'], user = config['username'], passwd = config['passwd'], db = config['dbname'])
    df_score = pd.read_sql('SELECT * FROM score_rec', con=connection)
    client = MongoClient('mongodb://'+mongoconfig['usr']+':'+ mongoconfig['pswd']+'@'+mongoconfig['host']+'/'+mongoconfig['db'])
    db = client[mongoconfig['db']]
    collection = db[mongoconfig['collection']]
    res = collection.find({"_type" : "node"})
    data = []
    for r in res:
        if 'field_post_main_category' in r:
            if r['field_post_main_category'] is not None:
                if len(r['field_post_main_category'])==1:
                    data.append([r['nid'], r['field_post_main_category']['target_id']])
                else:
                    for r1 in r['field_post_main_category']:
                        data.append([r['nid'], r1['target_id']])
    df_content = pd.DataFrame(data, columns=['Node','Category Ids'])
    client.close()
except:
    connection.close()
    exit()

df_score['Node'] = df_score['Node'].str.strip()
df_content['Node'] = df_content['Node'].apply(str).str.strip() 
df_content['Category Ids'] = df_content['Category Ids'].apply(str).str.strip()
new_df = pd.merge(df_score, df_content, left_on=['Node'], right_on=['Node'], how='inner')

new_df = new_df.drop(["Node"],1,inplace=False)
print new_df
new_df = new_df[['Category Ids','featurename','featurevalue','featureCount','jointCount']]
engine = create_engine("mysql+mysqldb://"+config['username']+':'+config['passwd']+"@"+config['hostname']+"/" + config['dbname'])
new_df.to_sql(con=engine, name='category_score', if_exists='replace',index=False)
cursor = connection.cursor()
cursor.execute ("drop table score_rec")
cursor.close()
connection.close()
