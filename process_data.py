import MySQLdb
import pandas as pd
from sqlalchemy import create_engine
import pymongo
from pymongo import MongoClient
import collections

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

categories = pd.read_csv('/home/ec2-user/rest_api/categories.csv', names=['Category','id'])
valid_category = categories.id.tolist()

try:
    connection = MySQLdb.connect (host = config['hostname'], user = config['username'], passwd = config['passwd'], db = config['dbname'])
    df_score = pd.read_sql('SELECT * FROM score_rec', con=connection)
    old_df = pd.read_sql('SELECT * FROM category_score', con=connection)
    client = MongoClient('mongodb://'+mongoconfig['usr']+':'+ mongoconfig['pswd']+'@'+mongoconfig['host']+'/'+mongoconfig['db'])
    db = client[mongoconfig['db']]
    collection = db[mongoconfig['collection']]
    data = []
    res = collection.find({"_type" : "node", "status":1})
    for r in res:
        node = str(r['nid']).strip()
        if 'field_node_index' in r:
            if r['field_node_index'] is not None:
                r1 = r['field_node_index']
                for r2 in r1:
                    if r2['target_id'] is not None:
                        categories = r2['target_id']
                        if isinstance(categories, collections.Iterable):
                            for c in categories:
                                if c in valid_category:
                                    data.append(node, c)
                        else:
                            c = categories
                            if c in valid_category:
                                data.append(node,c)
    df_content = pd.DataFrame(data, columns=['Node','Category Ids'])
    client.close()
except:
    connection.close()
    exit()

df_score['Node'] = df_score['Node'].str.strip()

df_content.Node = df_content.Node.astype(str)
df_content['Node'] = df_content['Node'].str.strip()


new_df = pd.merge(df_score, df_content, left_on=['Node'], right_on=['Node'], how='inner')

new_df = new_df.drop(["Node"],1,inplace=False)



old_df.rename(columns={'featureCount':'featureCount1', 'jointCount':'jointCount1'}, inplace=True)

merge_df = pd.merge(old_df, new_df, how ='outer', left_on=['featurename','featurevalue','Category Ids'], right_on=['featurename','featurevalue','Category Ids'])

merge_df = merge_df.fillna(0)

merge_df['featureCount'] = merge_df['featureCount'] + merge_df['featureCount1']
merge_df['jointCount'] = merge_df['jointCount'] + merge_df['jointCount1']

merge_df =  merge_df.drop(['featureCount1','jointCount1'],1 ,inplace=False)

print merge_df

merge_df = merge_df[['Category Ids','featurename','featurevalue','featureCount','jointCount']]

cursor = connection.cursor()
cursor.execute ("drop table category_score")
cursor.execute ("drop table score_rec")
cursor.execute ("drop table threshold")
cursor.execute ("create table threshold like threshold1")
cursor.execute ("insert into threshold select * from threshold1")
connection.commit()
cursor.close()
connection.close()
engine = create_engine("mysql+mysqldb://"+config['username']+':'+config['passwd']+"@"+config['hostname']+"/" + config['dbname'])
merge_df.to_sql(con=engine, name='category_score', if_exists='replace',index=False)

