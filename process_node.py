import MySQLdb
import pandas as pd
from sqlalchemy import create_engine

f = open('/home/ec2-user/rest_api/dbconfig.txt')
config = {}
for line in f:
    fields = line.strip().split(' ')
    config[fields[0]] = fields[1].strip()
f.close()

connection = MySQLdb.connect (host = config['hostname'], user = config['username'], passwd = config['passwd'], db = config['dbname'])
new_df = pd.read_sql('SELECT * FROM node_popular', con=connection)
old_df = pd.read_sql('SELECT * FROM node_popular_all', con=connection)

new_df1 = pd.read_sql('SELECT * FROM node_view', con=connection)
old_df1 = pd.read_sql('SELECT * FROM node_view_all', con=connection)

connection.close()

old_df.rename(columns={'sum(watch_time)':'sum(watch_time)1'}, inplace=True)

merge_df = pd.merge(old_df, new_df, how ='outer', left_on=['Node'], right_on=['Node'])

merge_df = merge_df.fillna(0)

merge_df['sum(watch_time)'] = merge_df['sum(watch_time)'] + merge_df['sum(watch_time)1']

merge_df =  merge_df.drop(['sum(watch_time)1'],1 ,inplace=False)


old_df1.rename(columns={'sum(Identifier)':'sum(Identifier)1'}, inplace=True)

merge_df1 = pd.merge(old_df1, new_df1, how ='outer', left_on=['Node'], right_on=['Node'])

merge_df1 = merge_df1.fillna(0)

merge_df1['sum(Identifier)'] = merge_df1['sum(Identifier)'] + merge_df1['sum(Identifier)1']

merge_df1 =  merge_df1.drop(['sum(Identifier)1'],1 ,inplace=False)


merge_df_final = pd.merge(merge_df,merge_df1, how='outer',left_on=['Node'], right_on=['Node'])

merge_df_final = merge_df_final.fillna(1)

print merge_df_final

merge_df_final['score'] = merge_df_final['sum(Identifier)'] *  merge_df_final['sum(watch_time)']

merge_df_final = merge_df_final.drop(['sum(Identifier)','sum(watch_time)'],1,inplace=False)


engine = create_engine("mysql+mysqldb://"+config['username']+':'+config['passwd']+"@"+config['hostname']+"/" + config['dbname'])
merge_df_final.to_sql(con=engine, name='node_popular_final', if_exists='replace',index=False)

