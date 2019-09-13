import MySQLdb
import pandas as pd
from sqlalchemy import create_engine

def splitListToRows(row,row_accumulator,target_column,separator):
        split_row = row[target_column].split(separator)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s
            row_accumulator.append(new_row)

connection = MySQLdb.connect (host = "127.0.0.1", user = "hadoop", passwd = "sayan123", db = "model")
df_content = pd.read_sql('SELECT * FROM content_rec', con=connection)

new_rows = []
df_content.apply(splitListToRows,axis=1,args = (new_rows,"Category Ids",","))
new_df = pd.DataFrame(new_rows)


categories = new_df["Category Ids"].drop_duplicates()

df = pd.DataFrame()

df["Node"] = new_df["Node"].drop_duplicates()


for c in categories:
    h = "category_"+ str(c).replace(" ","")
    df[h] = 0

for index, row in new_df.iterrows():
    print row
    node = row["Node"]
    cat = row["Category Ids"]
    cat =  "category_" + str(cat).replace(" ", "")
    df.loc[df["Node"]== node, cat] = 1

df = df.drop_duplicates()

print df
engine = create_engine("mysql+mysqldb://hadoop:"+'sayan123'+"@127.0.0.1/model")
with engine.connect() as conn, conn.begin():
    df.to_sql('content_map', conn, if_exists='replace')


df.to_csv('video_category.csv')
