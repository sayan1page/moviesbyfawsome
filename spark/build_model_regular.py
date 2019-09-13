from pyspark.sql import SQLContext
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import trim
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import pyspark.sql
import GeoIP



def ip_to_geo(ip):
    gi = GeoIP.open("/home/hadoop/GeoLiteCity.dat", GeoIP.GEOIP_STANDARD)
    gir = gi.record_by_name(ip)
    del gi
    if gir is not None:
        if gir['city'] is None:
            gir['city']=' '
        if gir['region_name'] is None:
            gir['region_name']=' '
        if gir['country_name'] is None:
            gir['country_name']=' '
        return gir['city'] + ',' + gir['region_name'] + ',' +  gir['country_name']
    else:
        return " , , "
    

geo_udf = udf(ip_to_geo,StringType())

sc = SparkContext(appName="BuildModelContent")  
sqlContext = SQLContext(sc)


channel_config = sqlContext.read.json('/tmp/config_channels.json')
channels_list = channel_config.select("channels").collect()
channels = [(row.channels) for row in channels_list]


lines = sc.textFile("/tmp/log2.txt")
rows = lines.filter(lambda line: 'newlog' not in line)

config = sqlContext.read.json('/tmp/config.json', multiLine=True)
lim = config.select('limit').collect()[0]['limit']


rows = rows.filter(lambda row: any(c in row for c in channels))
rdd = rows.map(lambda x: x.split('|'))
#r_f = rdd.first()
df_log = rdd.toDF()     #filter(lambda row: len(row) == len(r_f)).toDF()
rdd.unpersist()
sqlContext.clearCache()
df_log.show()


parameter_config = sqlContext.read.json('/tmp/config_parameters.json')
parameter_config.show()
column_index_list = parameter_config.select("Index").collect()
column_name_list = parameter_config.select("Name").collect()
column_index =  [col(row.Index) for row in column_index_list]
column_name =  [(row.Name) for row in column_name_list]
print column_index
print column_name

df_log_reduced = df_log.select(column_index)
df_log.unpersist()
sqlContext.clearCache()

oldColumns = df_log_reduced.schema.names
newColumns = column_name
df_log_reduced = reduce(lambda df_log_reduced, idx: df_log_reduced.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df_log_reduced)
df_log_reduced.show()


df_log_reduced = df_log_reduced.filter(df_log_reduced.timestamp.isNotNull())
df_log_reduced.show()
print df_log_reduced.count()

df_log_reduced = df_log_reduced.withColumn("timestamp",trim(col("timestamp")))
df_log_reduced = df_log_reduced.filter(df_log_reduced.timestamp.isNotNull())
df_log_reduced = df_log_reduced.withColumn("timestamp", df_log_reduced["timestamp"].cast(LongType()))
df_log_reduced.show()
print df_log_reduced.count()


db_config = sqlContext.read.format('csv').options(delimiter=' ').load("/tmp/dbconfig.txt")
db_config.show()
hostname = db_config.where(db_config._c0 == "hostname").select("_c1").rdd.flatMap(list).first()
username = db_config.where(db_config._c0 == "username").select("_c1").rdd.flatMap(list).first()
passwd = db_config.where(db_config._c0 == "passwd").select("_c1").rdd.flatMap(list).first()
dbname = db_config.where(db_config._c0 == "dbname").select("_c1").rdd.flatMap(list).first()


time_threshold = sqlContext.read.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='threshold',user=username,password=passwd).load()
time_threshold = time_threshold.withColumn("threshold", time_threshold["threshold"].cast(LongType()))
threshold = time_threshold.where(time_threshold.job == "BuildModelContent").select("threshold").rdd.flatMap(list).first()
print threshold
print lim
df_log_reduced = df_log_reduced[df_log_reduced.timestamp > threshold].limit(lim)
df_log_reduced.show()
print df_log_reduced.count()


status = "Regular job is continued"
if df_log_reduced.count() < lim:
    status = "Regular job is finshed"

if df_log_reduced.count() == 0:
    f = open('/home/hadoop/status.txt','w')
    f.write("Regular job is finshed")
    f.close()
    exit()


threshold = df_log_reduced.agg({"timestamp": "max"}).collect()[0]["max(timestamp)"]
print threshold
df_log_reduced = df_log_reduced.drop("timestamp")
time_threshold1 = time_threshold.toDF("job","threshold")
time_threshold.unpersist()
time_threshold1.show()
time_threshold1 = time_threshold1.withColumn("threshold", F.when(time_threshold1.job == "BuildModelContent",threshold).otherwise(F.lit(0)))
time_threshold1.show()
print time_threshold1.count()
time_threshold1.write.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='threshold1',user=username,password=passwd).mode('overwrite').save()


split_col = pyspark.sql.functions.split(df_log_reduced['Identifier'], ':')
df_log_reduced = df_log_reduced.withColumn('Identifier', split_col.getItem(3))
df_log_reduced.show()

df_log_reduced = df_log_reduced.withColumn("Node",trim(col("Node")))
click_data = df_log_reduced.select("Node","Identifier","NextNode")
click_data = click_data[ click_data.Node == ""]
click_data = click_data.drop("Node")
click_data = click_data.toDF("Identifier","Node")
#click_data.show(click_data.count(),False)
click_data.write.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='click_info',user=username,password=passwd).mode('append').save()


node_popular = df_log_reduced.select("NextNode","watch_time")
node_popular = node_popular.toDF("Node","watch_time")
node_popular = node_popular.groupby("Node").agg(F.sum("watch_time"))
node_popular.show()
node_popular.write.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='node_popular',user=username,password=passwd).mode('overwrite').save()
node_view = df_log_reduced.select("NextNode","Identifier")
node_view = node_view.toDF("Node","Identifier")
node_view = node_view.withColumn("Identifier", F.lit(1))
node_view = node_view.groupby("Node").agg(F.sum("Identifier"))
node_view.write.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='node_view',user=username,password=passwd).mode('overwrite').save()


df_log_reduced = df_log_reduced.withColumn("Node",trim(col("Node")))
df_log_reduced = df_log_reduced.withColumn("NextNode",trim(col("NextNode")))
df_log_reduced1 = df_log_reduced.select('Identifier','NextNode','Node')
df_log_reduced1 = df_log_reduced1.toDF('Identifier1','Node1', 'prevNode')
df_log_reduced = df_log_reduced.join(df_log_reduced1,((df_log_reduced['Node'] == df_log_reduced1['Node1']) & (df_log_reduced['Identifier'] == df_log_reduced1['Identifier1']) &(df_log_reduced['watch_time'] != '')),'inner')
df_log_reduced1.unpersist()
sqlContext.clearCache()
df_log_reduced = df_log_reduced.drop('Node1', 'NextNode','Identifier1')
sqlContext.clearCache()
df_log_reduced.show()


df_log = df_log_reduced.withColumn("score", (F.col("watch_time") / F.col("Duration")))
df_log_reduced.unpersist()
sqlContext.clearCache()

threshold_config = sqlContext.read.json('/tmp/config_threshold.json')
threshold_config.show()
for row in threshold_config.collect():
    df_log = df_log.withColumn("is_watched", F.when((df_log.channel_id == row.channel) & (df_log.score > row.threshold) ,F.lit(str(1))).otherwise(F.lit(str(0))))


df_log = df_log.drop('watch_time')
df_log = df_log.drop("Duration")
df_log = df_log.drop("score")

df_log = df_log.withColumn("ip",geo_udf(df_log["ip"]))
split_col = pyspark.sql.functions.split(df_log['ip'], ',')
df_log = df_log.withColumn('city', split_col.getItem(0))
df_log = df_log.withColumn('region', split_col.getItem(1))
df_log = df_log.withColumn('country', split_col.getItem(2))
df_log = df_log.drop('ip')

df_log.show()

sqlContext.clearCache()

for co in df_log.columns:
    if str(co) != 'is_watched' and str(co) != 'Node':
        feature_prob = df_log.groupby(co).count()
        feature_prob = feature_prob.withColumn('featureCount', col('count'))
        feature_prob.show()
        cond_prob = df_log.groupby(['is_watched', str(co),'Node']).count()   
        cond_prob = cond_prob.withColumn('jointCount', col('count'))
        cond_prob_joined = cond_prob.join(feature_prob,str(co))
        cond_prob.unpersist()
        feature_prob.unpersist()
        sqlContext.clearCache()
        cond_prob = cond_prob_joined[cond_prob_joined.is_watched != str(0)]
        cond_prob_joined.unpersist()
        sqlContext.clearCache()
        cond_prob = cond_prob.drop('is_watched')
        cond_prob = cond_prob.withColumn('featurevalue', col(str(co)))
        cond_prob = cond_prob.drop(str(co))
        cond_prob = cond_prob.drop('count')
        cond_prob = cond_prob.withColumn('featurename', lit(str(co)))
        cond_prob.show()
        cond_prob.write.format('jdbc').options(url="jdbc:mysql://"+ hostname + '/' + dbname,driver='com.mysql.jdbc.Driver',dbtable='score_rec',user=username,password=passwd).mode('append').save()
        cond_prob.unpersist()
        sqlContext.clearCache()

df_log.unpersist()
sqlContext.clearCache()
sc.stop()
f = open('/home/hadoop/status.txt','w')
f.write(status)
f.close()
