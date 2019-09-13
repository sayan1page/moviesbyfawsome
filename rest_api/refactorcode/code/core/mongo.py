from pymongo import MongoClient

class MongoDbWrapper(object):
    collection = None
    client = None
    db = None

    def __init__(self,configpath):
        f = open(configpath)
        mongoconfig = {}
        for line in f:
            fields = line.strip().split('=')
            mongoconfig[fields[0]] = fields[1].strip()
        f.close()
        self.client = MongoClient('mongodb://'+mongoconfig['usr']+':'+ mongoconfig['pswd']+'@'+mongoconfig['host']+'/'+mongoconfig['db'])
        self.db = self.client[mongoconfig['db']]
        self.collection = self.db[mongoconfig['collection']]

    def close_mongo(self):
        self.client.close()

    def get_data(self):
        return self.collection.find({"_type" : "node", "status":1})

