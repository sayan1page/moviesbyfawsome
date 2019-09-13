import  MySQLdb

class MysqlDbWrapper(object):
    connection = None
    cursor = None

    def __init__(self, configpath):
        f = open(configpath)
        config = {}
        for line in f:
            fields = line.strip().split(' ')
            config[fields[0]] = fields[1].strip()
        f.close()
        self.connection = MySQLdb.connect (host = config['hostname'], user = config['username'], passwd = config['passwd'], db = config['dbname'])
        self.cursor = self.connection.cursor()

    def close_mysql(self):
        self.cursor.close()
        self.connection.close()

    def get_data(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

