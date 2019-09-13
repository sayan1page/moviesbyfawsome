from mysql import MysqlDbWrapper

class MysqlUtility(object):
    mysql_instance = None

    def __init__(self, configpath):
        self.mysql_instance = MysqlDbWrapper(configpath)

    def load_score(self):
        scores = {}
        data = self.mysql_instance.get_data("select * from category_score")
        for row in data:
            feature_name= row[1].strip()
            provider = row[0].strip()
            feature_value = row[2].strip()
            score = row[4]/row[3]
            #print feature_name, feature_value, provider, score
            if feature_name not in scores:
                temp ={}
                temp[provider] = score
                temp1 = {}
                temp1[feature_value] = temp
                scores[feature_name] = temp1
            else:
                temp1 = scores[feature_name]
                if feature_value not in temp1:
                    temp ={}
                    temp[provider] = score
                    temp1[feature_value] = temp
                    scores[feature_name] = temp1
                else:
                    temp = temp1[feature_value]
                    temp[provider] = score
                    temp1[feature_value] = temp
                    scores[feature_name] = temp1
        return scores

    def load_popular_node(self):
        video_popular = {}
        data = self.mysql_instance.get_data ("select * from node_popular_final")
        total = 0
        for row in data:
            video_popular[row[0].strip()] = row[1]
            total = total + row[1]
        for k in video_popular:
            video_popular[k] = video_popular[k] / total
        return video_popular

    def load_context_map(self):
        site_context_map = {}
        data = self.mysql_instance.get_data("select iptv_site_id, iptv_search_type, iptv_search_id from ifood_iptv_sites")
        for row in data:
            channel = row[0]
            context_id = row[2]
            context_type = row[1]
            if (context_id is not None) and (context_type is not None):
                site_context_map[channel] = [context_type, context_id.split(',')]
        return site_context_map
    
    def load_click_info(self):
        click_info = {}
        data = self.mysql_instance.get_data("select * from click_info")
        for row in data:
            node = row[0].replace(" ", "")
            device = row[1].replace(" ", "")
            if device in click_info:
                if node not in click_info[device]:
                    click_info[device].append(node)
            else:
                temp = [node]
                click_info[device] = temp
        return click_info

    def load_que_info(self):
        que_info = {}
        data = self.mysql_instance.get_data("select deviceid, nid from iptv_queues")
        for row in data:
            node = str(row[1])
            device = str(row[0])
            if device in que_info:
                if node not in que_info[device]:
                    que_info[device].append(node)
            else:
                temp = [node]
                que_info[device] = temp
        return que_info

    def load_favourite(self):
        fav_info = {}
        data = self.mysql_instance.get_data("select uid, nid from favorite_nodes")
        for row in data:
            node = str(row[1])
            uid = str(row[0])
            if uid in fav_info:
                if node not in fav_info[uid]:
                    fav_info[uid].append(node)
            else:
                temp = [node]
                fav_info[uid] = temp
        return fav_info



    def shutdown(self):
        self.mysql_instance.close_mysql()


