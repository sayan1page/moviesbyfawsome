from mongo import MongoDbWrapper
import collections
import time

class MongoUtility(object):
    mongo_instance = None
    res = None

    def __init__(self, configpath):
        self.mongo_instance = MongoDbWrapper(configpath)

    def load_data(self):
        self.res = self.mongo_instance.get_data()
        #print self.res

    def load_geo_restrictons(self):
        self.load_data()
        valid_all_geo = []
        valid_us = []
        valid_uk = []
        valid_ca = []
        valid_fr = []
        valid_ge = []
        valid_it = []
        valid_au = []
        valid_me = []
        for r in self.res:
            node = str(r['nid']).strip()
            #print node
            if 'c_all' in r:
                if r['c_all'] == "1":
                    valid_all_geo.append(node)
            if 'c_us' in r:
                if r['c_us'] == "1":
                    valid_us.append(node)
            if 'c_us' in r:
                if r['c_uk'] == "1":
                    valid_uk.append(node)
            if 'c_ca' in r:
                if r['c_ca'] == "1":
                    valid_ca.append(node)
            if 'c_fr' in r:
                if r['c_fr'] == "1":
                    valid_fr.append(node)
            if 'c_ge' in r:
                if r['c_ge'] == "1":
                    valid_ge.append(node)
            if 'c_it' in r:
                if r['c_it'] == "1":
                    valid_it.append(node)
            if 'c_au' in r:
                if r['c_au'] == "1":
                    valid_au.append(node)
            if 'c_me' in r:
                if r['c_me'] == "1":
                    valid_me.append(node)
        return valid_all_geo, valid_us, valid_uk, valid_ca, valid_fr, valid_ge, valid_it, valid_au, valid_me

    def load_mapping(self):
        self.load_data()
        category_map = {}
        uid_map = {}
        reverse_category_map = {}

        for r in self.res:
            node = str(r['nid']).strip()
            if "field_image" not in r:
                continue
            if not r["field_image"]:
                continue
            if "field_video" not in r:
                continue
            if not r["field_video"]:
                continue
            if "user_scope" in r:
                if r["user_scope"] != "":
                    continue
            if "publish_date" not in r:
                continue
            if r["publish_date"] >  int(time.time()):
                continue
            score = 0
            if "u_rating" in r and r["u_rating"] is not None:
                score = score +  int(r["u_rating"])
            if "v_rating" in r and r["v_rating"] is not None:
                score = score + int(r["v_rating"])
            if "a_rating" in r and r["a_rating"] is not None:
                score = score + int(r["a_rating"])
            if "s_rating" in r and r["s_rating"] is not None:
                score = score +  int(r["s_rating"])
            if "p_rating" in r and r["p_rating"] is not None:
                score = score + int(r["p_rating"])
            if score < 18: 
                continue
            if 'field_node_index' in r:
                if r['field_node_index'] is not None:
                    r1 = r['field_node_index']
                    for r2 in r1:
                        if  r2['target_id'] is not None:
                            categories = r2['target_id']
                            if isinstance(categories, collections.Iterable):
                                for c in categories:
                                    if c in category_map:
                                        if node not in  category_map[c]:
                                            category_map[c].append(node)
                                    else:
                                        temp = []
                                        temp.append(node)
                                        category_map[c] = temp
                            else:
                                c = categories
                                if c in category_map:
                                    if node not in category_map[c]:
                                        category_map[c].append(node)
                                else:
                                    temp = []
                                    temp.append(node)
                                    category_map[c] = temp
            if 'uid' in r:
                uids = r['uid']
                if True: #isinstance(uids, collections.Iterable):
                    #for u in uids:
                    #    if u in uid_map:
                    #        if node not in  uid_map[u]:
                     #           uid_map[u].append(node)
                      #  else:
                      #      temp = []
                       #     temp.append(node)
                       #     uid_map[u] = temp
                #else:
                    u = uids
                    if u in uid_map:
                        if node not in  uid_map[u]:
                            uid_map[u].append(node)
                    else:
                        temp = []
                        temp.append(node)
                        uid_map[u] = temp
        for c in category_map:
            videos = category_map[c]
            for v in videos:
                if v in reverse_category_map:
                    reverse_category_map[v].append(c)
                else:
                    temp = []
                    temp.append(c)
                    reverse_category_map[v] = temp
        return category_map, uid_map, reverse_category_map

    def shutdown(self):
        self.mongo_instance.close_mongo()

