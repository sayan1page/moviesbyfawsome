from __future__ import division
import falcon
from falcon_cors import CORS
import json
from concurrent.futures import *
import collections
import random
from collections import OrderedDict
import sys
import yaml
import traceback

sys.path.append('./utility')
sys.path.append('./entity')
sys.path.append('./core')
from mysql_utility import MysqlUtility
from mongo_utility import MongoUtility
from general_utility import GeneralUtility
from video_layer import VideoLayer
from category_layer import CategoryLayer

class Predictor(object):
    scores = {}
    category_map = {}
    uid_map = {}
    site_context_map = {}
    reverse_category_map = {}
    video_popular = {}
    click_info = {}
    que_info = {}
    favourite = {}
    valid_category =[]
    valid_all_geo = []
    valid_us = []
    valid_uk = []
    valid_ca = []
    valid_fr = []
    valid_ge = []
    valid_it = []
    valid_me = []
    valid_au = []
    config = {}
    debug = True
    def __init__(self, debug):
        with open('../config/config.json') as f:
            self.config = yaml.safe_load(f)
        f.close()
        mysql_utility = MysqlUtility('../config/dbconfig.txt')
        self.scores = mysql_utility.load_score()
        self.video_popular = mysql_utility.load_popular_node()
        self.site_context_map = mysql_utility.load_context_map()
        self.click_info = mysql_utility.load_click_info()
        self.que_info = mysql_utility.load_que_info()
        self.favourite = mysql_utility.load_favourite()
        mysql_utility.shutdown()
        
        mongo_utility = MongoUtility('../config/mongoconfig.txt')
        self.valid_all_geo, self.valid_us, self.valid_uk, self.valid_ca, self.valid_fr, self.valid_ge, self.valid_it, self.valid_au, self.valid_me = mongo_utility.load_geo_restrictons()
        self.category_map, self.uid_map, self.reverse_category_map = mongo_utility.load_mapping()
        mongo_utility.shutdown()
        
        self.valid_category = GeneralUtility().get_valid_categories('../data/categories.csv')
        self.debug = debug
           
    
    def on_get(self, req, resp):
        mysql_utility = MysqlUtility('../config/dbconfig.txt')
        self.scores = mysql_utility.load_score()
        self.video_popular = mysql_utility.load_popular_node()
        self.click_info = mysql_utility.load_click_info()
        self.que_info = mysql_utility.load_que_info()
        self.favourite = mysql_utility.load_favourite()
        mysql_utility.shutdown()
    

    def on_post(self, req, resp):
        try:        
            res_final = {}
            input_json = json.loads(req.stream.read(),encoding='utf-8')
            local_config = {}
            chid = int(input_json['channel_id'].encode('ascii','ignore'))
            if str(chid) in self.config.keys():
                local_config = self.config[str(chid)]
            else:
                local_config = self.config["all"]
            
            default_category = GeneralUtility().load_default_category(local_config["default_category"])
            if 'prevNode' not in input_json:
                input_json['prevNode'] = ["000"]

            if 'prevNode' in input_json:
                nodes = input_json['prevNode']
                nodes_filtered = []
               
                device_id = input_json['Identifier']
                local_que = []
                if device_id in self.que_info:
                    local_que = self.que_info[device_id]
                if 'uid' in input_json:
                    uid = input_json['uid']
                    if uid in self.favourite:
                        local_que = local_que + self.favourite[uid]

                
                for n in nodes:
                    if device_id in self.click_info:
                        clicked_nodes = map(int,self.click_info[device_id])
                        if self.debug:
                            print "device found", device_id
                            print clicked_nodes
                            print n
                        if n in clicked_nodes:
                            if self.debug:
                                print "node found"
                            nodes_filtered.append(n)
                #random.shuffle(nodes_filtered)
                if self.debug:
                    print "Filtered nodes"
                    print nodes_filtered
                if not nodes_filtered:
                    nodes_filtered = ["000","000"]
                if len(nodes_filtered) == 1:
                    nodes_filtered.append(nodes_filtered[0])

                for i in range(1,-1,-1):               
                    prevNode = str(nodes_filtered[i])  
                    valid_videos = []
                    if chid in self.site_context_map:
                        context_type, context_ids = self.site_context_map[chid]
                        if 'nodeid' in context_type:
                            if self.debug:
                                print "node type"
                                print context_type, context_ids
                            for c in context_ids:
                                if int(c) in self.category_map:
                                    valid_videos = valid_videos + self.category_map[int(c)]
                        else:
                            if self.debug:
                                print "user type"
                                print context_type, context_ids
                            for c in context_ids:
                                if int(c) in self.uid_map:
                                    valid_videos = valid_videos +  self.uid_map[int(c)]
                        if self.debug:
                            print valid_videos
                    valid_geo = self.valid_all_geo
                    if 'country' in input_json:
                        country = input_json['country']
                        if country == 'United States':
                            valid_geo = valid_geo + self.valid_us
                        if country == 'United Kingdom':
                            valid_geo = valid_geo + self.valid_uk
                        if country == 'Australia':
                            valid_geo = valid_geo + self.valid_au
                        if country == 'Italy':
                            valid_geo = valid_geo + self.valid_it
                        if country == 'Mexico':
                            valid_geo = valid_geo + self.valid_me
                        if country == 'Canada':
                            valid_geo = valid_geo + self.valid_ca
                        if country == 'France':
                            valid_geo = valid_geo + self.valid_fr
                        if country == 'Germany':
                            valid_geo = valid_geo + self.valid_ge
                    valid_videos = list(set(valid_videos) & set(valid_geo))
                    if self.debug:
                        print "after geo matching"
                        print valid_videos

                    res = {}                
                    with ThreadPoolExecutor(max_workers=8) as pool:
                        future_array = { pool.submit(CategoryLayer().get_refined_category,input_json[f],f,local_config["category_weightedge"]["rule"], default_category, self.reverse_category_map, self.valid_category, local_config["no_of_category"], self.scores, prevNode, self.debug) : f for f in input_json }
                        for future in as_completed(future_array):
                            temp = future.result()
                            if temp is not None:
                                for k in temp:
                                    if k not in res:
                                        res[k] = temp[k]
                                    else:
                                        res[k] = res[k] + temp[k]
                    pool.shutdown(wait=True)
                    if self.debug:
                        print res
            
                    res_video = {}

                    with ThreadPoolExecutor(max_workers=8) as pool:
                        future_array = { pool.submit(VideoLayer().get_video_wrapper, valid_videos, c, res, prevNode, self.reverse_category_map, self.valid_category, local_config["block_categories"], self.video_popular, local_config["conditional_block_categories"], local_config["no_of_node"], local_config["no_of_match"],self.category_map, local_que, local_config["favourite_weight"],self.debug) : c for c in res.keys() }
                        for future in as_completed(future_array):
                            temp = future.result()
                            if temp is not None:
                                for v in temp:
                                    if v in res_video:
                                        res_video[v] = res_video[v] + temp[v]
                                    else:
                                        res_video[v] = temp[v]
                    pool.shutdown(wait=True)
            
                    res = {}
                    for i in range(len(list(res_video.items()))):
                        if len(list(res.items())) == local_config["no_of_node"] -10:
                            break
                        v,s = random.choice(list(res_video.items()))
                        while v in res:
                            v,s = random.choice(list(res_video.items()))
                        res[v] = s

                    res_final[prevNode] = OrderedDict(sorted(res.iteritems(), reverse=True,key=lambda (k,v): (v,k)))
                    if len(res_final.keys()) >= 2:
                        break
            if self.debug:
                print input_json
                print res_final
            resp.status = falcon.HTTP_200
            resp.body = str(json.dumps(res_final)).encode('ascii','ignore').replace("'", '"').replace("\\","")
        except Exception,e:
            resp.status = falcon.HTTP_500
            resp.body = ""
            if self.debug:
                print input_json
                print e 
                print traceback.format_exc()
    
cors = CORS(allow_all_origins=True,allow_all_methods=True,allow_all_headers=True) 
wsgi_app = api = falcon.API(middleware=[cors.middleware])
debug = False
if "debug" in sys.argv:
    debug = True
p = Predictor(True)
url = '/predict/'
api.add_route(url, p)

        
            
