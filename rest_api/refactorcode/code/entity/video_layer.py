class VideoLayer(object):

    def is_valid(self, categories1, v, reverse_category_map, valid_category, no_of_match):
        if str(v) not in reverse_category_map:
            return False
        if no_of_match == 0:
            return True
        video_categories =  reverse_category_map[str(v)]
        video_categories1 = []

        for c in video_categories:
            if str(c) in valid_category:
                video_categories1.append(c)

        if not categories1:
            return True
        if len(categories1) > 3:
            if len(list(set(categories1) & set(video_categories1))) >= no_of_match:
                return True
            else:
                return False
        else:
            if len(list(set(categories1) & set(video_categories1))) > 0:
                return True
            else:
                return False


    def get_video(self, valid_videos, c2, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, no_of_match, res_video,category_map, debug):
        if res_video is None:
            res_video = {}
        c1 = int(c2)
        count = 0
        categories = []
        if prevNode in  reverse_category_map:
                for c in reverse_category_map[prevNode]:
                    if str(c) in valid_category:
                        categories.append(c)
        popular_video = video_popular.keys()
        popular_valid_common = []
        if c1 in category_map:
            videos = category_map[c1]
            popular_valid_common = list(set(popular_video) & set(videos) & set(valid_videos))
            if not popular_valid_common:
                popular_valid_common = list(set(popular_video) & set(valid_videos))
        else:
            popular_valid_common = list(set(popular_video) & set(valid_videos))

        for v,p in sorted(video_popular.iteritems(), reverse=True, key=lambda (k,v): (v,k)):
            if len(res_video.keys()) == no_of_node:
                break
            if (str(v) == prevNode) or (v not in popular_valid_common):
                continue
            if str(v) in reverse_category_map:
                blocked = False
                for b in block_categories:
                    if b in reverse_category_map[str(v)]:
                        blocked = True
                        break
                if blocked:
                    continue
                if categories:
                    if self.is_valid(categories,v, reverse_category_map, valid_category, no_of_match):
                        if v in res_video:
                            res_video[v] = res_video[v] + video_popular[v] + no_of_match*no_of_match
                        else:
                            res_video[v] = video_popular[v] + no_of_match*no_of_match
                else:
                    if v in res_video:
                        res_video[v] = res_video[v] + video_popular[v]
                    else:
                        res_video[v] = video_popular[v]

                for rule in conditional_block_categories['rule']:
                    if not rule["then"]["blockOrallow"]:
                        if rule["if"]["relation"] == "or":
                            allow = False
                            checked = False
                        else:
                            allow = True
                            checked = False
                        for c in rule["if"]["categories"]:
                            if prevNode in reverse_category_map:
                                if rule["if"]["relation"] == "or":
                                    if c in reverse_category_map[prevNode]:
                                        allow = True
                                        checked = True
                                else:
                                    if c not in reverse_category_map[prevNode]:
                                        allow = False
                                        checked = True
                        if allow and checked:
                            for c in rule["then"]["categories"]:
                                if str(v) in reverse_category_map:
                                    if rule["then"]["relation"] == "or":
                                        if c in reverse_category_map[str(v)]:
                                            allow = True
                                            break
                                        else:
                                            allow = False
                                    else:
                                        if c not in reverse_category_map[str(v)]:
                                            allow = False
                                            break
                                else:
                                    allow = False
                        if checked:
                            if allow:
                                continue
                            else:
                                if v in res_video:
                                    del res_video[v]
                    else:
                        if rule["if"]["relation"] == "or":
                            block = False
                            checked = False
                        else:
                            block = True
                            checked = False
                        for c in rule["if"]["categories"]:
                            if prevNode in reverse_category_map:
                                if rule["if"]["relation"] == "or":
                                    if c in reverse_category_map[prevNode]:
                                        block = True
                                        checked = True
                                else:
                                    if c not in reverse_category_map[prevNode]:
                                        block = False
                                        checked = True
                        if block and checked:
                            for c in rule["then"]["categories"]:
                                if str(v) in reverse_category_map:
                                    if rule["then"]["relation"] == "or":
                                        if c in reverse_category_map[str(v)]:
                                            block = True
                                            break
                                        else:
                                            block = False
                                    else:
                                        if c not in reverse_category_map[str(v)]:
                                            block = False
                                            break
                                else:
                                    block = True
                        if checked:
                            if not block:
                                continue
                            else:
                                if v in res_video:
                                    del res_video[v]
        res_video_final = {}
        for v in res_video:
            res_video_final[str(v).strip()] = res_video[v]
        if debug:
            print "Category ", c2
            print "Video returned"
            print res_video_final
        return res_video_final

    def get_video_wrapper(self, valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, no_of_match, category_map, que_info, favorite_score, debug):
        res_video = self.get_video(valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, no_of_match, None,category_map, debug)
        if res_video is None:
            res_video = self.get_video(valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, 1, res_video, category_map, debug)
        else:
            if len(res_video.keys()) < no_of_node:
                no_of_node = no_of_node - len(res_video.keys())
                res_video = self.get_video(valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, 1, res_video, category_map, debug)
        if res_video is None:
            res_video = self.get_video(valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, 0, res_video, category_map, debug)
        else:
            if len(res_video.keys()) < no_of_node:
                no_of_node = no_of_node - len(res_video.keys())
                res_video = self.get_video(valid_videos, c, res, prevNode, reverse_category_map, valid_category, block_categories, video_popular, conditional_block_categories, no_of_node, 0, res_video, category_map, debug)

        for v in res_video:
            if v in que_info:
                res_video[v] = res_video[v] + favorite_score

        return res_video

