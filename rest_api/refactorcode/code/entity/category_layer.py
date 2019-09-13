class CategoryLayer(object):

    def get_category(self, feature_value, feature_name, rules, default_category, reverse_category_map, scores,prevNode):
        feature_name = feature_name.encode('ascii','ignore').strip()
        if feature_name == "prevNode":
            feature_value = prevNode
        if feature_value is None:
            return default_category
        feature_value = feature_value.encode('ascii','ignore').strip()
        res = {}
        found = False
        if feature_name in scores:
            temp = scores[feature_name]
            if feature_value in temp:
                res = temp[feature_value]
                found = True
            else:
                res = default_category
        else:
            res = default_category

        for rule in rules:
            if rule["origin"] == feature_name:
                if found:
                    for c in res:
                        res[c] = res[c] + rule["weight"]
                if feature_name == "prevNode":
                    if feature_value in  reverse_category_map:
                        categories = reverse_category_map[feature_value]
                        for c in categories:
                            if str(c) not in res:
                                res[str(c)] = rule["weight"]
                            else:
                                res[str(c)] = res[str(c)] + rule["weight"]
        return res

    def get_refined_category(self, feature_value, feature_name, rules, default_category, reverse_category_map, valid_category, no_of_category, scores, prevNode, debug):
        res = self.get_category(feature_value, feature_name, rules, default_category, reverse_category_map, scores, prevNode)
        res_final = {}
        for c in res:
            if str(c) in valid_category:
                if len(res_final) == no_of_category:
                    return res_final
                res_final[c] = res[c]
        if not res_final:
            res_final = default_category

        if len(res_final) < no_of_category:
            for c in default_category:
                if c not in res_final:
                    res_final[c] = 0
        if debug:
            print "Category returned for ", feature_name
            print res_final
        return res_final


