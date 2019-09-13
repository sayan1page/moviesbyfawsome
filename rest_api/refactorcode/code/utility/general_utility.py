import pandas as pd

class GeneralUtility(object):

    def get_valid_categories(self, filepath):
        categories = pd.read_csv(filepath, names=['Category','id'])
        return categories.id.tolist()

    def load_default_category(self, categories):
        default_category = {}
        for c in categories:
            default_category[c] = 0
        return default_category

