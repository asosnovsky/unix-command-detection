import re
import pandas as pd

CUSTOM_PATH = re.compile(r'\<.\>')
SAVE_FOLDER = "./data/02_feathers"

def contains_path(s: str) -> bool:
	if len(s.split('/'))>1:
		return True
	elif bool(CUSTOM_PATH.findall(s)):
		return True
	else:
		return False

data = pd.read_feather(f"{SAVE_FOLDER}/00_dataset.feather")
data['contains_path'] = data.line.apply(contains_path)



data.to_feather(f"{SAVE_FOLDER}/01_wt_features.feather")
