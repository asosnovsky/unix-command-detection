import pandas as pd
import os
from tqdm import tqdm
from typing import List, Dict, Any

START = "**SOF**"
END = "**EOF**"
INPUT_FOLDER= "./data/01_txt"
SAVE_FOLDER = "./data/02_bin"

data: List[Dict[str, Any]] = []
file_names = os.listdir(INPUT_FOLDER)

with tqdm(total=len(file_names)) as waiter:
	for fname in file_names:
		waiter.set_description(f"reading file {fname}")
		with open(f"{INPUT_FOLDER}/{fname}") as f:
			curr_line: List[Dict[str, Any]]  = []
			user = os.path.splitext(fname)[0]
			sess_id = 0
			sess_prefix = user+'->'

			if user in ["USER1","USER0"]:
				user = "USER1+0"

			for line_num, token in enumerate(f):
				token = token.strip()
				if token == START:
					if len(curr_line) > 1:
						raise Exception("curr_line not null")
				elif token == END:
					data = [*data, *curr_line]
					curr_line = []
					sess_id += 1
				else:
					curr_line.append({
						"user": user,
						"sess_id": sess_prefix + str(sess_id),
						"token": token,	
						"is_multi_line": len(token.split(' ')) > 1,
						"line_num": line_num,
					})

		waiter.update()

# Convert to data frame
data = pd.DataFrame(data)

data.to_pickle(f"{SAVE_FOLDER}/00_dataset.bin")