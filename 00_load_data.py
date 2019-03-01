import pandas as pd
import os
from tqdm import tqdm

START = "**SOF**"
END = "**EOF**"
INPUT_FOLDER= "./data/01_txt"
SAVE_FOLDER = "./data/02_feathers"

data = []
file_names = os.listdir(INPUT_FOLDER)

with tqdm(total=len(file_names)) as waiter:
	for fname in file_names:
		waiter.set_description(f"reading file {fname}")
		with open(f"{INPUT_FOLDER}/{fname}") as f:
			curr_line: [] = None
			user = os.path.splitext(fname)[0]
			sess_id = 0

			if user in ["USER1","USER0"]:
				user = "USER1+0"
			for line in f:
				line = line.strip()
				if line == START:
					if curr_line is None:
						curr_line = []
					else:
						raise Exception("curr_line not null")
				elif line == END:
					data = [*data, *curr_line]
					curr_line = None
				else:
					curr_line.append({
						"user": user,
						"sess_id": sess_id,
						"line": line,	
					})
					sess_id += 1

		waiter.update()

data = pd.DataFrame(data)
data.to_feather(f"{SAVE_FOLDER}/00_dataset.feather")