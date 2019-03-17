import re
import pandas as pd

from tqdm import tqdm

ARGS = re.compile(r'\<.\>')
OPTS = re.compile(r'-.')
PIPE = (";","|","&","&&","||")

def token_type(s: str) -> str:
    if bool(ARGS.findall(s)):
        return 'args'
    elif bool(OPTS.findall(s)):
        return 'opts'
    elif s in PIPE:
        return 'pipe'
    else:
        return 'cmd'

def parse_token_sequence(df: pd.DataFrame) -> pd.DataFrame:
    token_sequence = df.token_type.tolist()
    line = 0
    line_ids = [line]

    for i in tqdm(range(1, len(token_sequence)), desc=f"Building Token Sequence user={df.user.unique()}"):
        if token_sequence[i] == 'cmd' and token_sequence[i-1] != 'pipe':
            line += 1
            line_ids.append(line)
        else:
            line_ids.append(line)
    df.line_id = line_ids
    return df



data = pd.read_pickle("./data/02_bin/00_dataset.bin")
data = data.assign(
    line_id = 0,
    token_type = lambda x: x.token.apply(token_type)
)

data = data.groupby(['user', 'sess_id']).apply(parse_token_sequence)

user_data = data.groupby(['user', 'sess_id', 'line_id']).apply(
    lambda g: " ".join(g.token)
)

user_data.to_pickle("./data/02_bin/01_lines.bin")






