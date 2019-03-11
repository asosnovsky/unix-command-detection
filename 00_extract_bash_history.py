#
# Soryy, I placed everything in one file for this...
# ======================
# Imports
#======================

import pandas as pd
import re
import os
import random, string

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

from enum import Enum
from typing import NamedTuple, List, Dict, Tuple

#======================
# Constants
#======================

# Set these variables
INPUT_FILE = "./data/00_raw/00_ari_bash_history.txt"
OUTPUT_FILE = "./data/01_parsed/01_ari.pickle"
HOME_PATH = "/home/ari"

# General Defaults
PIPE_CMDS = (";","|","&","&&","||")
CD = "cd"
PATH_CMDS = [
    # common ones
    CD, "cat", "mv", "rm", "cp", "mkdir", "tail",
    # special software
    "firefox", "git", "chrome", "source", "code", 
]

#======================
# Token Manipulation
#======================
class TokenType(Enum):
    Path='path'
    Pipe='pipe'
    PathCMD='path-command'
    Unknown='n/a'

class Token(NamedTuple):
    token_text: str
    token_type: TokenType
    row_idx: int = -1
    start_path: str = ''
    end_path: str = ''

    @classmethod
    def tokenize(cls, text: str):
        if len(text.split('/')) > 1:
            return cls(token_text=text, token_type=TokenType.Path)
        elif text in PIPE_CMDS:
            return cls(token_text=text, token_type=TokenType.Pipe)
        elif text in PATH_CMDS:
            return cls(token_text=text, token_type=TokenType.PathCMD)
        else:
            return cls(token_text=text, token_type=TokenType.Unknown)

    @classmethod
    def tokenizer(cls, row_idx:int, start_path: str, line: str):
        prior_is_path_cmd = False
        prior_is_cd = False
        for text in line.split(' '):
            text = text.strip()
            end_path = start_path
            token = cls.tokenize(text)
            
            if prior_is_path_cmd and token.token_type != TokenType.PathCMD:
                token = cls(token_text=token.token_text, token_type=TokenType.Path)
            
            # check if ends with pipe cmd
            endswith_pipe = False
            if token.token_type != TokenType.Pipe:
                for pcmd in PIPE_CMDS:
                    if token.token_text.endswith(pcmd):
                        endswith_pipe = True
                        break

            if token.token_type == TokenType.PathCMD:
                prior_is_path_cmd = True
                prior_is_cd = token.token_text == CD
            elif prior_is_cd:
                text = token.token_text
                if endswith_pipe:
                    text = text[0:(len(text)-len(pcmd))]
                if text.startswith("/"):
                    end_path = text
                elif text.startswith("~"):
                    end_path = os.path.join(HOME_PATH, text[1:])
                elif text.startswith('./'):
                    end_path = os.path.join(start_path, text[2:])
                else:
                    end_path = os.path.join(start_path, text)
                prior_is_path_cmd = False
                prior_is_cd = False

            if endswith_pipe:
                yield cls(
                    token_text=token.token_text[0:(len(token.token_text)-len(pcmd))],
                    token_type=token.token_type,
                    row_idx=row_idx,
                    start_path=start_path,
                    end_path=end_path
                )
                yield cls(
                    token_text=pcmd,
                    token_type=TokenType.Pipe,
                    row_idx=row_idx,
                    start_path=end_path,
                    end_path=end_path
                )
            else:
                yield cls(
                    token_text=token.token_text,
                    token_type=token.token_type,
                    row_idx=row_idx,
                    start_path=start_path,
                    end_path=end_path
                )
            start_path = end_path

#======================
# Read file, convert to tokens
#======================
data = []
with open(INPUT_FILE) as inf:
    curr_path = HOME_PATH
    for row_idx, row in enumerate(inf):
        for token in Token.tokenizer(row_idx, curr_path, row.strip()):
            data.append(token)
        curr_path = token.end_path
            
data = pd.DataFrame(data)

#======================
# Path obstructions
#======================
ObtuseMapping = Dict[int, Dict[str, List[str]]]

def path_obtuse(path: str, optuse_paths: List[str]) -> Tuple[str, list]:
    if path not in optuse_paths:
        optuse_paths.append(path)
    return str(optuse_paths.index(path)), optuse_paths

def path_obtuse_full(path: str, op_paths: ObtuseMapping) -> Tuple[ str,  ObtuseMapping ]:
    last_sub_path = '/'
    obt_path = ""
    for level, subpath in enumerate(path.split('/')[1:]):
        if level not in op_paths.keys():
            op_paths[level] = {}
        if last_sub_path not in op_paths[level].keys():
            op_paths[level][last_sub_path] = []
        op_sp = op_paths[level][last_sub_path]
        subpath, op_sp = path_obtuse(subpath, op_sp)
        last_sub_path = subpath
        obt_path += '/'
        obt_path += subpath

    return obt_path, op_paths

# Get all unique paths
all_paths = set(
    data.end_path[data.token_type == TokenType.Path].unique()
).union(
    data.start_path[data.token_type == TokenType.Path].unique()
)


# Obtuse paths
mapping: Dict[str, str] = {}
op_paths: ObtuseMapping = {}
for text in all_paths:
    otext, op_paths = path_obtuse_full(text, op_paths)
    mapping[text] = otext

def cmd_obtusal(cmd: str, cmd_obtusals: List[str] = []):
    text, cmd_obtusal = path_obtuse(cmd, cmd_obtusals)
    return f'//{text}'

def apply_obts(row: pd.Series) -> pd.DataFrame:
    row.end_path = mapping[row.end_path]
    row.start_path = mapping[row.start_path]
    if row.token_type == TokenType.Path:
        if row.token_text in mapping.keys():
            row.token_text = mapping[row.token_text]
        else:
            row.token_text = cmd_obtusal(row.token_text)
    return row

# apply obstruction to all paths
ob_data = data.apply(
    axis=1,
    func=apply_obts 
)

#======================
# Rebuild final dataset and save
#======================
ob_data.groupby('row_idx').apply(
    lambda g: " ".join(g.token_text.values)
).to_pickle(OUTPUT_FILE)