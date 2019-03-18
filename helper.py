from typing import List
from tqdm import tqdm

def segment_and_pad(obs: List[int], size: int = 2, pad_pre = True) -> List[List[int]]:
    if len(obs) < size:
        if pad_pre:
            return [obs + [0]*(size-len(obs))]
        else:
            return [[0]*(size-len(obs)) + obs]
    return [
        obs[(i-size):i-1]+[txt]
        for i, txt in enumerate(obs[size-1:], start=size)
    ]

def segment_and_pad_all(seq: List[List[int]], size: int = 2, pad_pre = True) -> List[List[int]]:
    ret:List[List[int]] = []
    for s in tqdm(seq, desc='Segmenting and padding...'):
        ret = [*ret, *segment_and_pad(s, size, pad_pre)]
    return ret

