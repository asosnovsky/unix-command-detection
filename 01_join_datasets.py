import pandas as pd


ari_dataset = pd.read_pickle("./data/01_parsed/01_ari.pickle")

ari_dataset.head(20)