import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

test_case_txt = "test_case.txt"
t = 10000  # timestamp to plot

with open(test_case_txt, "r") as f:
    key_size, quotiont_size, value_size = [int(s) for s in f.readline().split()]
    data_size = int(f.readline())

data = pd.read_csv(test_case_txt, skiprows=2, sep=" ", header=None, dtype=np.int32)
data.rename(columns={0: "operation", 1: "key", 2: "value"}, inplace=True)

s = set()
for i in range(t):
    op, key, value = data.iloc[i]
    if op == 0:
        s.add(key)
    elif op == 1:
        s.discard(key)

quotiont = [key >> (key_size - quotiont_size) for key in s]
plt.hist(quotiont, bins=16)
plt.savefig("hist.png")