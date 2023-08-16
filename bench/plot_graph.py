#!/usr/bin/python3
import sys
import pandas as pd
from matplotlib import pyplot as plt

dir = "bench_run"
datastructs = [
    "rhm",
    "trhm",
    "grhm",
    "gzhm"
]

f = open("%s/test_params.txt" % (dir), "r")
lines = f.readlines()
key_bits = int(lines[0])
quotient_bits = int(lines[1])
value_bits = int(lines[2])
load_factor = int(lines[3])
churn_cycles = int(lines[4])
churn_ops = int(lines[5]) 

churn_points = []
for l in lines[6:]: 
    churn_points.append(float(l))

plt.figure(figsize=(20,6))
for d in datastructs:
    df = pd.read_csv('./%s/%s-load.txt' % (dir, d), delim_whitespace=True)
    plt.plot(df["x_0"], df["y_0"], label=d, marker='.')
    plt.xlabel("percent of keys inserted" )
    plt.ylabel("throughput")
plt.legend()
plt.title("LOAD PHASE: q_bits=%s, r_bits=%s, Load Factor=%s" 
    % (quotient_bits, key_bits - quotient_bits + value_bits, load_factor))
plt.savefig("plot_insert.png")
plt.close()

plt.figure(figsize=(20,12))
for d in datastructs:
    df = pd.read_csv('./%s/%s-churn.txt' % (dir, d), delim_whitespace=True)
    plt.plot(df["x_0"], df["y_0"], label=d)
    plt.xlabel("percent of churn test" )
    plt.ylabel("throughput")

plt.legend()
plt.title("CHURN PHASE: q_bits=%s, r_bits=%s ChurnOps: %s ChurnCycles: %s" 
    % (quotient_bits, key_bits - quotient_bits + value_bits, 0, 0))
plt.savefig("plot_churn.png")
plt.close()

plt.figure(figsize=(20,12))
for d in datastructs:
    df = pd.read_csv('./%s/%s-churn.txt' % (dir, d), delim_whitespace=True)
    df = df[df["x_0"].ge(95.0)]
    plt.plot(df["x_0"], df["y_0"], label=d, marker='.')
    plt.xlabel("percent of churn test" )
    plt.ylabel("throughput")

for churn_point in churn_points:
    if churn_point > 95.0:
        plt.vlines(churn_point, 0, df["y_0"].max())

plt.legend()
plt.title("CHURN PHASE: q_bits=%s, r_bits=%s #ChurnCycles: %s #ChurnoOp: %s" 
    % (quotient_bits, key_bits - quotient_bits + value_bits, churn_cycles, churn_ops))
plt.savefig("plot_churn-95.png")
plt.close()

