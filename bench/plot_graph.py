#!/usr/bin/python3
import sys
import pandas as pd
import os
from matplotlib import pyplot as plt

dir = "bench_run"
if len(sys.argv) > 1:
    dir = sys.argv[1]
variants = next(os.walk(dir))[1]

f = open("%s/%s/test_params.txt" % (dir, variants[0]), "r")

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

def plot_churn_op_throuput(op):
    plt.figure(figsize=(20,12))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[ (df["op"]==op) ]
        plt.plot(df["x_0"], df["y_0"], label=d)
        plt.xlabel("churn cycle" )
        plt.ylabel("throughput")
    plt.legend()
    plt.title("CHURN PHASE (%s): q_bits=%s, r_bits=%s ChurnOps: %s ChurnCycles: %s" 
    % (op ,quotient_bits, key_bits - quotient_bits + value_bits, churn_ops, churn_cycles))
    plt.savefig(os.path.join(dir, "plot_churn_%s.png" % op))
    plt.close()

def plot_latency_boxplots(op):
    data = []
    labels = []
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[(df["op"]==op)]
        data.append(df["latency"])
        labels.append(d)
    plt.figure(figsize=(20,12))
    plt.yscale('log')
    plt.boxplot(data, labels=labels)
    plt.savefig(os.path.join(dir, "plot_churn_latency_%s.png" % op))
    plt.close()

plt.figure(figsize=(20,6))
for d in variants:
    df = pd.read_csv('./%s/%s/load.txt' % (dir, d), delim_whitespace=True)
    plt.plot(df["x_0"], df["y_0"], label=d, marker='.')
    plt.xlabel("percent of keys inserted" )
    plt.ylabel("throughput")
plt.legend()
plt.title("LOAD PHASE: q_bits=%s, r_bits=%s, Load Factor=%s" 
    % (quotient_bits, key_bits - quotient_bits + value_bits, load_factor))
plt.savefig(os.path.join(dir, "plot_insert.png"))
plt.close()

plot_churn_op_throuput("DELETE")
plot_churn_op_throuput("INSERT")
plot_churn_op_throuput("LOOKUP")

plot_latency_boxplots("DELETE")
plot_latency_boxplots("INSERT")
plot_latency_boxplots("LOOKUP")