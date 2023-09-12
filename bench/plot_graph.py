#!/usr/bin/python3
import sys
import pandas as pd
import os
from matplotlib import pyplot as plt
from scipy.stats import hmean

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

def add_caption():
    plt.text(.5, -0.15, f"q_bits={quotient_bits}, r_bits={key_bits - quotient_bits + value_bits}, ChurnOps: {churn_ops}, ChurnCycles: {churn_cycles}", ha='center', transform=plt.gca().transAxes)

def plot_churn_overall_throughput():
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), delim_whitespace=True)
        # Filter out LOOKUP
        df = df[df['op'] != 'LOOKUP']
        df = df.groupby("churn_cycle").agg({"y_0": hmean})
        plt.plot(df.index, df["y_0"], label=d)
        plt.xlabel("churn percentage progress" )
        plt.ylabel("throughput")
    plt.legend()
    plt.title(f"CHURN PHASE (Insert + Delete) Overall Throughput")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_throughput.png"))
    plt.close()


def plot_churn_op_throuput(op):
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[ (df["op"]==op) ]
        plt.plot(df["x_0"], df["y_0"], label=d)
        plt.xlabel("churn percentage progress" )
        plt.ylabel("throughput")
    plt.legend()
    plt.title(f"CHURN PHASE {op} Throughput")
    add_caption()
    plt.tight_layout()
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
    fig = plt.figure(figsize=(10,6))
    plt.yscale('log')
    plt.boxplot(data, labels=labels)
    plt.title(f"{op} Latencies")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_latency_%s.png" % op))
    plt.close()

def plot_distribution(metric):
    num_variants = len(variants)
    fig = plt.figure(figsize=(10,6))
    total_bar_width = 0.8
    bar_idx = 0
    for d in variants:
        metric_file = './%s/%s/%s.txt' % (dir, d, metric)
        if (not os.path.isfile(metric_file)):
            continue
        df = pd.read_csv(metric_file, delim_whitespace=True)
        columns = df.columns
        plt.bar(df[columns[0]] - (bar_idx * total_bar_width/num_variants), df[columns[1]], label=d, width=total_bar_width/num_variants) 
        bar_idx = bar_idx + 1 
        plt.title("%s Distribution" % (columns[0]))
    plt.yscale('log')
    plt.legend()
    plt.savefig(os.path.join(dir, "plot_%s.png" % metric))
    plt.close()

plt.figure(figsize=(20,6))
for d in variants:
    df = pd.read_csv('./%s/%s/load.txt' % (dir, d), delim_whitespace=True)
    plt.plot(df["x_0"], df["y_0"], label=d, marker='.')
    plt.xlabel("percent of keys inserted" )
    plt.ylabel("throughput")
plt.legend()
plt.title("LOAD PHASE")
add_caption()
plt.tight_layout()
plt.savefig(os.path.join(dir, "plot_insert.png"))
plt.close()

plot_churn_op_throuput("DELETE")
plot_churn_op_throuput("INSERT")
plot_churn_op_throuput("LOOKUP")
plot_churn_overall_throughput()

plot_latency_boxplots("DELETE")
plot_latency_boxplots("INSERT")
plot_latency_boxplots("LOOKUP")

plot_distribution('home_slot_dist')
plot_distribution('tombstone_dist')
