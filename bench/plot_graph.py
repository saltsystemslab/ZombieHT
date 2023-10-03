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

csv_dir = os.path.join('csv')
os.makedirs(csv_dir, exist_ok=True)
csv_dir = os.path.join('csv', dir)
os.makedirs(csv_dir, exist_ok=True)

f = open("%s/%s/test_params.txt" % (dir, variants[0]), "r")

lines = f.readlines()
memory_usage = int(lines[0])
key_bits = int(lines[1])
quotient_bits = int(lines[2])
value_bits = int(lines[3])
load_factor = int(lines[4])
churn_cycles = int(lines[5])
churn_ops = int(lines[6]) 

churn_points = []
for l in lines[6:]: 
    churn_points.append(float(l))

def add_caption():
    plt.text(.5, -0.15, f"q_bits={quotient_bits}, r_bits={key_bits - quotient_bits + value_bits}, ChurnOps: {churn_ops}, ChurnCycles: {churn_cycles}", ha='center', transform=plt.gca().transAxes)

def plot_tombstone():
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_metadata.txt' % (dir, d), delim_whitespace=True)
        plt.plot(df["churn_cycle"], df["tombstones"], label=d)
        plt.xlabel("churn cycle" )
        plt.ylabel("tombstone_count")
    plt.legend()
    plt.title(f"CHURN Tombstone count")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_tombstones.png"))
    plt.close()

def plot_tombstone_ratio():
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_metadata.txt' % (dir, d), delim_whitespace=True)
        plt.plot(df["churn_cycle"], df["tombstones"]/(df["tombstones"] + df["occupied"]), label=d)
        plt.xlabel("churn cycle" )
        plt.ylabel("tombstone_count")
    plt.legend()
    plt.title(f"CHURN Tombstone to Occupied Ratio")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_tombstones_ratio.png"))
    plt.close()

def plot_churn_op_throuput_ts(name, ops):
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[ (df["op"].isin(ops)) ]
        if (len(df)==0):
            return
        df["thrput"] = df["num_ops"] / df["duration"] * 1000.0
        df["ts_sec"] = df["ts"] / 1e9
        thrput = (df["num_ops"].sum() / df["duration"].sum()) * 1000.0
        plt.plot(df["ts_sec"], df["thrput"], label="%s: %.3f" % (d, thrput) )
        plt.xlabel("test progression (sec)" )
        plt.ylabel("throughput (ops/usec)")
    plt.legend()
    plt.title(f"CHURN PHASE {name} Throughput")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_xtime_%s.png" % name))
    plt.close()

def plot_churn_op_throuput_churn(name, ops):
    plt.figure(figsize=(10,6))
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[ (df["op"].isin(ops)) ]
        if (len(df)==0):
            return
        thrput = (df["num_ops"].sum() / df["duration"].sum()) * 1000.0
        grouped = df.groupby("churn_cycle").agg({"num_ops": sum, "duration": sum})
        grouped["thrput"] = (grouped["num_ops"]/grouped["duration"]) * 1000.0
        plt.plot(grouped.index, grouped["thrput"], label="%s: %.3f" % (d, thrput) )
        plt.xlabel("test progression (churn_cycle)" )
        grouped.to_csv(os.path.join(csv_dir, f"{d}_{name}_throughput.csv"))
        plt.ylabel("throughput (ops/usec)")
    plt.legend()
    plt.title(f"CHURN PHASE {name} Throughput")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_%s.png" % name))
    plt.close()

def plot_memory_usage():
    data = []
    labels = []
    for d in variants:
        f_variant = open("%s/%s/test_params.txt" % (dir, d), "r")
        lines = f_variant.readlines()
        memory_usage = int(lines[0])
        data.append(memory_usage)
        labels.append(d)
        print(memory_usage)
    plt.bar(labels, data)
    plt.yscale('log')
    plt.ylabel("Size (B)")
    plt.title("Size usage (B)")
    add_caption()
    plt.title(f"Memory Usage")
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_memory_usage.png"))
    plt.close()

def plot_latency_boxplots(op):
    data = []
    labels = []
    for d in variants:
        df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), delim_whitespace=True)
        df = df.loc[(df["op"]==op)]
        if (len(df)==0):
            return
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

def plot_latency_boxplots_group(ops):
    data = []
    labels = []
    for op in ops:
        for d in variants:
            df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), delim_whitespace=True)
            df = df.loc[(df["op"]==op)]
            if (len(df)==0):
                return
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
        df=df.sort_values(columns[0])
        df.to_csv(os.path.join(csv_dir, f"{d}_{metric}.csv"))
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

plot_churn_op_throuput_ts("DELETE", ["DELETE"])
plot_churn_op_throuput_ts("INSERT", ["INSERT"])
plot_churn_op_throuput_ts("LOOKUP", ["LOOKUP"])
plot_churn_op_throuput_ts("OVERALL", ["INSERT", "DELETE", "LOOKUP"])
plot_churn_op_throuput_ts("OVERALL_NO_LOOKUP", ["INSERT", "DELETE"])
plot_churn_op_throuput_ts("MIXED", ["MIXED"])

plot_churn_op_throuput_churn("DELETE", ["DELETE"])
plot_churn_op_throuput_churn("INSERT", ["INSERT"])
plot_churn_op_throuput_churn("LOOKUP", ["LOOKUP"])
plot_churn_op_throuput_churn("OVERALL", ["INSERT", "DELETE", "LOOKUP"])
plot_churn_op_throuput_churn("OVERALL_NO_LOOKUP", ["INSERT", "DELETE"])
plot_churn_op_throuput_churn("MIXED", ["MIXED"])
plot_tombstone()
plot_tombstone_ratio()

plot_latency_boxplots("DELETE")
plot_latency_boxplots("INSERT")
plot_latency_boxplots("LOOKUP")
plot_latency_boxplots("MIXED")

plot_latency_boxplots_group(["DELETE", "INSERT", "LOOKUP"])

plot_distribution('home_slot_dist')
plot_distribution('tombstone_dist')
plot_memory_usage()
