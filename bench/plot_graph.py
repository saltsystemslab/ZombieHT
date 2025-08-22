#!/usr/bin/python3
import sys
import pandas as pd
import numpy as np
import os
from matplotlib import pyplot as plt
import json

method_mapping = {
    'ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED': 'ZombieHT(V)',
    'CUCKOO': 'Cuckoo',
    'GRHM': 'GraveyardHT',
    'GZHM_ADAPTIVE': 'ZombieHT(C)',
    'RHM': 'RobinHoodHT',
    'ICEBERG_SINGLE_THREAD': 'IcebergHT',
    'TRHM': 'TombstoneHT'
}
ordered = ['ZombieHT(C)', 'GraveyardHT', 'TombstoneHT', 'RobinHoodHT']

dir = "bench_run"
if len(sys.argv) > 1:
    dir = sys.argv[1]
    print(dir)
variants = next(os.walk(dir))[1]

csv_dir = os.path.join(sys.argv[2], 'csv')
print(csv_dir)
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
lookup_ops = int(lines[8]) 

for v in variants:
    f = open("%s/%s/test_params.txt" % (dir, v), "r")
    lines = f.readlines()
    churn_cycles = max(churn_cycles, int(lines[5]))
    churn_ops = max(churn_ops, int(lines[6]))
    lookup_ops = max(lookup_ops, int(lines[8]))



def load_factor(variant):
    f = open("%s/%s/test_params.txt" % (dir, variant), "r")
    lines = f.readlines()
    return int(lines[4])

churn_points = []
for l in lines[6:]: 
    churn_points.append(float(l))

def add_caption():
    plt.text(.5, -0.15, f"Update ChurnOps: {churn_ops}, Lookup ChurnOps: {lookup_ops} ChurnCycles: {churn_cycles}", ha='center', transform=plt.gca().transAxes)

def plot_tombstone():
    plt.figure(figsize=(10,6))
    for d in variants:
        if not os.path.exists('./%s/%s/churn_metadata.txt'% (dir, d)):
            continue
        df = pd.read_csv('./%s/%s/churn_metadata.txt' % (dir, d), sep='\s+')
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
        if not os.path.exists('./%s/%s/churn_metadata.txt' % (dir, d)):
            continue
        df = pd.read_csv('./%s/%s/churn_metadata.txt' % (dir, d), sep='\s+')
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
        if not os.path.exists('./%s/%s/churn_thrput.txt'% (dir, d)):
            continue
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), sep='\s+')
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


def plot_churn_op_throuput_churn(name, ops, csv=False, stats=False):
    plt.figure(figsize=(10,6))
    thrput_table = pd.DataFrame()
    overall_tput = {}
    covar = {}
    for d in variants:
        if not os.path.exists('./%s/%s/churn_thrput.txt'% (dir, d)):
            continue
        df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), sep='\s+')
        df = df.loc[ (df["op"].isin(ops)) ]
        if (len(df)==0):
            return
        thrput = (df["num_ops"].sum() / df["duration"].sum()) * 1000.0
        overall_tput[d] = thrput
        grouped = df.groupby("churn_cycle").agg({"num_ops": "sum", "duration": "sum"})
        grouped["thrput"] = (grouped["num_ops"]/grouped["duration"]) * 1000.0
        l = d
        if d == 'ABSL_LINEAR_REHASH_CLUSTER_DEAMORTIZED':
            l = 'GZHM(V)'
        if d == 'ICEBERG_SINGLE_THREAD':
            l = 'ICEBERG'

        plt.plot(grouped.index, grouped["thrput"], label="%s: %.3f" % (l, thrput) )
        plt.xlabel("test progression (churn_cycle)" )

        grouped.to_csv(os.path.join(csv_dir, f"{d}_{name}_throughput.csv"))
            # Adding 0 throughput for TRHM to show that it didn't get that far.
        NUM_CHURN_CYCLES=churn_cycles
        if (len(grouped) < NUM_CHURN_CYCLES + 1):
            with open(os.path.join(csv_dir, f"{d}_{name}_throughput.csv"), "a") as csvfile:
                for i in range(len(grouped), NUM_CHURN_CYCLES):
                    csvfile.write(f"{i},0,0,0\n")
        plt.ylabel("throughput (ops/usec)")
    ot = (pd.DataFrame(overall_tput, index=["Thput"]).transpose().sort_index())
    ot.columns = ['MOps/sec']  # Rename the column
    ot.index.name = 'Table'   # Name the index
    ot = (ot.rename(index=method_mapping))

    ot.loc[ordered].to_latex(os.path.join(csv_dir, 'ordered_throughput.tex'), float_format='%.2f')

    plt.legend()
    update_ratio = (2 * churn_ops) / (lookup_ops + 2 * churn_ops) * 100.0
    lookup_ratio = (lookup_ops) / (lookup_ops + 2 * churn_ops) * 100.0
    plt.title(f"{name} Throughput, Update:Lookup Ratio = {update_ratio:.0f}:{lookup_ratio:.0f}")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_churn_%s.png" % name))
    plt.close()


def plot_latency_boxplots(op):
    data = []
    labels = []
    for d in variants:
        if not os.path.exists('./%s/%s/churn_latency.txt'% (dir, d)):
            continue
        df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), sep='\s+')
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
            if not os.path.exists('./%s/%s/churn_latency.txt' % (dir, d) ):
                continue
            df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), sep='\s+')
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
        df = pd.read_csv(metric_file, sep='\s+')
        columns = df.columns
        df=df.sort_values(columns[0])
        df.to_csv(os.path.join(csv_dir, f"{d}_{metric}.csv"))
        plt.plot(df[columns[0]] - (bar_idx * total_bar_width/num_variants), df[columns[1]], label=d) 
        bar_idx = bar_idx + 1 
        plt.title("%s Distribution" % (columns[0]))
    plt.yscale('log')
    plt.legend()
    plt.savefig(os.path.join(dir, "plot_%s.png" % metric))
    plt.close()


def humanize_nanoseconds(sec):
    time_str = ('%.2f') % (sec / 1000.0)
    return time_str

def latency_distribution(dir, ops):
    summaries = pd.DataFrame()
    all_summaries = pd.DataFrame()
    for op in ops:
        rel_var = {}
        for d in variants:
            df = pd.read_csv('./%s/%s/churn_latency.txt' % (dir, d), delim_whitespace=True)
            df = df.loc[(df["op"]==op)]
            if (len(df)==0):
                continue
            summaries[f'{d}'] = df['latency'].describe(percentiles=[.50, .90, .9999])
            rel_var[d] = summaries[d]['std']/summaries[d]['mean'] * 100.0
        hsum = pd.DataFrame()
        for column in summaries.columns:
            hsum[column] = summaries[column].map(lambda x : humanize_nanoseconds(x))
        hsum = pd.concat([hsum, pd.DataFrame(rel_var, index=['relVar'])])
        print(os.path.join(csv_dir, f"{op}_latency.tex"))
        with open(os.path.join(csv_dir, f"{op}.tex"), "w") as table_file:
            table_file.write(hsum[["GZHM", "RHM", "TRHM", "GRHM", "GZHMV", "ABSL", "CLHT", "ICEBERG", "CUCKOO"]].to_latex(float_format='%.2f'))
            #table_file.write(hsum[["ABSL"]].to_markdown())
            #print(hsum[["ABSL"]])

def humanize_bytes(bytes):
    bytes_str = ('%d B') % (bytes)
    if (bytes > 1024):
        bytes_str = ('%.2lf KB') % (bytes / (1024.0))
    if (bytes > 1024 * 1024):
        bytes_str = ('%.2lf MB') % (bytes / (1024.0 * 1024))
    if (bytes > 1024 * 1024 * 1024):
        bytes_str = ('%.2lf GB') % (bytes / (1024.0 * 1024 * 1024))
    return bytes_str

def get_thrput(d):
    df = pd.read_csv('./%s/%s/churn_thrput.txt' % (dir, d), sep='\s+')
    thrput = (df["num_ops"].sum() / df["duration"].sum()) * 1000.0
    return thrput

def memory_usage(dir):    
    data = []
    labels = []
    hm_type = []
    lfs = []
    tput = []
    df = pd.DataFrame()
    for d in variants:
        f_variant = open("%s/%s/test_params.txt" % (dir, d), "r")
        lines = f_variant.readlines()
        memory_usage = int(lines[0])
        data.append(memory_usage)
        print(data)
        labels.append(d)
        hm_type.append(d) # Hack, first three characters to identify hashmap type for space efficiency study.
        lfs.append(load_factor(d))
    df['Hashmap'] = labels
    df['hm_type'] = hm_type
    df['USAGE'] = data
    df['Size'] = df['USAGE'].map(lambda x: humanize_bytes(x))
    df['Space Efficiency'] = (0.95 * 1694498816) / df['USAGE'] # Hardcoded for 2^27 bits and 0.95 load factor from universe of 2^64
    df['Hashmap'] = df['Hashmap'].map(method_mapping) 
    with open(os.path.join(csv_dir, f"mem.tex"), "w") as table_file:
            table_file.write(df[['Hashmap', "Size", "Space Efficiency"]].to_latex(float_format="%.4f"))



def plot_load_phase():
    overall = {}
    plt.figure(figsize=(20,6))
    for d in variants:
        if not os.path.exists('./%s/%s/load.txt' % (dir, d)):
            continue
        f_variant = open("%s/%s/test_params.txt" % (dir, d), "r")
        lines = f_variant.readlines()
        memory_usage = int(lines[0])
        df = pd.read_csv('./%s/%s/load.txt' % (dir, d), sep='\s+')
        #df['se'] = (df['x_0'] * load_factor(d) / 100.0) * ((2**quotient_bits) * 16) / memory_usage
        df['se'] = (df['x_0'] * load_factor(d) / 100.0) * (0.95 * 1694498816) / memory_usage
        df['lf'] = df['x_0'] * 0.95 #(df['x_0'] * load_factor(d) / 100.0) * (0.95 * 1694498816) / memory_usage
        df["y_0"] = df["y_0"] * 1000.0 
        df.to_csv(os.path.join(csv_dir, f"{d}_load_phase.csv"))
        plt.plot(df["lf"], df["y_0"], label=d + ':' + str(df["y_0"].mean()), marker='.')
        plt.xlabel("load_factor" )
        plt.ylabel("throughput")
        overall[d] = df["y_0"].mean()
    plt.legend()
    plt.title("LOAD PHASE")
    add_caption()
    plt.tight_layout()
    plt.savefig(os.path.join(dir, "plot_insert.png"))
    plt.close()

    ot = (pd.DataFrame(overall, index=["Thput"]).transpose().sort_index())
    ot.columns = ['MOps/sec']  # Rename the column
    ot.index.name = 'Table'   # Name the index
    ot = (ot.rename(index=method_mapping))
    ot.loc[ordered].to_latex(os.path.join(csv_dir, 'ordered_load_throughput.tex'), float_format='%.2f')


memory_usage(dir)
plot_load_phase()
plot_churn_op_throuput_churn("OVERALL", ["INSERT", "DELETE", "LOOKUP"], csv=True, stats=True)
plot_distribution('home_slot_dist')
plot_distribution('tombstone_dist')