#!/usr/bin/python3
import sys
import pandas as pd
from matplotlib import pyplot as plt


df_rhm = pd.read_csv('rhm-thrput-load.txt', delim_whitespace=True)
df_trhm = pd.read_csv('trhm-thrput-load.txt', delim_whitespace=True)
plt.xlabel("inital_load_%")
plt.ylabel("ops/usec")
plt.plot(df_rhm["x_0"], df_rhm["y_0"], label="rhm")
plt.plot(df_trhm["x_0"], df_trhm["y_0"], label="trhm")
plt.legend()
plt.title("Q=22, R=16, Initial Load 90%, 10 churn cycles, 40000 I+D per churn")
plt.savefig("plot_load.png")
plt.close()

df_rhm = pd.read_csv('rhm-thrput-churn.txt', delim_whitespace=True)
df_trhm = pd.read_csv('trhm-thrput-churn.txt', delim_whitespace=True)
plt.xlabel("churn_%")
plt.ylabel("ops/usec")
plt.plot(df_rhm["x_0"], df_rhm["y_0"], label="rhm")
plt.plot(df_trhm["x_0"], df_trhm["y_0"], label="trhm")
plt.legend()
plt.title("Q=22, R=16, Initial Load 90%, 10 churn cycles, 40000 I+D per churn")
plt.savefig("plot_churn.png")
plt.close()