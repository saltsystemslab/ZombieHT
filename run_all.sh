#!/bin/bash

./bench/run_churn_paper_small.sh
./bench/run_churn_paper_nomix.sh &> report_nomix.txt
./bench/run_churn_paper_mixed.sh &> report_mix.txt
