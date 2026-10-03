"""Simule la logique du job batch-churn (mapper + reducer) en Python.
Sert a verifier les resultats attendus et a generer output-full/ et output/.
Usage : python simulate_batch.py <csv> <fichier_sortie>"""
import csv, sys
from collections import defaultdict

def band(a):
    return "<30" if a < 30 else "30-39" if a < 40 else "40-49" if a < 50 else "50-59" if a < 60 else "60+"

cnt, tot = defaultdict(int), defaultdict(float)
with open(sys.argv[1], newline="") as f:
    for i, r in enumerate(csv.reader(f)):
        if i == 0 or len(r) < 10:
            continue
        k = (r[4], band(int(r[6])))
        cnt[k] += 1
        tot[k] += float(r[8])
with open(sys.argv[2], "w") as o:
    for k in sorted(cnt):                      # Hadoop trie les cles
        o.write(f"{k[0]}\t{k[1]}\t{cnt[k]}\t{tot[k]:.2f}\n")
print(len(cnt), "lignes ecrites")
