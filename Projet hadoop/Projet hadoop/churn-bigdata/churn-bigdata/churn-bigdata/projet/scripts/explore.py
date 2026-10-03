"""Exploration rapide de Customer-Churn-Records.csv. Usage : python explore.py [chemin_csv]"""
import sys
import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "../data/Customer-Churn-Records.csv"
df = pd.read_csv(path)
print("Forme :", df.shape)
print(df.dtypes, "\n")
print("Valeurs manquantes :", int(df.isna().sum().sum()))
print("\nClients par pays :\n", df["Geography"].value_counts())
print("\nClients par genre :\n", df["Gender"].value_counts())
print("\nAge :", df["Age"].describe().round(1).to_dict())
print("CreditScore :", df["CreditScore"].describe().round(1).to_dict())
print("\nSolde nul :", int((df["Balance"] == 0).sum()), "clients ; total =", round(df["Balance"].sum(), 2))
print("Solde > 150000 :", int((df["Balance"] > 150000).sum()), "| CreditScore < 500 :", int((df["CreditScore"] < 500).sum()))
print("Colonne Exited presente :", "Exited" in df.columns)
