# Dataset : Customer Churn Records

Fichier : `Customer-Churn-Records.csv` (source : Kaggle, "Bank Customer Churn" - version a 10 colonnes).

```
RowNumber,CustomerId,Surname,CreditScore,Geography,Gender,Age,Tenure,Balance,NumOfProducts
```

- Separateur **virgule**, une ligne d'en-tete, fins de ligne Windows (CRLF).
- Aucun champ ne contient de virgule : un simple `split(",")` suffit en Java (verifie : les 10 001 lignes ont exactement 10 champs).
- Fichier de ~0,7 Mo : il tient dans un seul bloc HDFS (un seul mapper).

## Constat apres exploration (`../scripts/explore.py`)

- **10 000 lignes**, 10 colonnes, **aucune valeur manquante**.
- 3 pays : France 5 014, Germany 2 509, Spain 2 477. Genre : Male 5 457, Female 4 543.
- Age : moyenne 38,9 (min 18, max 92). CreditScore : moyenne 650,5 (min 350, max 850).
- **3 617 clients ont un solde nul**. Total des soldes : 764 858 892,88.
- 969 clients avec un solde > 150 000 ; 632 avec un CreditScore < 500 (seuils des alertes du streaming).
- **Pas de colonne de date** : le flux streaming est simule a cadence reguliere (`event_time` ajoute par le producteur).
- **Pas de colonne `Exited`** : impossible de predire le churn avec ce fichier (voir `../ml/`).
