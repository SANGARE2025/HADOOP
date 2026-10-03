# Modele de prediction

`predict_model.py` entraine un Random Forest (scikit-learn) et sauvegarde `model.joblib` + `metrics.json`.

```
pip install pandas scikit-learn joblib
python predict_model.py ../data/Customer-Churn-Records.csv
```

- Si le CSV contient `Exited` (version complete du jeu Kaggle) : prediction du **churn** (la colonne `Complain`, quasi identique a `Exited`, est exclue pour eviter la fuite de donnees).
- Sinon (fichier fourni, 10 colonnes) : prediction de `has_balance` (solde > 0), cible derivee de `Balance`.
  Resultat : accuracy 0,796 (modele naif 0,638), ROC-AUC 0,847.
