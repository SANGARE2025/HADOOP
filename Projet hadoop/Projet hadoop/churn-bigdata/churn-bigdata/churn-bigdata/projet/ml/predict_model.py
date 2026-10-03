"""
Modele de prediction - Customer Churn
Usage :  python predict_model.py Customer-Churn-Records.csv
         python predict_model.py Customer-Churn-Records.csv --target Exited

Dependances : pip install pandas scikit-learn joblib

- Si le CSV contient la colonne "Exited" (version complete du jeu de donnees Kaggle),
  le modele predit le churn (classification binaire).
- Sinon (cas du fichier fourni, 10 colonnes), il predit "has_balance" :
  le client a-t-il un solde > 0 ? (cible derivee de Balance).
"""
import argparse, json
import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (accuracy_score, classification_report,
                             confusion_matrix, roc_auc_score)
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

ap = argparse.ArgumentParser()
ap.add_argument("csv")
ap.add_argument("--target", default=None)
args = ap.parse_args()

df = pd.read_csv(args.csv)

# ---- choix de la cible ------------------------------------------------
target = args.target or ("Exited" if "Exited" in df.columns else "has_balance")
drop = ["RowNumber", "CustomerId", "Surname"]
if target == "has_balance":
    df["has_balance"] = (df["Balance"] > 0).astype(int)
    drop.append("Balance")            # sinon fuite de la cible
if target == "Exited" and "Complain" in df.columns:
    drop.append("Complain")           # quasi identique a Exited : fuite de donnees
y = df[target]
X = df.drop(columns=[c for c in drop + [target] if c in df.columns])

cat_cols = X.select_dtypes(exclude="number").columns.tolist()
num_cols = [c for c in X.columns if c not in cat_cols]

# ---- pipeline : encodage + Random Forest --------------------------------
pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols)],
                        remainder="passthrough")
model = Pipeline([("pre", pre),
                  ("rf", RandomForestClassifier(n_estimators=300, min_samples_leaf=5,
                                                class_weight="balanced",
                                                random_state=42, n_jobs=-1))])

X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)
model.fit(X_tr, y_tr)

# ---- evaluation ---------------------------------------------------------
pred = model.predict(X_te)
proba = model.predict_proba(X_te)[:, 1]
print(f"Cible : {target} | train={len(X_tr)} test={len(X_te)}")
print(f"Taux de la classe 1 : {y.mean():.3f} (accuracy d'un modele naif : {max(y.mean(), 1 - y.mean()):.3f})")
print(f"Accuracy : {accuracy_score(y_te, pred):.3f} | ROC-AUC : {roc_auc_score(y_te, proba):.3f}")
print(confusion_matrix(y_te, pred))
print(classification_report(y_te, pred, digits=3))

names = model.named_steps["pre"].get_feature_names_out()
imp = pd.Series(model.named_steps["rf"].feature_importances_, index=names).sort_values(ascending=False)
print("Variables les plus importantes :\n", imp.head(8).round(3))

# ---- sauvegarde ---------------------------------------------------------
joblib.dump(model, "model.joblib")
json.dump({"target": target, "features": X.columns.tolist(),
           "accuracy": float(accuracy_score(y_te, pred)),
           "roc_auc": float(roc_auc_score(y_te, proba))}, open("metrics.json", "w"), indent=2)

# ---- exemple de prediction sur un nouveau client (ex : message Kafka) ---
exemple = X.iloc[[0]]
print("\nExemple :", exemple.to_dict("records")[0])
print("Probabilite de la classe 1 :", round(float(model.predict_proba(exemple)[0, 1]), 3))
