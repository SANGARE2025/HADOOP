# batch-churn

Job MapReduce : **nombre de clients et solde total par pays et par tranche d'age**, a partir de `../data/Customer-Churn-Records.csv`.

- `com.projetfinal.batch.ChurnMapper` : parse chaque ligne CSV, ignore l'en-tete, emet `"Pays\tTrancheAge"` -> `"1;Balance"`. Tranches : `<30`, `30-39`, `40-49`, `50-59`, `60+`. Les lignes mal formees incrementent le compteur `Churn / MALFORMED_LINES`.
- `com.projetfinal.batch.ChurnCombiner` : additionne (nombre, somme) cote mapper.
- `com.projetfinal.batch.ChurnReducer` : somme finale, sortie `Pays\tTranche\tNbClients\tSoldeTotal`.
- `com.projetfinal.batch.SoldeParPaysTranche` : driver (supprime le repertoire de sortie s'il existe).

## Execution en local

```
mvn package
mvn dependency:build-classpath -Dmdep.outputFile=cp.txt
java -cp "target/classes:$(cat cp.txt)" com.projetfinal.batch.SoldeParPaysTranche \
     src/main/resources/input/sample.csv src/main/resources/output
```
(sous Windows, remplacer `:` par `;` et definir `HADOOP_HOME` avec `winutils.exe`, voir `../../tp1-wordcount/README.md`).

## Execution sur le cluster Docker

```
docker cp ../data/Customer-Churn-Records.csv hadoop-master:/root/Customer-Churn-Records.csv
docker cp target/batch-churn-1.jar hadoop-master:/root/batch-churn-1.jar

docker exec hadoop-master bash -lc "
  hadoop fs -mkdir -p input-churn &&
  hadoop fs -put -f /root/Customer-Churn-Records.csv input-churn/ &&
  hadoop jar /root/batch-churn-1.jar com.projetfinal.batch.SoldeParPaysTranche input-churn output-churn &&
  hadoop fs -cat output-churn/part-r-00000
"
```

## Resultat attendu : 15 lignes (3 pays x 5 tranches)

`src/main/resources/output-full/part-r-00000` (extrait) :
```
France   30-39   2250   141026326.19
Germany  30-39    997   118949825.97
Spain    30-39   1099    66285876.65
```
Somme par pays = 311 332 479,49 (France), 300 402 861,38 (Germany), 153 123 552,01 (Spain).

> **Statut** : les fichiers `output-full/` et `output/` (echantillon `sample.csv`, 40 clients) ont ete **generes par simulation Python** de la meme logique (`../scripts/simulate_batch.py`). Le code Java n'a pas ete compile ici : apres votre premiere execution reelle (locale puis cluster), comparez avec ces fichiers et remplacez-les par la vraie sortie.
