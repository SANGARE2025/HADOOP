# TP1 - Hadoop et MapReduce

Projet Maven du TP1 (voir `../tp1_hadoop_map_raduce.pdf`), applique au fichier `Customer-Churn-Records.csv`.

## Contenu

- `tn.insat.tp1.WordCount` (+ `TokenizerMapper` / `IntSumReducer`) : exemple du sujet. Test : `src/main/resources/input/file.txt`.
- `tn.insat.tp1.BalanceByGeography` (+ `BalanceByGeographyMapper` / `BalanceSumCombiner` / `BalanceSumReducer`) : **exercice d'application** - total des soldes par pays (equivalent de « total des ventes par magasin » ; `Geography` remplace `magasin`, `Balance` remplace `cout`).
- `tn.insat.tp1.GeographyStats` : bonus - nombre de clients, total et moyenne par pays.

## Execution en local (IntelliJ)

Main class `tn.insat.tp1.BalanceByGeography`, arguments :
```
src/main/resources/input/Customer-Churn-Records.csv src/main/resources/output
```
Sortie attendue (`part-r-00000`) :
```
France	311332479.49
Germany	300402861.38
Spain	153123552.01
```

## Cluster Docker (3 noeuds)

```
docker pull liliasfaxi/spark-hadoop:hv-2.7.2
docker network create --driver=bridge hadoop
docker run -itd --net=hadoop -p 50070:50070 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
  --name hadoop-master --hostname hadoop-master liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd -p 8040:8042 --net=hadoop --name hadoop-slave1 --hostname hadoop-slave1 liliasfaxi/spark-hadoop:hv-2.7.2
docker run -itd -p 8041:8042 --net=hadoop --name hadoop-slave2 --hostname hadoop-slave2 liliasfaxi/spark-hadoop:hv-2.7.2
docker exec -it hadoop-master bash
./start-hadoop.sh
```

## Build + execution sur le cluster

```
mvn clean package
docker cp target/churn-mapreduce-1.jar hadoop-master:/root/churn-mapreduce-1.jar
docker cp src/main/resources/input/Customer-Churn-Records.csv hadoop-master:/root/

# dans hadoop-master :
hadoop fs -mkdir -p input
hadoop fs -put -f Customer-Churn-Records.csv input
hadoop jar churn-mapreduce-1.jar tn.insat.tp1.BalanceByGeography input/Customer-Churn-Records.csv output-balance
hadoop fs -cat output-balance/part-r-00000
```

> Statut : code non compile dans l'environnement de redaction (pas de Maven/Hadoop disponible) ; resultats attendus verifies par simulation Python.
