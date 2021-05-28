# ST-KNN

```bash
# 基于华均处理后的Lorry数据集再次处理
spark-submit \
--master yarn \
--deploy-mode cluster \
--num-executors 30 \
--driver-cores 5 \
--driver-memory 5G \
--executor-cores 5 \
--executor-memory 12G \
--class com.urbancomputing.Main st-knn-1.0.jar lorry hdfs://just//experiment/ksim/cleaned_data/lorry hdfs://just//experiment/st_knn/linestring/lorry
```

```bash
# 数据统计
spark-submit \
--executor-memory 10g \
--executor-cores 8 \
--driver-memory 10g \
--driver-cores 8 \
--class com.urbancomputing.Main st-knn-1.0.jar geom-analysis hdfs://just//experiment/st_knn/linestring/lorry linestring
```