# Data Process for ACM SIGSPATIAL 2021 ST-KNN-JOIN

```bash
# 数据统计
spark-submit \
--class com.urbancomputing.Main st-knn-1.0.jar geom-analysis hdfsPath [point,linestring,ploygon]
```

```bash
# 将滴滴盖亚数据转为JUST新引擎可加载的轨迹数据
spark-submit \
--class com.urbancomputing.Main st-knn-1.0.jar didi-to-new-just inputHdfsPath outputHdfsPath
```

```bash
# MBR过滤
spark-submit \
--class com.urbancomputing.Main st-knn-1.0.jar extract hdfsPath mbrParma
```
