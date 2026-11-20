from pyspark import SparkContext, SparkConf
import numpy as np

# 1. 强制使用本地伪分布式模式
conf = SparkConf().setAppName("ParallelKMeans").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# 2. 加载数据
lines = sc.textFile("file:///home/hadoop/kmeans_exp/kmeans_data.csv")
data = lines.map(lambda line: np.array([float(x) for x in line.split(",")])).cache()

# 3. 固定初始质心
init_centroids = np.array([[2.0, 2.0], [8.0, 3.0], [5.0, 9.0]])
centers = init_centroids.copy()

def assign(point, centers):
    distances = [np.sum((point - c) ** 2) for c in centers]
    best = int(np.argmin(distances))
    return (best, (point, 1)) # 返回 (簇标签, (坐标, 计数1))

max_iter = 20
tol = 1e-6

# 4. 迭代 MapReduce 过程
for i in range(max_iter):
    broadcast_centers = sc.broadcast(centers)
    # Map 阶段：每个点找最近质心
    assigned = data.map(lambda p: assign(p, broadcast_centers.value))
    # Reduce 阶段：按标签聚合，计算坐标总和与总数
    agg = assigned.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    
    # 更新质心
    new_centers_list = agg.map(lambda kv: (kv[0], kv[1][0] / kv[1][1])).collect()
    new_centers = np.zeros_like(centers)
    for idx, vec in new_centers_list:
        new_centers[idx] = vec

    # 检查收敛
    shift = np.linalg.norm(new_centers - centers)
    centers = new_centers
    broadcast_centers.unpersist()
    if shift < tol:
        print(f"Spark并行版在第 {i+1} 次迭代后收敛。")
        break

# 5. 输出最终标签
broadcast_centers = sc.broadcast(centers)
final_labels = data.map(lambda p: assign(p, broadcast_centers.value)[0]).collect()

print("\n===== 成功得到结果 =====")
print("Spark 并行计算最终质心：")
print(centers)

# 6. 保存结果供对比
np.savetxt("spark_centers.csv", centers, delimiter=",")
np.savetxt("spark_labels.csv", np.array(final_labels), fmt="%d")
print("结果文件已保存！")

sc.stop()
