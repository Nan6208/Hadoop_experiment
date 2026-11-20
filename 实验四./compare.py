# compare.py
import numpy as np

# 读取两端生成的标签
single = np.loadtxt("single_labels.csv", dtype=int)
spark = np.loadtxt("spark_labels.csv", dtype=int)

print("样本数: 单机", len(single), "| Spark", len(spark))
print("直接比较完全一致：", np.array_equal(single, spark))

# 如果编号顺序不同，按质心对齐后再比较
if not np.array_equal(single, spark):
    sc_c = np.loadtxt("single_centers.csv", delimiter=",")
    pc_c = np.loadtxt("spark_centers.csv", delimiter=",")
    mapping = {}
    for i, c in enumerate(pc_c):
        j = int(np.argmin([np.linalg.norm(c - s) for s in sc_c]))
        mapping[i] = j
    spark_aligned = np.array([mapping[x] for x in spark])
    print("对齐编号后完全一致：", np.array_equal(single, spark_aligned))
    if not np.array_equal(single, spark_aligned):
        diff = np.where(single != spark_aligned)[0]
        print("发现不一致的样本数：", len(diff))
        print("不一致的前20个索引：", diff[:20])
