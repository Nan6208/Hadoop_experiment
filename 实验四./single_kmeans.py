# single_kmeans.py
import numpy as np

# 1. 加载数据
data = np.loadtxt("kmeans_data.csv", delimiter=",")

# 2. 固定初始质心
init_centroids = np.array([[2.0, 2.0], [8.0, 3.0], [5.0, 9.0]])

def kmeans_single(X, centers, max_iter=20, tol=1e-6):
    centers = centers.copy()
    for i in range(max_iter):
        # 计算每个点到质心的距离，并分配簇
        distances = np.linalg.norm(X[:, np.newaxis, :] - centers[np.newaxis, :, :], axis=2)
        labels = np.argmin(distances, axis=1)

        # 更新质心位置
        new_centers = np.array([
            X[labels == k].mean(axis=0) if np.sum(labels == k) > 0 else centers[k]
            for k in range(len(centers))
        ])

        # 判断是否收敛（质心移动距离小于阈值）
        shift = np.linalg.norm(new_centers - centers)
        centers = new_centers
        if shift < tol:
            print(f"单机版在第 {i+1} 次迭代后收敛。")
            break

    # 最后一次分配
    distances = np.linalg.norm(X[:, np.newaxis, :] - centers[np.newaxis, :, :], axis=2)
    labels = np.argmin(distances, axis=1)
    return centers, labels

# 3. 运行 K-Means
final_centers, final_labels = kmeans_single(data, init_centroids)

print("单机计算最终质心：")
print(final_centers)
print("前10个标签：", final_labels[:10])

# 4. 保存结果供对比
np.savetxt("single_centers.csv", final_centers, delimiter=",")
np.savetxt("single_labels.csv", final_labels, fmt="%d")
