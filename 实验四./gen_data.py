# gen_data.py
import numpy as np

np.random.seed(42)  # 固定种子，保证每次生成相同数据
c1 = np.random.normal(loc=[2, 2], scale=1.0, size=(100, 2))
c2 = np.random.normal(loc=[8, 3], scale=1.0, size=(100, 2))
c3 = np.random.normal(loc=[5, 9], scale=1.0, size=(100, 2))
data = np.vstack([c1, c2, c3])
np.savetxt("kmeans_data.csv", data, delimiter=",", fmt="%.6f")
print("数据形状：", data.shape)
print("x 范围：", data[:, 0].min(), "~", data[:, 0].max())
print("y 范围：", data[:, 1].min(), "~", data[:, 1].max())
