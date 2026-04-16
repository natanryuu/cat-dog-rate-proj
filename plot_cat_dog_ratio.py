"""
折線圖：台北市 12 區貓犬比趨勢 (2015-2024)
Input:  data/petgov_panel_包含戶數.csv
Output: outputs/fig_cat_dog_ratio_trend.png
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib

# 中文字體設定（Windows）
matplotlib.rcParams["font.sans-serif"] = ["Microsoft JhengHei", "SimHei"]
matplotlib.rcParams["axes.unicode_minus"] = False

# 讀取資料
df = pd.read_csv("data/petgov_panel_包含戶數.csv")

# 畫圖
fig, ax = plt.subplots(figsize=(12, 6))

for district, group in df.groupby("district"):
    ax.plot(group["year"], group["cat_dog_ratio"], marker="o", label=district)

# 貓犬比 = 1 的參考線
ax.axhline(y=1, color="red", linestyle="--", linewidth=1, alpha=0.7, label="貓犬比 = 1")

ax.set_xlabel("年份", fontsize=12)
ax.set_ylabel("貓犬比（貓/犬）", fontsize=12)
ax.set_title("台北市 12 區貓犬比趨勢（2015–2025）", fontsize=14)
ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=9)
ax.set_xticks(df["year"].unique())
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig("outputs/fig_cat_dog_ratio_trend.png", dpi=200, bbox_inches="tight")
plt.close()

print("已儲存至 outputs/fig_cat_dog_ratio_trend.png")
