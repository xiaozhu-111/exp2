import re
import pandas as pd
import matplotlib.pyplot as plt

# 读取结果文件
data = []
with open("DiscountRate_results.txt", "r", encoding="utf-8") as f:
    for line in f:
        parts = line.strip().split("\t")
        if len(parts) != 2:
            continue
        discount_str, usage_rate = parts
        try:
            usage_rate = float(usage_rate)
        except:
            continue

        # 判断是否为满减格式
        if ":" in discount_str:
            nums = re.findall(r"[0-9]+", discount_str)
            if len(nums) >= 2:
                x, y = map(float, nums[:2])
                discount_rate = 1 - y / x
            else:
                discount_rate = None
        else:
            # 直接折扣
            try:
                discount_rate = float(discount_str)
            except:
                discount_rate = None

        if discount_rate is not None:
            data.append((discount_rate * 100, usage_rate))

# 构建 DataFrame
df = pd.DataFrame(data, columns=["DiscountPercent", "UsageRate"])
df = df.sort_values("DiscountPercent")

# 打印结果
print("=== Discount Percentage vs Usage Rate ===")
print(df)

# 绘图（无中文）
plt.figure(figsize=(9, 5))
plt.plot(df["DiscountPercent"], df["UsageRate"], marker='o', linestyle='-', color='royalblue', label="Usage Rate")

plt.title("Relationship between Discount Percentage and Coupon Usage", fontsize=12)
plt.xlabel("Discount Percentage (%)", fontsize=11)
plt.ylabel("Usage Rate", fontsize=11)
plt.grid(alpha=0.3)
plt.legend()
plt.tight_layout()
plt.show()

# 计算相关系数
corr = df["DiscountPercent"].corr(df["UsageRate"])
print(f"\nCorrelation between discount percentage and usage rate: {corr:.3f}")

# 简要分析
if corr < 0:
    print("Interpretation: Negative correlation – higher discount (lower %) increases usage probability.")
elif corr > 0:
    print("Interpretation: Positive correlation – higher discount percentage (smaller discount) increases usage.")
else:
    print("Interpretation: No clear correlation between discount and usage.")
