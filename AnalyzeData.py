import pandas as pd

df = pd.read_json(f'中古屋/all_data.json')




df = df.drop(columns=["土地位置建物門牌", "編號","每坪單價", "土地坪數", "總價元", "車位坪數", "車位總價元", "車位坪單價","建物坪數"])

df['屋齡'] = df['屋齡'].apply(lambda x: x if x >= 0 else 0)# 将负值设为 0
df['屋齡'] = df['屋齡'].fillna(0)# 填充 NaN 值
df['屋齡'] = df['屋齡'] // 10 * 10
df.loc[df['屋齡'] >= 100, '屋齡'] = 100# 将剩余的 NaN 值填充为 100

# 对房间数进行处理
for col in ["房數", "廳數", "衛數","總樓層數"]:
    df[col] = df[col].fillna(0)
    df[col] = pd.cut(df[col], bins=range(0, 11), labels=[f'{i}' for i in range(1, 11)], right=False)
    df[col] = df[col].str.extract(r'(\d+)').astype(float).fillna(0)  # 提取数字后填充NaN值为0


for col in [ '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別']:
    unique_room_numbers = df[col].unique()# 获取 "房數" 列的唯一值
    print(f"{col} 唯一值總數: {len(unique_room_numbers)}\n")# 打印唯一值总数
    for room_number in unique_room_numbers:# 打印每个唯一值
        print(f'{col} : {room_number}')

# 按指定列分组，并计算每个组合的数量
grouped = df.groupby(['year', 'season', '縣市', '鄉鎮市區', '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別']).size()
# 打印分组结果
print(grouped)

grouped.to_excel("grouped.xlsx")



        
        
        
        