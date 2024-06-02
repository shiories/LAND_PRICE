import pandas as pd

df = pd.read_json(f'中古屋/all_data.json')


#df = df.drop(columns=["土地位置建物門牌", "編號","每坪單價", "土地坪數", "總價元", "車位坪數", "車位總價元", "車位坪單價","建物坪數"])


df = df.drop(columns=["土地位置建物門牌", "編號", "車位坪數" , "車位總價元", "車位坪單價"])


for col in [ '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別']:
    unique_room_numbers = df[col].unique()# 获取 "房數" 列的唯一值
    print(f"\n{col} 唯一值總數: {len(unique_room_numbers)}")# 打印唯一值总数
    for room_number in unique_room_numbers:# 打印每个唯一值
        print(f'{col} : {room_number}')     
        
        
        

# 按指定列分组，并计算每个组合的平均值
grouped = df.groupby( ['year', 'season', '縣市', '鄉鎮市區', '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別']).mean().reset_index()

# 计算每个组的大小
group_size = df.groupby( ['year', 'season', '縣市', '鄉鎮市區', '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別']).size().reset_index(name='count')

# 合并 mean 结果与 size 结果
grouped = pd.merge(grouped, group_size, on=['year', 'season', '縣市', '鄉鎮市區', '土地類型', '建物型態', '屋齡', '主要建材', '管理組織', '總樓層數', '房數', '廳數', '衛數', '隔間', '車位類別'])

# 打印分组结果
print(grouped)

# 将结果保存到 Excel 文件
grouped.to_excel("grouped.xlsx", index=False)






        
        
        
        