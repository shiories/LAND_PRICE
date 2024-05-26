import os
import pandas as pd
import matplotlib.pyplot as plt
import requests
import zipfile
import time
from datetime import datetime
import os

class RealEstateAnalyzer:

    def __init__(self):

        self.city_codes = {
            'a': '台北市',
            'b': '台中市',
            'c': '基隆市',
            'd': '台南市',
            'e': '高雄市',
            'f': '新北市',
            'g': '宜蘭縣',
            'h': '桃園市',
            'i': '嘉義市',
            'j': '新竹縣',
            'k': '苗栗縣',
            'l': '台中縣',
            'm': '南投縣',
            'n': '彰化縣',
            'o': '新竹市',
            'p': '雲林縣',
            'q': '嘉義縣',
            'r': '台南縣',
            's': '高雄縣',
            't': '屏東縣',
            'u': '花蓮縣',
            'v': '台東縣',
            'w': '金門縣',
            'x': '澎湖縣',
            'y': '陽明山管理局',
            'z': '連江縣'
        }


    def real_estate_crawler(self, year, season):
        if year > 1000:
            year -= 1911

        res = requests.get("https://plvr.land.moi.gov.tw//DownloadSeason?season="+str(year)+"S"+str(season)+"&type=zip&fileName=lvr_landcsv.zip")
        fname = str(year)+str(season)+'.zip'
        open(fname, 'wb').write(res.content)

        folder = 'real_estate' + str(year) + str(season)
        if not os.path.isdir(folder):
            os.mkdir(folder)

        with zipfile.ZipFile(fname, 'r') as zip_ref:
            zip_ref.extractall(folder)

        time.sleep(10)


    def read_real_estate_data(self, type_name = str('房屋買賣交易')):
        '''
        type_name = '房屋買賣交易', '新成屋交易', '租房交易'
        '''
        self.type_name = type_name
        # 检查文件夹是否存在，如果不存在则创建
        if not os.path.exists(self.type_name):
            os.makedirs(self.type_name)

        self.real_estate_file_types = { 'a': '房屋買賣交易', 'b': '新成屋交易', 'c': '租房交易' }
        self.type = self.real_estate_file_types[self.type_name]

        # 獲取目前的目錄下以'real'開頭的資料夾清單
        dirs = [d for d in os.listdir() if d[:4] == 'real']
        
        for d in dirs:
            dfs = []
            for code, city in self.city_codes.items():
                # 讀取每個資料夾中的'a_lvr_land_a.csv'檔，並將其載入為DataFrame
                try:
                    df = pd.read_csv(os.path.join(d, f'{code}_lvr_land_{self.type}.csv'), index_col=False, low_memory=False)
                    # 將資料夾名稱的最後一個字元添加到DataFrame中的'Q'列中
                    df['season'] = d[-1:]
                    df["縣市"] = city
                    # 將DataFrame添加到dfs列表中
                    dfs.append(df.iloc[1:])

                except:
                    pass
                
            data_df = pd.concat(dfs, sort=True)# 將所有DataFrame連接成一個DataFrame
            data_df.reset_index(drop=True, inplace=True) # 重設索引，確保索引唯一
            try:
                data_df = self.preprocess_data(data_df) # 資料清理
            except:
                print(f"錯誤季度: {d[-4:]}")

            data_df.to_json(f'{self.type_name}/{d[-4:]}.json', orient='records', indent=4)
            #data_df.to_excel(f'type_name/{d[-4:]}.xlsx')
            print(f"季度 {d[-4:]} 已完成")
            
        return
        

    def chinese_to_arabic(self, chinese_num):
        if pd.isnull(chinese_num):  # 檢查是否為 NaN
            return 0  # 如果是 NaN，則返回 0
        chinese_digits = {'零': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, '七': 7, '八': 8, '九': 9}
        chinese_units = {'十': 10, '百': 100, '千': 1000, '萬': 10000, '億': 100000000}
        chinese_num = chinese_num.replace("層", "")

        total = 0
        num = 0
        for char in chinese_num:
            if char in chinese_digits:
                num = chinese_digits[char]
            elif char in chinese_units:
                if char == '十' and num == 0:  # 处理类似"十一"的情况
                    num = 1
                total += num * chinese_units[char]
                num = 0
        total += num
        return total


    def calculate_building_age(self, date):
        # 如果日期為空，返回空值
        if pd.isnull(date) or date == '':
            return -1
        date = str(date)
        try:
            # 將日期轉換為字符串，進行截取操作並轉換為浮點數，最後轉換為整數並加上 1911
            year = 1911 + int(date[:-4])
            now = int(datetime.now().year)
        except:
            return -1
        # 返回計算結果
        return now - year


    def preprocess_data(self, df):
        df = df.copy()
        df = df[df['備註'].isnull()]  # 刪除有備註之交易（多為親友交易、價格不正常之交易）

        # 單位換算
        df['year'] = df['交易年月日'][:-4].astype(int) + 1911
        df['每坪單價'] = df['單價元平方公尺'].astype(float) * 3.30579 # 平方公尺換成坪
        df['土地坪數'] = df['土地移轉總面積平方公尺'].astype(float) * 0.3025 # 平方公尺換成坪
        df['建物坪數'] = df['建物移轉總面積平方公尺'].astype(float) * 0.3025 # 平方公尺換成坪
        try:
            df['車位坪數'] = df['車位移轉總面積(平方公尺)'].astype(float) * 0.3025 # 平方公尺換成坪
        except:
            df['車位坪數'] = df['車位移轉總面積平方公尺'].astype(float) * 0.3025 # 平方公尺換成坪
        df['陽台坪數'] = df['陽台面積'].astype(float) * 0.3025 # 平方公尺換成坪
        df['附屬建物坪數'] = df['附屬建物面積'].astype(float) * 0.3025 # 平方公尺換成坪
        df['屋齡'] = df['建築完成年月'].apply(self.calculate_building_age)

        # 型別轉換
        df['建物型態'] = df['建物型態'].str.split('(').str[0] # 建物型態
        df['建物現況格局-隔間'] = df['建物現況格局-隔間'].map({'有': True, '無': False})
        df['有無管理組織'] = df['有無管理組織'].map({'有': True, '無': False})        
        df['總樓層數'] = df['總樓層數'].apply(self.chinese_to_arabic)        # 將"總樓層數"列應用轉換函數
        df['都市土地使用分區'] = df['都市土地使用分區'].fillna("特")

        # 刪除列
        df = df.drop(columns=['交易標的', '交易筆棟數', '移轉層次', '移轉編號', '電梯', '非都市土地使用分區', '非都市土地使用編定', '備註']).copy()               
        df = df.rename(columns={'建物現況格局-隔間': '隔間', '有無管理組織': '管理組織', '建物現況格局-廳': '廳數',
                                '建物現況格局-房': '房數', '建物現況格局-衛': '衛數', '都市土地使用分區': '土地類型'})
        # 將index改成年月日
        df.index = pd.to_datetime((df['交易年月日'][:-4].astype(int) + 1911).astype(str) + df['交易年月日'].str[-4:], format='%Y%m%d', errors='coerce')
        df = df[[
            'year', 'season', '縣市', '鄉鎮市區','土地類型',
            '土地位置建物門牌',  '編號', '建物型態', '屋齡', '主要建材', 
            '管理組織', '總樓層數', '房數', '廳數', '衛數',
            '隔間', '土地坪數', '建物坪數', '附屬建物坪數', '陽台坪數',
            '總價元', '車位坪數', '車位總價元', '車位類別'
        ]]
        return df


    def plot_price_trend(self, df):
        plt.rcParams['font.family'] = ['Microsoft YaHei', 'sans-serif']
        plt.figure(figsize=(8, 4.5))
        prices = {}
        for district in set(df['鄉鎮市區']):
            cond = (
                (df['主要用途'] == '住家用')
                & (df['鄉鎮市區'] == district)
                & (df['建物坪數'] < df["建物坪數"].quantile(0.95))
                & (df['建物坪數'] > df["建物坪數"].quantile(0.05))
            )

            groups = df[cond]['year']
            prices[district] = df[cond]['建物坪數'].astype(float).groupby(groups).mean().loc[2012:]

        price_history = pd.DataFrame(prices)
        price_history.plot()
        plt.grid(True)
        plt.savefig(f'房價走勢圖.png', dpi=300)
        plt.close()
            
            
    def plot_price_distribution(self, df):
        plt.rcParams['font.family'] = ['Microsoft YaHei', 'sans-serif']
        plt.figure(figsize=(8, 4.5))
        for district in set(df['鄉鎮市區']):
            dfdistrict = df[df['鄉鎮市區'] == district]
            dfdistrict['建物坪數'][dfdistrict['建物坪數'] < 2000000].hist(bins=120, alpha=0.7)

        plt.xlim(0, 2000000)
        plt.legend(set(df['鄉鎮市區']))
        plt.grid(True)
        plt.savefig(f'房價分佈圖.png', dpi=300)
        plt.close()

   
    def get_range(self, star_year, end_year):
        for year in range(star_year, end_year):
            for season in range(1, 5):
                print(year, season)
                analyzer.real_estate_crawler(year, season)




if __name__ == "__main__":
    analyzer = RealEstateAnalyzer()
    #analyzer.real_estate_crawler(101, 3)   # 取得特定季資料
    #analyzer.get_range(108, 112)  # 取得特定範圍資料

    analyzer.read_real_estate_data(type_name="房屋買賣交易") #建立各季度 json檔

    #analyzer.plot_price_trend(df)    # 繪製房價走勢圖
    #analyzer.plot_price_distribution(df)    # 繪製房價分佈圖