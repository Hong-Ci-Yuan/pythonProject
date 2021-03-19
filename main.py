import numpy as np
import pandas as pd
import pandas_read_xml as pdx
import cn2an
import re

file_name_array = ['a_lvr_land_a', 'b_lvr_land_a', 'e_lvr_land_a', 'f_lvr_land_a', 'h_lvr_land_a']
file_row_array = []
j = 0
df_a = ()
df_b = ()
df_e = ()
df_f = ()
df_h = ()


def get_dataframe(file_name):
    df = pdx.read_xml('D:\PycharmProjects\pythonProject\config\\' + file_name + '.xml', ['lvr_land', '買賣'],
                      encoding='UTF-8')
    return df


def get_total_parking_space(param):
    total = 0
    for str in param:
        space = re.search('車位(\d+)', str).group(1)
        total = total + int(space)
    return total


for file_name in file_name_array:
    if file_name.startswith('a'):
        df_a = get_dataframe(file_name)
    if file_name.startswith('b'):
        df_b = get_dataframe(file_name)
    if file_name.startswith('e'):
        df_e = get_dataframe(file_name)
    if file_name.startswith('f'):
        df_f = get_dataframe(file_name)
    if file_name.startswith('h'):
        df_h = get_dataframe(file_name)

# df_all = pd.DataFrame(pd.concat([df_a, df_b, df_e, df_f, df_h]))
df_all = pd.merge(df_a, df_b, how='outer')
df_all = df_all.merge(df_e, how='outer')
df_all = df_all.merge(df_f, how='outer')
df_all = df_all.merge(df_h, how='outer')
df_all.to_csv('D:/all.csv', index=False)
condition = ((df_all['主要用途'] == '住家用') & (df_all['建物型態'].str.contains('住宅大樓')) & (
        df_all['總樓層數'].isnull() == False))
filteredData = df_all[condition].reset_index(drop=True)
for i in filteredData['總樓層數']:
    temp = cn2an.cn2an(i.replace('層', '')) >= 13
    file_row_array.append(temp)

for i in file_row_array:
    if i != True: filteredData = filteredData.drop(index=[j])
    j = j + 1
filteredData.to_csv('D:/filter_a.csv', index=False)

parking_space_sum = get_total_parking_space(df_all['交易筆棟數'])
price_ave = np.asarray(df_all['總價元'], dtype=np.float).mean()
parking_space_ave = np.asarray(df_all['車位總價元'], dtype=np.float).sum() / parking_space_sum
dict = {'總件數': [df_all.shape[0]], '總車位數': [parking_space_sum], '平均總價元': [round(price_ave, 2)],
        '平均車位總價元': [round(parking_space_ave, 2)]}
df_sum = pd.DataFrame(dict)
df_sum.to_csv('D:/filter_b.csv', index=False)
