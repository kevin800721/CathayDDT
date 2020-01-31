import pandas as pd
import pycnnum as cnn
import numpy as np

## prepare the dataframe "df_all"
dir_input="C:\\Users\\Kevin&LinTES\\Desktop\\2001_國泰數數發中心\\不動產買賣資料\\"
#dtype_all={"交易年月日":str, "建築完成年月":str, "建物現況格局-房":int, "建物現況格局-廳":int, "建物現況格局-衛":int}
dtype_all={"交易年月日":str, "建築完成年月":str}
df_a=pd.read_csv(dir_input+"a_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_b=pd.read_csv(dir_input+"b_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_e=pd.read_csv(dir_input+"e_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_f=pd.read_csv(dir_input+"f_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_h=pd.read_csv(dir_input+"h_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_all=pd.concat([df_a,df_b,df_e,df_f,df_h], axis=0, ignore_index=True)

## filter_a
# 主要用途為住家用
bool_fa=df_all.loc[:,"主要用途"] == "住家用"
df_fa=df_all.loc[bool_fa["main use"]]

# 建物型態包含"住宅大樓"四字
sr_fa=pd.Series(df_fa["建物型態"].values.flatten())
bool_fa=sr_fa.str.contains("住宅大樓")
bool_fa=bool_fa[bool_fa].index      # only "True" indices left
df_fa=df_fa.iloc[bool_fa,:]

# 總樓層數大於等於十三層
sr_fa=pd.Series(df_fa["總樓層數"].values.flatten())
sr_fa.dropna(inplace=True)
sr_fa=sr_fa.str.slice(0,-1)
for i in range(sr_fa.shape[0]):
    sr_fa[i]=cnn.cn2num(sr_fa[i])   # Chinese number to Arabic number
bool_fa=sr_fa>=13
bool_fa=bool_fa[bool_fa].index      # only "True" indices left
df_fa=df_fa.iloc[bool_fa,:]

## filter b
# 總件數
tot_num=df_all.shape[0]
# 總車位數
sr_fb=pd.Series(df_all["交易筆棟數"].values.flatten())
tot_pl=sr_fb.str.slice(-1).astype(int).sum()
# 平均總價元
avg_pr=df_all["總價元"].mean()
avg_pr=avg_pr.map('${:,.2f}'.format)[0]
# 平均車位總價元
avg_bpr=df_all["車位總價元"].replace(0, np.NaN).mean()
avg_bpr=avg_bpr.map('${:,.2f}'.format)[0]
arr_fb=[["總件數",tot_num],["總車位數",tot_pl],["平均總價元",avg_pr],["平均車位總價元",avg_bpr]]
df_fb=pd.DataFrame(arr_fb)

## export the target dataframes to csv files
df_fa.to_csv("filter_a.csv", sep=",", index=False, encoding="utf-8-sig")
df_fb.to_csv("filter_b.csv", sep=",", index=False, encoding="utf-8-sig", header=False)