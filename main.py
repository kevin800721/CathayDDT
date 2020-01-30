import pandas as pd

dir_input="C:\\Users\\Kevin&LinTES\\Desktop\\2001_國泰數數發中心\\不動產買賣資料\\"
#dtype_all={"交易年月日":str, "建築完成年月":str, "建物現況格局-房":int, "建物現況格局-廳":int, "建物現況格局-衛":int}
dtype_all={"建物型態":str, "交易年月日":str, "建築完成年月":str}
df_a=pd.read_csv(dir_input+"a_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_b=pd.read_csv(dir_input+"b_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_e=pd.read_csv(dir_input+"e_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_f=pd.read_csv(dir_input+"f_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_h=pd.read_csv(dir_input+"h_lvr_land_a.csv", header=[0,1], dtype=dtype_all)
df_all=pd.concat([df_a,df_b,df_e,df_f,df_h], axis=0, ignore_index=True)

#filter
bool_fa=df_all.loc[:,"主要用途"] == "住家用"
df_fa=df_all.loc[bool_fa["main use"]]

sr_fa=pd.Series(df_fa["建物型態"].values.flatten())
bool_fa=sr_fa.str.contains("住宅大樓")
bool_fa=bool_fa[bool_fa].index      # only "True" indices left
df_fa=df_fa.iloc[bool_fa,:]

