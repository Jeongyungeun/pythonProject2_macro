import requests
from bs4 import BeautifulSoup
import pymysql
import re
import pandas as pd
from tqdm import tqdm
import time


# TODO PEGR 구하기.
# 1.현재 PER 구하기 (현 시가총액/ 당기순이익 (4분기 합산))
# 2. EPS증가율 구하기 (컨센 있는경우, 없는 경우)__ 3년치와 1년치 EPS증가율

db_conf = {
        "host": "127.0.0.1",
        "user": 'root',
        "password": "!wjddbsrms3@",
        "database": "stock_db"
    }

def df_1_out():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("select date, net_profit_control, eps, code from naver_fs_y")
    data_ = cur.fetchall()
    df_ = pd.DataFrame(data_)
    con.commit()
    con.close()
    return df_

def df_1_out_qoq():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("select date, net_profit_control, eps, code from naver_fs_q")
    data_ = cur.fetchall()
    df_ = pd.DataFrame(data_, columns=['date', 'np', 'eps', 'code'])
    con.commit()
    con.close()
    return df_


# 현 시가총액 ( 네이버 부가정보에서 크롤링)
def get_total_price(code):
    # 현 시가총액(단위 억원)
    r = requests.get("https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={}".format(code))
    bs_rt = BeautifulSoup(r.text, 'html.parser')

    find_table = bs_rt.find("table", id="cTB11")
    tr_data = find_table.findAll("tr")
    abc = []
    for id_x, i in enumerate(tr_data):
        abc.append(i.find('td').text.strip())

    asd = float(''.join(abc[4][:-2].split(',')))
    return asd # float 형태의 시가총액


def code_list():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("select symbol from stock_code")
    row = cur.fetchall()
    code_list = []
    for a in row:
        code_list.append(a[0])
    con.commit()
    con.close()
    return code_list


def code_list_jongmok():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("select symbol, name, sector  from stock_code")
    df_a = pd.DataFrame(cur.fetchall(), columns=['code', 'jongmok', 'upjong'])
    return df_a





def get_total_income(code, df_, df_qoq):

# 1. 가져올 데이터 ( date, eps, net_profit_control, code)

# 컨센이 있는경우


    try:
        ### 1년치 재무제표를 비교 #############
        df_1 = df_[df_[3].isin([code])]
        df_2 = df_1[df_1[0].isin(["2020/12(E)"])]
        key_ = df_2[1].values
        eps_1 = df_2[2].values  # 20년 추정 eps
        df_3 = df_1[df_1[0].isin(["2019/12"])]
        eps_2 = df_3[2].values # 19년  eps
        df_4 = df_1[df_1[0].isin(['2017/12'])]
        eps_3 = df_4[2].values # 17년 eps

        ######### 분기별 재무제표를 합산 ############
        df_q_1 = df_qoq[df_qoq['code'].isin([code])]
        df_q_1[df_q_1.columns[1:3]] = df_q_1[df_q_1.columns[1:3]].astype(float)
        df_q_1 = df_q_1.set_index('date')
        df_q_1.fillna(0)
        np4 = df_q_1.loc['2020/09']['np']+df_q_1.loc['2020/06']['np'] + df_q_1.loc['2020/03']['np'] +df_q_1.loc['2019/12']['np']
        eps_4 = df_q_1.loc['2020/09']['eps']+df_q_1.loc['2020/06']['eps'] + df_q_1.loc['2020/03']['eps'] +df_q_1.loc['2019/12']['eps']



        df = code_list_jongmok()
        df = df.set_index('code')
        name = df.loc[code]['jongmok']
        up_jong = df.loc[code]['upjong']


        if key_ != 'nan':
            per = round(get_total_price(code)/float(key_),1)
            eps_year_rate = (float(eps_1)-float(eps_2))*100/float(eps_2)
            eps_3year_rate = (float(eps_1)-float(eps_3))*100/float(eps_3)*3
            pegr_1 = round(per/eps_year_rate,2)
            pegr_3 = round(per/eps_3year_rate,2)


            if 0 < pegr_1 < 1.0 or 0< pegr_3 <1.0 :
                acd = [code, name, up_jong, pegr_1, pegr_3, float(eps_1), float(eps_2), float(eps_3), per]
                return acd

        else:
            per = round(get_total_price(code) / np4, 1) # per 를 구함
            eps_year_rate = (eps_4 - float(eps_2)) * 100 / float(eps_2)
            eps_3year_rate = (eps_4 - float(eps_3)) * 100 / float(eps_3) * 3
            pegr_1 = round(per / eps_year_rate, 2)
            pegr_3 = round(per / eps_3year_rate, 2)

            if 0 < pegr_1 < 1.0 and 0< pegr_3 <1.0 :
                acd = [code, name, up_jong, pegr_1, pegr_3, float(eps_1), float(eps_2), float(eps_3), per]
                return acd




    except:
        print("예외 발생")
        pass









if __name__ == "__main__":
    df_ = df_1_out()
    df_qoq = df_1_out_qoq()
    df_10 = []
    for i in code_list():
        df_10.append(get_total_income(i, df_, df_qoq))
    index_1 = []
    for dx, i in enumerate(df_10):
        if i != None:
            index_1.append(i)

    ac = pd.DataFrame(index_1, columns=['code','name', 'upjong', 'pegr_1', 'pegr_3', '20년_추정eps', '19년_추정eps', '17년_추정eps', 'per'])
    ac[ac.columns[3:]] = ac[ac.columns[3:]].astype(float)
    # ac[2] = ac[2].astype(float)

    ac.to_excel('pegr1_all.xlsx')

    df = df_1_out_qoq()
    print(df)



