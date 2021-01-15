import pandas as pd
from datetime import datetime, date, timedelta
import pymysql
from selenium import webdriver
import requests
import json
from tqdm import tqdm

options = webdriver.ChromeOptions()
options.headless = True
options.add_argument("window-size=1920x1080")
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36")

driver = webdriver.Chrome(options=options)
driver.maximize_window()

head = {'Host' : 'marketdata.krx.co.kr', 'User-Agent' : 'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'}



# 터미널에서 git clone "https://github.com/FinanceData/marcap.git" marcap     를 실행..
# marcap 함수를 쓸수 있다.



####### marcap 함수로 가져온 정보와 현재 정보를 크롤링해서 써야 하는데....


db_conf = {
    "host": "127.0.0.1",
    "user": 'root',
    "password": "!wjddbsrms3@",
    "database": "stock_db"
}

############## 시가 총액 데이터 베이스 저장 #####################
# 1.Date : 날짜 (DatetimeIndex)
# 2.Code : 종목코드
# 3.Name : 종목이름
# 4.Open : 시가
# 5.High : 고가
# 6.Low : 저가
# 7.Close : 종가
# 8.Volume : 거래량
# 9.Amount : 거래대금
# 10.Changes : 전일대비
# 11.ChagesRatio : 전일비
# 12.Marcap : 시가총액(백만원)
# 13.Stocks : 상장주식수
# 14.MarcapRatio : 시가총액비중(%)
# 15.ForeignShares : 외국인 보유주식수
# 16.ForeignRatio : 외국인 지분율(%)
# 17.Rank: 시가총액 순위 (당일)

###### 테이블 만들기########################

def create_table(db_conf):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS MARKET_CAP_2(
    number INT(20) AUTO_INCREMENT NOT NULL,
    date VARCHAR(10),
    code VARCHAR(6),
    name VARCHAR(30),
    open float,
    high float,
    low float,
    close float,
    volume float,
    amount float,
    changes float,
    changes_ratio float,
    marcap_million float,
    stocks float,
    marcap_ratio float,
    foreign_shares  float,
    foreigns_ratio float,
    collected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key (number))
    """)

    con.commit()
    con.close()

    return 0

######################################### 서드파티 라이브러리를 이용해서

def insert_marcap_data(db_conf, df):

    con = pymysql.connect(**db_conf)
    cur = con.cursor()

    df = df.reset_index(drop=False)
    df['Date'] = df['Date'].astype(str)
    df = df.fillna(0)
    #     print(df.head(10))

    for date, code, name, open, high, low, close, volume, amount, changes, changesratio, marcap, stocks, marcapratio,\
        foreignshares, foreignratio in zip(df['Date'], df['Code'], df['Name'], df['Open'], df['High'], df['Low'],
                                           df['Close'], df['Volume'], df['Amount'],
                                           df['Changes'], df['ChagesRatio'], df['Marcap'], df['Stocks'],
                                           df['MarcapRatio'], df['ForeignShares'],
                                           df['ForeignRatio']):
        if len(code) > 6:
            continue
        round(marcap, 2)
        cur.execute(
            """
            REPLACE INTO MARKET_CAP (date, code, name, open, high, low, close, volume, amount, changes, changes_ratio, marcap_million, stocks, marcap_ratio, 
            foreign_shares, foreigns_ratio) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (date, code, name, open, high, low, close, volume, amount, changes, changesratio, marcap, stocks,
                  marcapratio, foreignshares, foreignratio))

    con.commit()
    con.close()

    return 0








###### 한국거래소에서(krx) 시가총액 상위하위 순서대로 가져오기 위해서...

##### selenium 으로 가져와야 하는데... 날짜를 맞추기 위해서 필요한 working_day 함수


def working_day(y, m, d):
    datelist = pd.bdate_range(end=date.today()-timedelta(1), start=datetime(y, m, d)).tolist()  #어제 날짜
    date_list = []
    for i in datelist:
        i = i.strftime("%Y%m%d")
        date_list.append(i)
    return date_list

###################krx 시가총액 정보 가져오기 함수 ###########################################
##########강의 데이터 크롤링과 저장 fastcampas 에 있음...

def mar_get_from_krx(date_):
    otp_url = "http://marketdata.krx.co.kr/contents/COM/GenerateOTP.jspx"
    #krx 화면에서 요청한 정보를 네트워크 generate OTP에서 볼수 있다.

    data = {'bld': 'MKD/04/0404/04040200/mkd04040200_01', 'name': 'form'} # 위 화면에서 밑으로 내려보면 Query String parameter 이 있다..
    otp_r = requests.get(otp_url, params=data, headers=head)
    # print(otp_url)
    # print(otp_r.text)
    market_cap = "http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx"
    payload = {'market_gubun': 'ALL',
               'sect_tp_cd': 'ALL',
               'schdate': date_,
               'pagePath': '/contents/MKD/04/0404/04040200/MKD04040200.jsp',
               'curPage': '1'}
    payload['code'] = otp_r.text

    # post_code = requests.post(market_cap, data=payload, headers=head)  #post 방식으로 요청하는데... 그걸 긁어 오려면 payload 와 market_cap 정보가 필요
    # print(post_code.text)
    rt_list= []
    for cnt in range(1, 300):
        otp_r = requests.get(otp_url, params=data, headers=head)
        payload['code'] = otp_r.text
        if(cnt %10 ==0):
            print(cnt)
        payload['curPage'] = cnt
        rt3 = requests.post(market_cap, data=payload, headers=head)
        if(len(rt3.text) > 15):
            rt_list.append(rt3.text)
        else:
            break
    total_df = pd.DataFrame()
    for x in rt_list:
        rt = json.loads(x)
        total_df = pd.concat([total_df, pd.DataFrame(rt['시가총액 상하위'])])
    if 'rn' in total_df.columns:
        del total_df['rn']

    if 'totCnt' in total_df.columns:
        del total_df['totCnt']
    total_df['date'] = date_[:4] + '-' + date_[4:6] + '-' + date_[6:8]
    # print(total_df.info())

    column = {'date': 'Date', 'isu_cd': 'Code','kor_shrt_isu_nm': 'Name', 'opnprc': 'Open', 'hgprc': 'High', 'lwprc': 'Low', 'isu_cur_pr': 'Close', 'isu_tr_vl': 'Volume', 'isu_tr_amt': 'Amount', 'prv_dd_cmpr': 'Changes', 'updn_rate': 'ChagesRatio', 'cur_pr_tot_amt': 'Marcap', 'lst_stk_vl': 'Stocks',
                  'tot_amt_per': 'MarcapRatio', 'f1': 'ForeignShares', 'f2': 'ForeignRatio'}
    total_df.fillna(0) # 결측치 처리
    total_df.rename(columns=column, inplace=True)
    # print(total_df.head(10))
    total_df.reset_index(drop=False)
    return total_df




    # print(total_df.head(10))
    # total_df.info()






# 다 자료 모으고 진행
######## 데이터 프레임 형 변환과 소수점 변환#################################

def process_df(total_df):
    total_df_sub = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Changes', 'ChagesRatio', 'Marcap', 'Stocks',
         'MarcapRatio', 'ForeignShares', 'ForeignRatio']
    # print(total_df.info())

    if 'fluc_tp_cd' in total_df.columns:
        del total_df['fluc_tp_cd']

    print(total_df.info())
    for i in total_df_sub:

       try:
           total_df[i] = total_df[i].apply(lambda x: x.replace(",", '')).apply(pd.to_numeric)
       except:
            print('예외발생')
            continue

    total_df = total_df[['Date', 'Code', 'Name', 'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 'Changes', 'ChagesRatio', 'Marcap', 'Stocks',
         'MarcapRatio', 'ForeignShares', 'ForeignRatio']]

    insert_marcap_data(db_conf, total_df)

    return 0


def crawling_marcap_for_date():
    list_date1 = working_day(2020, 7, 3)
    list_date = list_date1[14:21] ###7/3일 앞에서 부터 차례로 7일씩 크롤링 21~28까지 하면 됨 (20200722 일 크롤링 하였음)--- 7월 27일부터 해야함

    for i in tqdm(list_date):
        df = mar_get_from_krx(i)
        if df.shape[0]>2:
            process_df(df)
            print(i,"일 크롤링")


    # print(vacant_df.info())

## 데이터 베이스 삭제 코드##############################################################
def delete_df(db_conf):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""DELETE FROM market_cap WHERE date= %s""", ("2020-07-27"))

    con.commit()
    con.close()

##############################3



if __name__ == "__main__":
    # create_table(db_conf)

    # df_marcap = marcap_data('1995-05-16','2020-04-01')

    # insert_marcap_data(db_conf, df_marcap)

    # crawling_marcap_for_date()
    # date_ = working_day(2020,7,3)
    # print(date_)
    # print(date_[0:2])
    # delete_df(db_conf)

#  TODO : 휴일일 경우 처리!(해결)

# TODO : 20년 4월 2일부터 데이터 긁기.... 1주일단위??

