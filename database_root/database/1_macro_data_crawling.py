import FinanceDataReader as fdr
import pymysql
from datetime import datetime
from pandas_datareader import wb
import pandas_datareader.data as web
import pandas_datareader as pdr
import datetime
import requests
import json
import pandas as pd
import openpyxl

# 	1. GDP 성장률(한국)
# 	2. GDP 성장률(미국)
# 	3. 금리(한국)(콜금리, CD금리, 국고채(3년, 10년), 대출금리
# 	4. 금리(미국)(콜금리, CD금리, 국고채(3년, 10년, 30년), 하이일드 채권금리, 장단기 금리차, TED스프레드
# 	5. 물가(생산자물가지수, 소비자물가지수, 수출입물가지수)한국
# 	6. 물가(생산자물가지수, 소비자물가지수, 수출입물가지수)미국
# 	7. 환율
# 	8. 국제수지(경상수지,자본수지,금융수지)
# 	9. 소비자동향지수
# 	10. 기업경기실사지수(BSI)
# 	11. 발틱운임지수(BDI)
# 	12. GDP대비 시가총액지수
# 	13. 외국인투자현황



db_conf = {
    "host": "127.0.0.1",
    "user": 'root',
    "password": "!wjddbsrms3@",
    "database": "macro_db"
    }
##########################################################    GDP_annual   ##############
def create_table_gdp_annual(db_conf):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS gdp_annual(
    year VARCHAR(6),
    country VARCHAR(20),
    gdp_growth VARCHAR(40),
    collected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key (year, country))
    """)
    con.commit()
    con.close()

    return 0

def insert_data(df):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()

    for year, country, gdp_growth, in zip(df["year"], df["country"], df["gdp_growth"]):
        cur.execute("""
        REPLACE INTO gdp_annual (year, country, gdp_growth) VALUES(%s,%s,%s)
        """, (year, country, gdp_growth))

    con.commit()
    con.close()

    return 0

def crawlling_gdp_annual():
    code = "NY.GDP.MKTP.KD.ZG"
    data = wb.download(indicator=code, country=['US', 'KOR'], start=1980, end=2020)
    data = data.reset_index(drop=False)
    data.rename(columns={'NY.GDP.MKTP.KD.ZG': "gdp_growth"}, inplace=True)
    data = data[['year', 'country', 'gdp_growth']]
    data["year"] = data["year"].astype(str)
    data["gdp_growth"] = data["gdp_growth"].astype(str)

    return data
################################분기별 성장률######################################################3

#미국 분기별 성장률
def create_table_gdp_quarterly(db_conf):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS gdp_quarterly(
    date VARCHAR(10),
    country VARCHAR(10),
    gdp_growth_quarterly VARCHAR(10),
    collected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key (date, country))
    """)
    con.commit()
    con.close()

    return 0

def insert_data_gdp_quarterly(df):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()

    for DATE, country, gdp_growth_quarterly, in zip(df["DATE"], df["country"], df["gdp_growth_quarterly"]):
        cur.execute("""
        REPLACE INTO gdp_quarterly (DATE, country, gdp_growth_quarterly) VALUES(%s,%s,%s)
        """, (DATE, country, gdp_growth_quarterly))

    con.commit()
    con.close()

    return 0


def crawlling_gdp_quarterly():

    start = datetime.datetime(1951, 1, 1)
    end = datetime.datetime(2020, 7, 15)
    ticker1 = 'A191RO1Q156NBEA'
    ticker2 = 'NAEXKP01KRQ657S'
    country1 = "US"
    country2 = "KOR"
    def get_gdp(ticker, country):
        pd.options.display.float_format = '{:.2f}'.format # 소숫점 이하 2자리까지만 표시.
        name = 'gdp_growth_quarterly'
        df = web.DataReader(ticker, 'fred', start, end)
        df.rename(columns={ticker: name}, inplace=True)
        df['country'] = country
        df = df.reset_index()
        df = df[["DATE", "country", "gdp_growth_quarterly"]]
        df["DATE"] = df["DATE"].astype(str)
        return df
    df = pd.concat([get_gdp(ticker1, country1), get_gdp(ticker2, country2)], ignore_index=True)
    df['gdp_growth_quarterly']=df['gdp_growth_quarterly'].round(2)
    # df.to_excel(r'C:\Users\yungu\Desktop\sample1.xlsx')
    # df1 = get_gdp(ticker2, country2)
    # print(df1['gdp_growth_quarterly'])
    return df

###########################################fred에서 인플레이션 값가져오기###############################
def macro_value(code):
    df = pdr.DataReader(code, 'fred', start='1990-01-01')
    # df.to_excel('inflation.xlsx')
    return df






### 한국 통계 사이트
def korea_gdp_growth():

    now = datetime.datetime.today()
    ap = now.strftime('%Y%m%d')
    url = "http://ecos.bok.or.kr/api/StatisticSearch/{0}/{1}/{2}/{3}/{4}/{5}/{6}/{7}/{8}/{9}"
    API = "VDIQYXGOUBWUG1ANYMEN"
    type_ = 'json'
    language = 'kr'
    start = '1' #요청문서수 시작
    end = '10000' #요청문서수 끝
    code = '111Y055' #통계코드 첫번째꺼
    period = "QQ"  # MM
    start_day = "19900101"
    end_day = ap
    type_code = "10111" #세부코드
    # urls = "http://ecos.bok.or.kr/api/StatisticSearch/VDIQYXGOUBWUG1ANYMEN/json/kr/1/10/060Y001/DD/20100101/20201010/010101000"
    r = requests.get(url.format(API, type_, language, start, end, code, period, start_day, end_day, type_code))
    # print(r.text)
    df_ = pd.DataFrame(json.loads(r.text)['StatisticSearch']['row'])
    print(df_.head(10))
    df_.to_excel(r'C:\Users\yungu\Desktop\sample.xlsx')

    # 가공해줘야 한다. 쓸데 없는 칼럼이 많다.
    # df_.drop(
    #     ["UNIT_NAME", "STAT_NAME", "ITEM_CODE1", "ITEM_CODE2", "ITEM_CODE3", "ITEM_NAME2", "ITEM_NAME3", "STAT_CODE"],
    #     axis='columns', inplace=True)
    # df = df_[["TIME", "ITEM_NAME1", "DATA_VALUE"]]


###############################################################
if __name__ == "__main__":
    #####################################333gdp 성장률(미국, 한국)년간 크롤링
    # create_table_gdp_annual(db_conf)
    # data = crawlling_gdp_annual()
    # insert_data(data)


    df_1 = macro_value('T10YIE')


    ###################################### gdp 성장률(미국, 한국)분기별 크롤링
    # create_table_gdp_quarterly(db_conf)
    # df = crawlling_gdp_quarterly()
    # insert_data_gdp_quarterly(df)

    # 한국 통계 사이트에서
    # korea_gdp_growth()