import requests
from bs4 import BeautifulSoup
import pymysql
import re
import pandas as pd
from tqdm import tqdm
import time




def naver_fs_parser(code, time):
    otp_url = "https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={}".format(code)
    # 이 url로 parameter 인 id와 encparam을 추출해야 한다.
    r_tmp = requests.get(otp_url)

    # r_tmp.text.find("encparam:") #으로 확인한다.

    pattern_enc = re.compile("encparam: '(.+)'", re.IGNORECASE)# 정규식으로 pattern을 뽑아 낸다.
    pattern_id = re.compile("id: '(.+?)'", re.IGNORECASE)

    target_text = r_tmp.text
    encparam = pattern_enc.search(target_text).groups()[0]
    id_ = pattern_id.search(target_text).groups()[0]

    # freq_typ
    # A: 전체
    # Y: 연간
    # Q: 분기
    # fin_type
    # 주재무제표: 0
    # K - IFRS(별도): 3
    # K - IFRS(연결): 4
    # K - GAAP(개별): 1
    # K - GAAP(연결): 2

    payload = {}
    payload['cmp_cd'] = code
    payload['fin_typ'] = 0
    payload['freq_typ'] = time
    payload['encparam'] = encparam
    payload['id'] = id_

    head = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:67.0) Gecko/20100101 Firefox/67.0',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Content-Type': 'application/json',
        'Referer': "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?",
        'X-Requested-With': 'XMLHttpRequest'
    }

    finacial_url = "https://navercomp.wisereport.co.kr/v2/company/ajax/cF1001.aspx?" # 실제 파싱해야할 데이터의 베이스 주소이다. 여기에 파라미터를 붙인다.
    # 위에 otp_url은 파라미터를 찾기 위한 url이다.
    r = requests.get(finacial_url, params=payload, headers=head)
    financial = pd.read_html(r.text)[1]
    finalcial_trans = financial.transpose().reset_index()
    finalcial_trans.iloc[0, 1] = "기간"
    finalcial_trans.columns = finalcial_trans.loc[0]
    finalcial_trans['기간']=finalcial_trans['기간']
    finalcial_trans.drop([0], inplace=True)
    finalcial_trans.drop(['주요재무정보'], axis=1, inplace=True)
    finalcial_trans['종목코드'] = code
    # finalcial_trans.drop(finalcial_trans.columns[0], axis="columns", inplace=True)
    finalcial_trans = finalcial_trans.astype('str')

    # finalcial_trans.info()
    # finalcial_trans.dtypes
    # print(finalcial_trans.head(10))

    return finalcial_trans



############################################# 네이버 자료 데이터 베이스 넣기###############################
db_conf = {
        "host": "127.0.0.1",
        "user": 'root',
        "password": "!wjddbsrms3@",
        "database": "stock_db"
    }



def naver_fs_to_db(db_conf, df):
    con = pymysql.connect(**db_conf)
    cur = con.cursor()

    # df = df.fillna(-1)
    #     print(df.head(10))


    for date, sale, profit, profit_anounce, tax_before_pf, net_profit, net_profit_control, net_profit_nocontrol, asset,\
    debt, capital_all, capital_control, capital_nocontrol, capital, sale_cash_flow, invest_cash_flow, financing_cash_flow,\
    capex, fcf, debt_interest, profit_ratio, net_profit_ratio, roe, roa, debt_ratio, reverse_captital_ratio, eps, per, bps,\
    pbr, dps , dividend_rate, dividend_propensity, stock_num, code in zip(df[df.columns[0]], df[df.columns[1]], df[df.columns[2]],\
                                                                          df[df.columns[3]], df[df.columns[4]], df[df.columns[5]],\
                                                                          df[df.columns[6]], df[df.columns[7]], df[df.columns[8]],\
                                                                          df[df.columns[9]], df[df.columns[10]], df[df.columns[11]],\
                                                                          df[df.columns[12]], df[df.columns[13]], df[df.columns[14]],\
                                                                          df[df.columns[15]], df[df.columns[16]], df[df.columns[17]],\
                                                                          df[df.columns[18]], df[df.columns[19]], df[df.columns[20]],\
                                                                          df[df.columns[21]], df[df.columns[22]], df[df.columns[23]],\
                                                                          df[df.columns[24]], df[df.columns[25]], df[df.columns[26]],\
                                                                          df[df.columns[27]], df[df.columns[28]], df[df.columns[29]],\
                                                                          df[df.columns[30]], df[df.columns[31]], df[df.columns[32]],\
                                                                          df[df.columns[33]], df[df.columns[34]]):

        date = date.split(" ")[0]


        cur.execute(
            """
            REPLACE INTO NAVER_FS_Q (date, sale, profit, profit_anounce, tax_before_pf, net_profit, net_profit_control, net_profit_nocontrol, asset,\
            debt, capital_all, capital_control, capital_nocontrol, capital, sale_cash_flow, invest_cash_flow, financing_cash_flow,\
            capex, fcf, debt_interest, profit_ratio, net_profit_ratio, roe, roa, debt_ratio, reverse_captital_ratio, eps, per, bps,\
             pbr, dps , dividend_rate, dividend_propensity, stock_num, code) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (date, sale, profit, profit_anounce, tax_before_pf, net_profit, net_profit_control, net_profit_nocontrol, asset,\
            debt, capital_all, capital_control, capital_nocontrol, capital, sale_cash_flow, invest_cash_flow, financing_cash_flow,\
            capex, fcf, debt_interest, profit_ratio, net_profit_ratio, roe, roa, debt_ratio, reverse_captital_ratio, eps, per, bps,\
             pbr, dps , dividend_rate, dividend_propensity, stock_num, code))

    con.commit()
    con.close()

    return 0






def create_naver_table():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS NAVER_FS_Q(
    number INT(20) AUTO_INCREMENT NOT NULL,
    date VARCHAR(20),
    sale VARCHAR(20),
    profit VARCHAR(20),
    profit_anounce VARCHAR(20),
    tax_before_pf VARCHAR(20),
    net_profit VARCHAR(20),
    net_profit_control VARCHAR(20),
    net_profit_nocontrol VARCHAR(20),
    asset VARCHAR(20),
    debt VARCHAR(20),
    capital_all VARCHAR(20),
    capital_control VARCHAR(20),
    capital_nocontrol VARCHAR(20),
    capital VARCHAR(20),
    sale_cash_flow VARCHAR(20),
    invest_cash_flow VARCHAR(20),
    financing_cash_flow VARCHAR(20),
    capex VARCHAR(20),
    fcf VARCHAR(20),
    debt_interest VARCHAR(20),
    profit_ratio VARCHAR(20),
    net_profit_ratio VARCHAR(20),
    roe VARCHAR(20),
    roa VARCHAR(20),
    debt_ratio VARCHAR(20),
    reverse_captital_ratio VARCHAR(20),
    eps VARCHAR(20),
    per VARCHAR(20),
    bps VARCHAR(20),
    pbr VARCHAR(20),
    dps VARCHAR(20),
    dividend_rate VARCHAR(20),
    dividend_propensity VARCHAR(20),
    stock_num VARCHAR(20),
    code VARCHAR(10), 
    collected_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    primary key (number))
    """)

    con.commit()
    con.close()

    return 0


##TODO 데이터 프레임 칼럼 바꾸고, sql칼럼명 생성하기

def fs_parsing_to_db():
    con = pymysql.connect(**db_conf)
    cur = con.cursor()
    cur.execute("select symbol from stock_code")
    row = cur.fetchall()
    code_list = []
    for a in row:
        code_list.append(a[0])
    con.commit()
    con.close()
    # ads = len(code_list)
    # bc = code_list.index('950200')
    for num, code in tqdm(enumerate(code_list[1150:])): # 200개 이하 단위로 크롤링??
        time.sleep(1.1)
        if num % 190 == 189:
            time.sleep(200)
        try:
            df_ = naver_fs_parser(code, "Q")
        except:
            continue
        # print(df_)
        naver_fs_to_db(db_conf, df_)
        print(num,"개 완료!", code)





if __name__ == "__main__":
    # create_naver_table()
    fs_parsing_to_db()


    # print(df_.columns)
    # print(df_.info())
    # print(df_)

