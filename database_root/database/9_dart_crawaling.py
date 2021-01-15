import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re


head = {'User-Agent' : 'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36'}


###################   open dart 이용해서 공시 문서 받기 #####################
api_url = "https://opendart.fss.or.kr/api/list.json"
api_key = "ee3e3513e0fa6a9ebe0ad39ba08e8d1915c4e9ce"

code = ['264660']  #회사 코드
start_day = "20100101"   # 시작날짜
dart_type = ['A001', 'A002', 'A003']   #공시 유형(세부 유형도 조정할 수 있다.)


payload = {'crtfc_key': api_key, 'bgn_de': start_day, 'pblntf_detail_ty': dart_type, 'corp_code': code }

r = requests.get(api_url, params=payload)
get_text = json.loads(r.text)

total_df = pd.DataFrame(get_text['list'])
print(total_df.info())
print(total_df)

# print(total_df.loc[1,'rcept_no'])   # 리포트 number


########################전자공시문서 저장하기 ###############################

def download_docu(url, para, filename):
    with open(filename, "wb") as f:
        res = requests.get(url, params=para, headers=head)
        f.write(res.content)
        print(res.content)

# pdf 다운받는 경로
pdf_url = 'http://dart.fss.or.kr/pdf/download/pdf.do?'


# 팝업 뜰때 경로
popup_url = 'http://dart.fss.or.kr/pdf/download/main.do?'

# 실제 리포트 주소
main_url = 'http://dart.fss.or.kr/dsaf001/main.do?'


report_nm = total_df.loc[1, 'rcept_no']

for content in total_df.iterrows():
    print(content[1]['rcept_no'])
    url_result = requests.get(main_url, params={'rcpNo': content[1]['rcept_no']})
    pdf_download_re = re.findall(r"'(.*?)'", BeautifulSoup(url_result.text, "html.parser").find("a", href="#download")["onclick"])[1]
    # print(pdf_download_re)

    pdf_dict = {'rcp_no': content[1]['rcept_no'], 'dcm_no': pdf_download_re}
    print(pdf_dict)

    download_docu(pdf_url, pdf_dict, "E:/dart/" + content[1]['report_nm'] + ".pdf")
