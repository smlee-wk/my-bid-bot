import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 사용자 설정 ---
KEYWORDS = ['브랜딩', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍', '마케팅', '컨설팅', '판로', '입점', '창업', '소상공인', '중소기업', '스타트업', '전략', '기획', '파트너', '멘토']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    # 한국 시간 기준 어제 오전 9시 이후 공고 (UTC 0시 기준 실행용)
    time_limit = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    for kw in KEYWORDS:
        params = {
            'serviceKey': os.environ.get('SERVICE_KEY'),
            'numOfRows': '100',
            'bidNtceNm': kw,
            'type': 'json'
        }
        
        try:
            res = requests.get(url, params=params).json()
            items = res['response']['body'].get('items', [])
            
            for item in items:
                title = item.get('bidNtceNm', '')
                inst_name = item.get('ntceInstNm', '')
                
                # 필터링 로직
                if item['bidNtceDt'] < time_limit: continue
                if '수의' in item.get('cntrctCnclsMthdNm', ''): continue
                if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                if not any(k in title for k in KEYWORDS): continue
                
                # 지역 필터링
                region = item.get('rgstRt', '')
                is_region_ok = any(r in region for r in [MY_REGION, '전국', '제한없음']) or not region
                if not is_region_ok: continue
                
                # 업종 필터링
                limit_yn = item.get('bidNtcePartcptnIndstryLmtYn', 'N')
                ind_name = item.get('indstryTy', '')
                is_ind_ok = (limit_yn == 'N' or any(code in ind_name for code in MY_INDUSTRIES) or any(word in ind_name for word in ['공고서', '참조']))
                if not is_ind_ok: continue

                # 가격 정보 (배정예산) 추가
                raw_price = item.get('assignAmt', '0')
                try:
                    price = "{:,}".format(int(float(raw_price))) if raw_price else "0"
                except:
                    price = raw_price

                all_data.append([
                    title, inst_name, price, 
                    ind_name if limit_yn == 'Y' else '제한없음',
                    item.get('cntrctCnclsMthdNm', ''), region if region else '제한없음',
                    item['bidNtceDt'], item['bidNtceDtlUrl']
                ])
        except Exception as e:
            print(f"Error: {e}")
            
    return pd.DataFrame(all_data).drop_duplicates().values.tolist()

def update_sheet(data):
    if not data:
        print("신규 공고 없음")
        return
    creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    client = gspread.authorize(creds)
    # 구글 시트 제목 '나라장터_수집'이 정확해야 합니다.
    sheet = client.open("나라장터_수집").get_worksheet(0)
    sheet.append_rows(data)
    print(f"{len(data)}건 추가 완료")

if __name__ == "__main__":
    bids = fetch_bids()
    update_sheet(bids)
