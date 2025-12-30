import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 사용자 설정 ---
KEYWORDS = ['브랜딩', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍', '마케팅', '컨설팅', '판로', '입점', '창업', '소상공인', '중소기업', '스타트업', '전략', '기획', '파트너', '멘토']
EXCLUDE_KEYWORDS = ['실행', '대행'] # 테스트를 위해 '운영', '제작' 제외해봄
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    # 테스트를 위해 7일치 수집
    time_limit = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    service_key = os.environ.get('SERVICE_KEY')
    
    for kw in KEYWORDS:
        params = {
            'serviceKey': service_key,
            'numOfRows': '100',
            'bidNtceNm': kw,
            'type': 'json'
        }
        
        try:
            res = requests.get(url, params=params, timeout=10)
            print(f"--- 키워드 '{kw}' 검색 결과 ---")
            
            if res.status_code != 200:
                print(f"API 호출 실패 (상태코드: {res.status_code})")
                continue
                
            data = res.json()
            items = data.get('response', {}).get('body', {}).get('items', [])
            
            if not items:
                print(f"검색된 공고 없음")
                continue

            print(f"검색된 공고 수: {len(items)}")
            
            for item in items:
                title = item.get('bidNtceNm', '')
                inst_name = item.get('ntceInstNm', '')
                
                # 로그 확인용 (무엇이 걸러지는지 보기 위해)
                # if any(ex in title for ex in EXCLUDE_KEYWORDS): 
                #     print(f"[제외됨 - 키워드]: {title}")
                #     continue

                # 시간 필터
                if item['bidNtceDt'] < time_limit: continue
                
                # 지역 필터 (비어있으면 통과 포함)
                region = item.get('rgstRt', '')
                is_region_ok = not region or any(r in region for r in [MY_REGION, '전국', '제한없음', '전체'])
                if not is_region_ok: continue
                
                # 업종 필터 (비어있거나 참조면 통과 포함)
                limit_yn = item.get('bidNtcePartcptnIndstryLmtYn', 'N')
                ind_name = item.get('indstryTy', '')
                is_ind_ok = (limit_yn == 'N' or not ind_name or 
                             any(word in ind_name for word in ['공고서', '참조']) or 
                             any(code in ind_name for code in MY_INDUSTRIES))
                if not is_ind_ok: continue

                # 가격 정보
                raw_price = item.get('assignAmt', '0')
                try:
                    price = "{:,}".format(int(float(raw_price))) if raw_price else "0"
                except:
                    price = raw_price

                all_data.append([
                    title, inst_name, price, 
                    ind_name if ind_name else '정보없음',
                    item.get('cntrctCnclsMthdNm', ''), region if region else '제한없음',
                    item['bidNtceDt'], item['bidNtceDtlUrl']
                ])
        except Exception as e:
            print(f"에러 발생: {e}")
            
    return pd.DataFrame(all_data).drop_duplicates().values.tolist()

def update_sheet(data):
    if not data:
        print("최종 수집된 공고가 0건입니다. (필터링에 의해 모두 제거되었을 수 있음)")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건의 공고를 시트에 추가했습니다.")
    except Exception as e:
        print(f"시트 업데이트 에러: {e}")

if __name__ == "__main__":
    bids = fetch_bids()
    update_sheet(bids)
