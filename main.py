import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 사용자 설정 ---
KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인']
EXCLUDE_KEYWORDS = ['실행', '대행']
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    now = datetime.now()
    # 7일 전부터 오늘까지
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [문서 기준 최신 주소]
    # 404가 난다면 이 주소가 유일한 정답입니다.
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    service_key = os.environ.get('SERVICE_KEY', '')
    
    for kw in KEYWORDS:
        # params에 넣으면 requests 라이브러리가 자동으로 주소를 올바르게 조합합니다.
        params = {
            'serviceKey': requests.utils.unquote(service_key), # 인증키 특수문자 처리
            'numOfRows': '100',
            'pageNo': '1',
            'inprogrsBidPblancYn': 'Y',
            'bidNtceNm': kw,
            'bidNtceBgnDt': start_date,
            'bidNtceEndDt': end_date,
            'type': 'json'
        }
        
        try:
            # 404 에러 방지를 위해 주소와 파라미터를 명확히 분리해서 호출
            res = requests.get(url, params=params, timeout=20)
            
            # 로그 확인용: 실제 생성된 전체 주소를 출력합니다.
            print(f"[{kw}] 호출 URL: {res.url}")
            
            if res.status_code == 200:
                try:
                    data = res.json()
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    if items:
                        print(f"[{kw}] {len(items)}건 수집됨")
                        for item in items:
                            title = item.get('bidNtceNm', '')
                            if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                            all_data.append([
                                title, item.get('ntceInstNm', ''), '가격확인필요', 
                                item.get('indstryTy', '정보없음'), item.get('cntrctCnclsMthdNm', ''),
                                item.get('rgstRt', '제한없음'), item.get('bidNtceDt', ''), item.get('bidNtceDtlUrl', '')
                            ])
                    else:
                        print(f"[{kw}] 결과 없음")
                except Exception as json_e:
                    print(f"[{kw}] JSON 응답 해석 실패: {json_e}")
            else:
                print(f"[{kw}] 실패 (상태코드: {res.status_code})")
                
        except Exception as e:
            print(f"[{kw}] 네트워크 에러: {e}")
            
    return all_data

def update_sheet(data):
    if not data:
        print("수집된 데이터가 없어 시트를 업데이트하지 않습니다.")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건 시트 기록 완료")
    except Exception as e:
        print(f"시트 에러: {e}")

if __name__ == "__main__":
    results = fetch_bids()
    update_sheet(results)
