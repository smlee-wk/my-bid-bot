import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. 사용자 설정 ---
KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인']
EXCLUDE_KEYWORDS = ['실행', '대행'] 
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    now = datetime.now()
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [주소 수정] 404 에러 시 아래 주소 중 하나가 정답입니다. 
    # 포털 공식 문서의 '요청주소' 체계를 가장 정확하게 반영한 주소입니다.
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    service_key = os.environ.get('SERVICE_KEY', '')
    
    for kw in KEYWORDS:
        # 404 방지: params에 모든 걸 넣고 requests가 주소를 만들게 합니다.
        params = {
            'serviceKey': requests.utils.unquote(service_key), # 키 디코딩 처리
            'numOfRows': '100',
            'pageNo': '1',
            'inprogrsBidPblancYn': 'Y',
            'bidNtceNm': kw,
            'bidNtceBgnDt': start_date,
            'bidNtceEndDt': end_date,
            'type': 'json'
        }
        
        try:
            # 404가 뜨면 주소 자체가 틀린 것임
            res = requests.get(url, params=params, timeout=20)
            print(f"[{kw}] 요청 주소 확인: {res.url}") # 로그에서 실제 생성된 주소 확인용
            
            if res.status_code == 404:
                print(f"[{kw}] 404 에러: 주소를 찾을 수 없습니다. URL 설정을 확인하세요.")
                continue
            
            if res.status_code == 200:
                data = res.json()
                items = data.get('response', {}).get('body', {}).get('items', [])
                if items:
                    print(f"[{kw}] {len(items)}건 발견")
                    # (이하 필터링 로직 동일...)
                    for item in items:
                        title = item.get('bidNtceNm', '')
                        if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                        all_data.append([title, item.get('ntceInstNm', ''), '0', '정보없음', '계약', '지역', item.get('bidNtceDt', ''), item.get('bidNtceDtlUrl', '')])
            else:
                print(f"[{kw}] 에러 발생: 상태코드 {res.status_code}")
                
        except Exception as e:
            print(f"[{kw}] 예외 발생: {e}")
            
    return all_data

# (update_sheet 함수 등 나머지 코드는 동일)
