import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. 사용자 설정 ---
KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    now = datetime.now()
    # 최근 7일치 데이터
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [공식 주소] 404를 피하는 유일한 주소
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    # GitHub Secrets에서 가져온 인증키
    service_key = os.environ.get('SERVICE_KEY', '')
    
    for kw in KEYWORDS:
        # 500 에러를 피하기 위해 모든 파라미터를 URL에 직접 때려 넣습니다.
        # 이렇게 하면 requests가 키를 변환(Encoding)하지 않습니다.
        full_url = (
            f"{url}?serviceKey={service_key}"
            f"&numOfRows=100&pageNo=1&inprogrsBidPblancYn=Y&type=json"
            f"&bidNtceNm={kw}"
            f"&bidNtceBgnDt={start_date}&bidNtceEndDt={end_date}"
        )
        
        try:
            # params를 쓰지 않고 완성된 주소(full_url)로 직접 호출
            res = requests.get(full_url, timeout=20)
            
            print(f"[{kw}] 호출 시도 (상태코드: {res.status_code})")
            
            if res.status_code == 200:
                data = res.json()
                # 나라장터 API는 성공해도 내부 에러 메시지를 줄 때가 있습니다.
                header = data.get('response', {}).get('header', {})
                if header.get('resultCode') != '00':
                    print(f"[{kw}] API 내부 오류: {header.get('resultMsg')}")
                    continue

                items = data.get('response', {}).get('body', {}).get('items', [])
                
                if items:
                    print(f"[{kw}] {len(items)}건 검색됨")
                    for item in items:
                        title = item.get('bidNtceNm', '')
                        if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                        
                        region = item.get('rgstRt', '')
                        is_region_ok = not region or any(r in region for r in [MY_REGION, '전국', '제한없음', '전체'])
                        if not is_region_ok: continue

                        raw_price = item.get('assignAmt', '0')
                        try:
                            price = "{:,}".format(int(float(raw_price))) if raw_price else "0"
                        except:
                            price = raw_price

                        all_data.append([
                            title, item.get('ntceInstNm', ''), price, 
                            item.get('indstryTy', '정보없음'),
                            item.get('cntrctCnclsMthdNm', ''), region if region else '제한없음',
                            item.get('bidNtceDt', ''), item.get('bidNtceDtlUrl', '')
                        ])
                else:
                    print(f"[{kw}] 결과 없음")
            else:
                # 500 에러가 여기서 찍힌다면 키가 아직 서버에 등록 안 된 것입니다.
                print(f"[{kw}] 서버 응답 에러: {res.status_code}")
                
        except Exception as e:
            print(f"[{kw}] 에러: {e}")
            
    return all_data

def update_sheet(data):
    if not data:
        print("수집 데이터가 없습니다.")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, [
            'https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건 시트 업데이트 완료")
    except Exception as e:
        print(f"시트 에러: {e}")

if __name__ == "__main__":
    results = fetch_bids()
    update_sheet(results)
