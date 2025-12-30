import os
import requests
import json
import gspread
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials
import urllib.parse

# --- 1. 사용자 설정 ---
KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    now = datetime.now()
    # 최근 7일치 공고 대상
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # 최신 서버 주소 (Service05)
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    service_key = os.environ.get('SERVICE_KEY', '').strip()
    
    for kw in KEYWORDS:
        # 한글 키워드 인코딩 처리
        encoded_kw = urllib.parse.quote(kw)
        
        # 64자리 키 전용 안전 주소 조합
        full_url = (
            f"{url}?serviceKey={service_key}"
            f"&numOfRows=100&pageNo=1&inprogrsBidPblancYn=Y&type=json"
            f"&bidNtceNm={encoded_kw}"
            f"&bidNtceBgnDt={start_date}&bidNtceEndDt={end_date}"
        )
        
        try:
            res = requests.get(full_url, timeout=20)
            print(f"[{kw}] 응답 코드: {res.status_code}")
            
            if res.status_code == 200:
                data = res.json()
                header = data.get('response', {}).get('header', {})
                
                # API 내부적인 오류 메시지 확인
                if header.get('resultCode') != '00':
                    print(f"[{kw}] API 메시지: {header.get('resultMsg')}")
                    continue

                items = data.get('response', {}).get('body', {}).get('items', [])
                if items:
                    print(f"[{kw}] {len(items)}건 발견")
                    for item in items:
                        title = item.get('bidNtceNm', '')
                        # 제외 키워드 필터링
                        if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                        
                        # 지역 필터링
                        region = item.get('rgstRt', '제한없음')
                        is_region_ok = any(r in region for r in [MY_REGION, '전국', '전체', '제한없음'])
                        if not is_region_ok: continue

                        # 가격 및 데이터 정리
                        raw_price = item.get('assignAmt', '0')
                        try:
                            price = "{:,}".format(int(float(raw_price))) if raw_price else "0"
                        except:
                            price = raw_price

                        all_data.append([
                            title, 
                            item.get('ntceInstNm', ''), 
                            price, 
                            item.get('indstryTy', '정보없음'),
                            item.get('cntrctCnclsMthdNm', ''), 
                            region,
                            item.get('bidNtceDt', ''), 
                            item.get('bidNtceDtlUrl', '')
                        ])
                else:
                    print(f"[{kw}] 검색 결과 없음")
            else:
                print(f"[{kw}] 서버 에러 (500 등): {res.text}")
                
        except Exception as e:
            print(f"[{kw}] 실행 중 에러: {e}")
            
    return all_data

def update_sheet(data):
    if not data:
        print("입력할 데이터가 없습니다.")
        return
        
    try:
        # Google Sheets 인증
        creds_json = os.environ.get('GOOGLE_CREDS')
        if not creds_json:
            print("구글 인증 정보(GOOGLE_CREDS)가 없습니다.")
            return
            
        creds_dict = json.loads(creds_json)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        
        # 시트 열기 (이름이 정확해야 함)
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"✅ 성공: {len(data)}건의 데이터를 구글 시트에 기록했습니다.")
        
    except Exception as e:
        print(f"❌ 시트 업데이트 실패: {e}")

if __name__ == "__main__":
    print(f"🚀 수집 시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    collected_bids = fetch_bids()
    update_sheet(collected_bids)
