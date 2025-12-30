import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. 사용자 설정 ---
KEYWORDS = ['브랜딩', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍', '마케팅', '컨설팅', '판로', '입점', '창업', '소상공인', '중소기업', '스타트업', '전략', '기획', '파트너', '멘토']
EXCLUDE_KEYWORDS = ['실행', '제작']
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    # 최신 공고부터 7일치 수집 (500 에러 방지를 위해 날짜 범위 명시)
    now = datetime.now()
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [수정] 포털에 명시된 최신 End Point 주소 적용
    url = 'https://apis.data.go.kr/1230000/ad/BidPublicInfoService/getBidPblancListInfoServcPPSSrch'
    
    # ... 생략 ...
    for kw in KEYWORDS:
        # 1. 키를 안전하게 처리
        decoded_key = requests.utils.unquote(service_key)
        
        # 2. params에서 serviceKey를 제외하고 정의
        params = {
            'numOfRows': '100',
            'pageNo': '1',
            'inprogrsBidPblancYn': 'Y',
            'bidNtceNm': kw,
            'bidNtceBgnDt': start_date,
            'bidNtceEndDt': end_date,
            'type': 'json'
        }
        
        try:
            # 3. URL에 인증키를 직접 결합하여 보냄 (가장 확실한 방법)
            full_url = f"{url}?serviceKey={service_key}" # 혹은 decoded_key
            res = requests.get(full_url, params=params, timeout=20)
    
    for kw in KEYWORDS:
        params = {
            'serviceKey': requests.utils.unquote(service_key) if '%' in service_key else service_key,
            'numOfRows': '100',
            'pageNo': '1',
            'inprogrsBidPblancYn': 'Y',
            'bidNtceNm': kw,
            'bidNtceBgnDt': start_date,
            'bidNtceEndDt': end_date,
            'type': 'json'
        }
        
        try:
            # 타임아웃을 20초로 늘려 서버 응답 대기
            res = requests.get(url, params=params, timeout=20)
            
            if res.status_code != 200:
                print(f"[{kw}] 호출 실패: 상태코드 {res.status_code}")
                continue
                
            items = res.json().get('response', {}).get('body', {}).get('items', [])
            if not items: continue

            print(f"[{kw}] 검색됨: {len(items)}건")
            
            for item in items:
                title = item.get('bidNtceNm', '')
                inst_name = item.get('ntceInstNm', '')
                
                # [필터] 제외 키워드
                if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                
                # [필터] 지역 (서울 또는 전국/제한없음)
                region = item.get('rgstRt', '')
                is_region_ok = not region or any(r in region for r in [MY_REGION, '전국', '제한없음', '전체'])
                if not is_region_ok: continue
                
                # [필터] 업종 (내 코드 포함 또는 제한없음/참조)
                limit_yn = item.get('bidNtcePartcptnIndstryLmtYn', 'N')
                ind_name = item.get('indstryTy', '')
                is_ind_ok = (limit_yn == 'N' or not ind_name or 
                             any(word in ind_name for word in ['공고서', '참조']) or 
                             any(code in ind_name for code in MY_INDUSTRIES))
                if not is_ind_ok: continue

                # 가격 포맷팅
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
            print(f"에러 ({kw}): {e}")
            
    # 중복 제거 후 반환
    if not all_data: return []
    return pd.DataFrame(all_data).drop_duplicates().values.tolist()

def update_sheet(data):
    if not data:
        print("수집된 공고가 없습니다.")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
        client = gspread.authorize(creds)
        
        # 시트 이름 '나라장터_수집' 확인 필수
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건 업데이트 완료")
    except Exception as e:
        print(f"시트 업데이트 에러: {e}")

if __name__ == "__main__":
    results = fetch_bids()
    update_sheet(results)
