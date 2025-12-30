import os
import requests
import pandas as pd
import gspread
import json
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# --- 1. 사용자 설정 ---
KEYWORDS = ['브랜딩', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍', '마케팅', '컨설팅', '판로', '입점', '창업', '소상공인', '중소기업', '스타트업', '전략', '기획', '파트너', '멘토']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']
MY_INDUSTRIES = ['1169', '4440', '9999']
MY_REGION = '서울특별시'

def fetch_bids():
    all_data = []
    now = datetime.now()
    # 최근 7일치 데이터 수집
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [중요] 포털에 명시된 최신 주소 (중간에 /ad/ 포함 여부 확인)
    url = 'http://apis.data.go.kr/1230000/ad/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    # GitHub Secrets에서 가져온 서비스키
    service_key = os.environ.get('SERVICE_KEY', '')
    
    for kw in KEYWORDS:
        # 500 에러 방지를 위해 인증키를 URL에 직접 결합 (requests 자동 인코딩 회피)
        full_url = f"{url}?serviceKey={service_key}"
        
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
            # 500 에러가 잦으므로 verify=False 혹은 timeout을 충분히 줍니다.
            res = requests.get(full_url, params=params, timeout=30)
            
            if res.status_code != 200:
                print(f"[{kw}] 호출 실패: 상태코드 {res.status_code}")
                continue
                
            # 응답 데이터 파싱
            try:
                data = res.json()
            except:
                print(f"[{kw}] JSON 파싱 실패 (서버 응답 이상)")
                continue

            items = data.get('response', {}).get('body', {}).get('items', [])
            if not items:
                continue

            print(f"[{kw}] 검색됨: {len(items)}건")
            
            for item in items:
                title = item.get('bidNtceNm', '')
                inst_name = item.get('ntceInstNm', '')
                
                # [필터] 제외 키워드
                if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                
                # [필터] 지역
                region = item.get('rgstRt', '')
                is_region_ok = not region or any(r in region for r in [MY_REGION, '전국', '제한없음', '전체'])
                if not is_region_ok: continue
                
                # [필터] 업종
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
            print(f"에러 발생 ({kw}): {e}")
            
    if not all_data:
        return []
    
    # 중복 제거 (공고번호나 제목 기준)
    df = pd.DataFrame(all_data)
    df = df.drop_duplicates()
    return df.values.tolist()

def update_sheet(data):
    if not data:
        print("최종 수집된 데이터가 없습니다.")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, [
            'https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        
        # 구글 시트 제목 확인
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건의 데이터를 시트에 추가했습니다.")
    except Exception as e:
        print(f"시트 업데이트 에러: {e}")

if __name__ == "__main__":
    results = fetch_bids()
    update_sheet(results)
