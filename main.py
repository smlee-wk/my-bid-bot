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
    # 최근 7일치 데이터 (형식: YYYYMMDDHHMM)
    start_date = (now - timedelta(days=7)).strftime('%Y%m%d0000')
    end_date = now.strftime('%Y%m%d2359')
    
    # [공식 문서 기준 주소]
    url = 'http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch'
    
    # GitHub Secrets에서 가져온 인증키
    service_key = os.environ.get('SERVICE_KEY', '')
    
    for kw in KEYWORDS:
        # 1. 인증키를 제외한 나머지 파라미터 설정
        params_dict = {
            'numOfRows': '100',
            'pageNo': '1',
            'inprogrsBidPblancYn': 'Y',
            'bidNtceNm': kw,
            'bidNtceBgnDt': start_date,
            'bidNtceEndDt': end_date,
            'type': 'json'
        }
        
        # 2. 주소와 인증키를 수동으로 결합 (인코딩 꼬임 방지 핵심)
        target_url = f"{url}?serviceKey={service_key}"
        
        try:
            # 3. 나머지 파라미터만 params로 넘겨서 호출
            res = requests.get(target_url, params=params_dict, timeout=20)
            
            # 로그 확인용 (URL이 제대로 만들어졌는지 확인)
            print(f"[{kw}] 호출 시도... (상태코드: {res.status_code})")
            
            if res.status_code == 200:
                try:
                    data = res.json()
                    items = data.get('response', {}).get('body', {}).get('items', [])
                    
                    if items:
                        print(f"[{kw}] {len(items)}건 검색됨")
                        for item in items:
                            title = item.get('bidNtceNm', '')
                            # [필터] 제외 키워드
                            if any(ex in title for ex in EXCLUDE_KEYWORDS): continue
                            
                            # [필터] 지역
                            region = item.get('rgstRt', '')
                            is_region_ok = not region or any(r in region for r in [MY_REGION, '전국', '제한없음', '전체'])
                            if not is_region_ok: continue
                            
                            # 데이터 정리
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
                                region if region else '제한없음',
                                item.get('bidNtceDt', ''), 
                                item.get('bidNtceDtlUrl', '')
                            ])
                    else:
                        print(f"[{kw}] 결과 없음")
                except Exception as json_e:
                    print(f"[{kw}] JSON 변환 실패: {json_e}")
            else:
                print(f"[{kw}] 서버 응답 에러: {res.status_code}")
                
        except Exception as e:
            print(f"[{kw}] 네트워크 에러: {e}")
            
    return all_data

def update_sheet(data):
    if not data:
        print("최종 수집된 공고가 없어 시트를 업데이트하지 않습니다.")
        return
    try:
        creds_dict = json.loads(os.environ.get('GOOGLE_CREDS'))
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, [
            'https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive'
        ])
        client = gspread.authorize(creds)
        
        # 구글 시트 제목 확인 필수
        sheet = client.open("나라장터_수집").get_worksheet(0)
        sheet.append_rows(data)
        print(f"성공: {len(data)}건의 공고를 시트에 추가했습니다.")
    except Exception as e:
        print(f"시트 업데이트 에러: {e}")

if __name__ == "__main__":
    bids = fetch_bids()
    update_sheet(bids)
