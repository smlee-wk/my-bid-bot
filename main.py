import os
import json
import time
import requests
import gspread
import urllib.parse
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# ----------------------------
# Config
# ----------------------------
BASE_URL = "http://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch"

# ✅ 선생님 의도(유지)
INCLUDE_KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']

# ✅ 업종 제한(4자리)
MY_INDUSTRIES = ['1169', '4440', '9999']

# ✅ 지역 범위: 서울(11) + 전국(00)
ALLOWED_REGION_CODES = ['11', '00']

# ✅ PPSSrch 날짜 파라미터
INQRY_DIV = os.environ.get("INQRY_DIV", "1")  # 1: 공고게시일시, 2: 개찰일시
DAYS_BACK = int(os.environ.get("DAYS_BACK", "2"))

# ✅ API 호출 세팅
NUM_OF_ROWS = int(os.environ.get("NUM_OF_ROWS", "100"))
TIMEOUT_SEC = int(os.environ.get("TIMEOUT_SEC", "20"))
MAX_RETRY = int(os.environ.get("MAX_RETRY", "3"))

# ✅ Google Sheets
SHEET_NAME = os.environ.get("SHEET_NAME", "나라장터_수집")
WORKSHEET_INDEX = int(os.environ.get("WORKSHEET_INDEX", "0"))

# ✅ 중복폭발 방지(옵션)
READ_EXISTING_PK = os.environ.get("READ_EXISTING_PK", "1") == "1"
EXISTING_PK_LOOKBACK = int(os.environ.get("EXISTING_PK_LOOKBACK", "5000"))


# ----------------------------
# Helpers
# ----------------------------
def _safe_items(payload: dict):
    """API 응답에서 items를 안전하게 list로 반환(단건 dict 방어)"""
    body = payload.get("response", {}).get("body", {})
    items = body.get("items", [])
    if isinstance(items, dict):
        return [items]
    return items if isinstance(items, list) else []


def _get_total_count(payload: dict) -> int:
    body = payload.get("response", {}).get("body", {})
    try:
        return int(body.get("totalCount", 0))
    except Exception:
        return 0


def _is_ok(payload: dict) -> bool:
    """200이어도 header의 resultCode 확인"""
    header = payload.get("response", {}).get("header", {})
    return str(header.get("resultCode", "")).strip() in ("00", "0", "SUCCESS")


def _matches_title_rules(title: str) -> bool:
    """포함 키워드 1개 이상 + 제외 키워드 없음"""
    if not any(k in title for k in INCLUDE_KEYWORDS):
        return False
    if any(x in title for x in EXCLUDE_KEYWORDS):
        return False
    return True


def _format_price(item: dict) -> str:
    """대표 금액: presmptPrce > bdgtAmt > assignAmt"""
    raw = item.get("presmptPrce") or item.get("bdgtAmt") or item.get("assignAmt") or ""
    if raw in (None, ""):
        return ""
    try:
        return "{:,}".format(int(float(str(raw).replace(",", ""))))
    except Exception:
        return str(raw)


def _request_with_retry(url: str, params: dict) -> requests.Response:
    """
    - 5xx면 재시도
    - 실패 원인 파악용으로 마지막 status/text/exception을 남김
    """
    last_exc = None
    last_status = None
    last_text = None

    for attempt in range(1, MAX_RETRY + 1):
        try:
            res = requests.get(url, params=params, timeout=TIMEOUT_SEC)

            if res.status_code >= 500:
                last_status = res.status_code
                last_text = (res.text or "")[:200]
                wait = 2 ** (attempt - 1)
                print(f"⚠️ HTTP {res.status_code} 재시도 {attempt}/{MAX_RETRY} ({wait}s) - {last_text}")
                time.sleep(wait)
                continue

            return res

        except Exception as e:
            last_exc = e
            wait = 2 ** (attempt - 1)
            print(f"⚠️ 요청 예외 재시도 {attempt}/{MAX_RETRY} ({wait}s): {e}")
            time.sleep(wait)

    raise RuntimeError(f"API 요청 실패(재시도 소진): status={last_status}, text={last_text}, exc={last_exc}")


# ----------------------------
# Main
# ----------------------------
def fetch_and_update():
    now = datetime.now()
    start_dt = (now - timedelta(days=DAYS_BACK)).strftime("%Y%m%d0000")
    end_dt = now.strftime("%Y%m%d2359")

    # ✅ 핵심 패치 1) SERVICE_KEY 더블 인코딩 방지: unquote
    service_key = urllib.parse.unquote(os.environ.get("SERVICE_KEY", "").strip())
    if not service_key:
        raise ValueError("SERVICE_KEY 환경변수가 비어 있습니다.")

    # Google creds
    creds_json = os.environ.get("GOOGLE_CREDS", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDS 환경변수가 비어 있습니다.")
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).get_worksheet(WORKSHEET_INDEX)

    # 헤더 없으면 추가
    if not sheet.acell("A1").value:
        sheet.append_row(["pk", "title", "agency", "price", "region_cd", "industry_cd", "matched_kws", "notice_dt", "detail_url", "collected_at"])

    # ✅ (옵션) 기존 pk 일부 로드
    existing_pk = set()
    if READ_EXISTING_PK:
        last_row = sheet.row_count
        start_row = max(2, last_row - EXISTING_PK_LOOKBACK + 1)
        rng = f"A{start_row}:A{last_row}"
        vals = sheet.get(rng)
        for row in vals:
            if row and row[0]:
                existing_pk.add(row[0])

    print(f"🚀 최적화 수집 시작: {now:%Y-%m-%d %H:%M:%S} / {start_dt}~{end_dt}")
    print(f"   - 업종 {len(MY_INDUSTRIES)} × 지역 {len(ALLOWED_REGION_CODES)} (키워드 호출 제거)")

    rows = []
    seen_pk_run = set()

    for region_cd in ALLOWED_REGION_CODES:
        for ind_cd in MY_INDUSTRIES:
            page = 1
            total_count = None

            while True:
                params = {
                    "serviceKey": service_key,
                    "type": "json",
                    "numOfRows": NUM_OF_ROWS,
                    "pageNo": page,

                    # ✅ PPSSrch 규격
                    "inqryDiv": INQRY_DIV,
                    "inqryBgnDt": start_dt,
                    "inqryEndDt": end_dt,

                    # ✅ 요청 단계 필터(업종/지역)
                    "prtcptLmtRgnCd": region_cd,
                    "indstrytyCd": ind_cd,
                }

                res = _request_with_retry(BASE_URL, params)

                if res.status_code != 200:
                    print(f"❌ HTTP {res.status_code} / ind={ind_cd} rgn={region_cd} : {(res.text or '')[:200]}")
                    break

                payload = res.json()
                if not _is_ok(payload):
                    header = payload.get("response", {}).get("header", {})
                    print(f"⚠️ resultCode 비정상 / ind={ind_cd} rgn={region_cd} : {header}")
                    break

                if total_count is None:
                    total_count = _get_total_count(payload)

                items = _safe_items(payload)
                if not items:
                    break

                for item in items:
                    title = (item.get("bidNtceNm") or "").strip()
                    if not title:
                        continue

                    # ✅ 제목에서 포함/제외 키워드 필터(최적화)
                    if not _matches_title_rules(title):
                        continue

                    bid_no = str(item.get("bidNtceNo", "")).strip()
                    bid_ord = str(item.get("bidNtceOrd", "")).strip()
                    pk = f"{bid_no}-{bid_ord}" if (bid_no or bid_ord) else f"{title}|{item.get('ntceInstNm','')}|{item.get('bidNtceDt','')}"

                    # 실행 내 중복
                    if pk in seen_pk_run:
                        continue
                    seen_pk_run.add(pk)

                    # 시트 기존 중복
                    if READ_EXISTING_PK and pk in existing_pk:
                        continue

                    matched = [k for k in INCLUDE_KEYWORDS if k in title]
                    matched_kws = ",".join(matched)

                    rows.append([
                        pk,
                        title,
                        item.get("ntceInstNm", ""),
                        _format_price(item),
                        region_cd,
                        ind_cd,
                        matched_kws,
                        item.get("bidNtceDt", ""),
                        item.get("bidNtceDtlUrl", ""),
                        now.strftime("%Y-%m-%d %H:%M:%S"),
                    ])

                # totalCount 기반 페이지 종료(정확)
                if total_count is not None:
                    max_page = (total_count + NUM_OF_ROWS - 1) // NUM_OF_ROWS
                    if page >= max_page:
                        break

                page += 1

            print(f"✅ 완료 ind={ind_cd} rgn={region_cd} / 신규후보(누적) {len(rows)}건")

    if not rows:
        print("📭 신규 데이터가 없습니다.")
        return

    sheet.append_rows(rows)
    print(f"🎉 최종 저장 완료: {len(rows)}건 (최적화 + 중복 방지 포함)")


if __name__ == "__main__":
    fetch_and_update()
