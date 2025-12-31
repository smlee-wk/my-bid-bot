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
# ✅ HTTPS로 변경 (중요)
BASE_URL = "https://apis.data.go.kr/1230000/BidPublicInfoService05/getBidPblancListInfoServcPPSSrch"

INCLUDE_KEYWORDS = ['브랜딩', '마케팅', '컨설팅', '스타트업', '소상공인', '브랜드', '리브랜딩', 'BI', 'CI', '네이밍']
EXCLUDE_KEYWORDS = ['실행', '대행', '운영', '제작']

MY_INDUSTRIES = ['1169', '4440', '9999']
ALLOWED_REGION_CODES = ['11', '00']

INQRY_DIV = os.environ.get("INQRY_DIV", "1")
DAYS_BACK = int(os.environ.get("DAYS_BACK", "2"))

NUM_OF_ROWS = int(os.environ.get("NUM_OF_ROWS", "100"))
TIMEOUT_SEC = int(os.environ.get("TIMEOUT_SEC", "20"))
MAX_RETRY = int(os.environ.get("MAX_RETRY", "3"))

SHEET_NAME = os.environ.get("SHEET_NAME", "나라장터_수집")
WORKSHEET_INDEX = int(os.environ.get("WORKSHEET_INDEX", "0"))

READ_EXISTING_PK = os.environ.get("READ_EXISTING_PK", "1") == "1"
EXISTING_PK_LOOKBACK = int(os.environ.get("EXISTING_PK_LOOKBACK", "5000"))


# ----------------------------
# Helpers
# ----------------------------
def _safe_items(payload: dict):
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
    header = payload.get("response", {}).get("header", {})
    return str(header.get("resultCode", "")).strip() in ("00", "0", "SUCCESS")


def _matches_title_rules(title: str) -> bool:
    if not any(k in title for k in INCLUDE_KEYWORDS):
        return False
    if any(x in title for x in EXCLUDE_KEYWORDS):
        return False
    return True


def _format_price(item: dict) -> str:
    raw = item.get("presmptPrce") or item.get("bdgtAmt") or item.get("assignAmt") or ""
    if raw in (None, ""):
        return ""
    try:
        return "{:,}".format(int(float(str(raw).replace(",", ""))))
    except Exception:
        return str(raw)


def _pick_field(item: dict, *candidates):
    for k in candidates:
        v = item.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def _request_with_retry(url: str, params: dict, label: str) -> requests.Response:
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
                print(f"⚠️ [{label}] HTTP {res.status_code} 재시도 {attempt}/{MAX_RETRY} ({wait}s) - {last_text}")
                time.sleep(wait)
                continue

            return res

        except Exception as e:
            last_exc = e
            wait = 2 ** (attempt - 1)
            print(f"⚠️ [{label}] 요청 예외 재시도 {attempt}/{MAX_RETRY} ({wait}s): {e}")
            time.sleep(wait)

    raise RuntimeError(f"[{label}] API 요청 실패(재시도 소진): status={last_status}, text={last_text}, exc={last_exc}")


def _call_ppssrch(service_key: str, base_params: dict) -> requests.Response:
    """
    ✅ 핵심: serviceKey를 params로 넘기지 않고 URL에 직접 붙여서(인코딩 꼬임 방지)
    _type/type도 자동 fallback.
    또한 혹시 서버가 _type/type 키 이름에 민감하면 자동 전환.
    """
    # serviceKey는 "한 번만" 인코딩된 형태로 URL에 부착
    # - 이미 %2F 같은 게 들어있어도 그대로 유지되도록 safe='%' 옵션 사용
    sk_for_url = urllib.parse.quote(service_key, safe="%")

    url_with_key = f"{BASE_URL}?serviceKey={sk_for_url}"

    # 1) _type=json
    p1 = dict(base_params)
    p1["_type"] = "json"
    res = _request_with_retry(url_with_key, p1, label="URLKey+_type")

    # 500 Unexpected이면 2) type=json
    if res.status_code >= 500 and "Unexpected" in (res.text or ""):
        p2 = dict(base_params)
        p2["type"] = "json"
        res = _request_with_retry(url_with_key, p2, label="URLKey+type")

    return res


# ----------------------------
# Main
# ----------------------------
def fetch_and_update():
    now = datetime.now()
    start_dt = (now - timedelta(days=DAYS_BACK)).strftime("%Y%m%d0000")
    end_dt = now.strftime("%Y%m%d2359")

    # ✅ serviceKey: 디코드(unquote)는 하되, URL에 붙일 때는 quote(safe='%')로 "한 번만" 인코딩
    service_key = urllib.parse.unquote(os.environ.get("SERVICE_KEY", "").strip())
    if not service_key:
        raise ValueError("SERVICE_KEY 환경변수가 비어 있습니다.")

    creds_json = os.environ.get("GOOGLE_CREDS", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDS 환경변수가 비어 있습니다.")
    creds_dict = json.loads(creds_json)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)
    sheet = client.open(SHEET_NAME).get_worksheet(WORKSHEET_INDEX)

    if not sheet.acell("A1").value:
        sheet.append_row(["pk", "title", "agency", "price", "region_cd", "industry_cd", "matched_kws", "notice_dt", "detail_url", "collected_at"])

    existing_pk = set()
    if READ_EXISTING_PK:
        last_row = sheet.row_count
        start_row = max(2, last_row - EXISTING_PK_LOOKBACK + 1)
        vals = sheet.get(f"A{start_row}:A{last_row}")
        for r in vals:
            if r and r[0]:
                existing_pk.add(r[0])

    print(f"🚀 최적화 수집 시작: {now:%Y-%m-%d %H:%M:%S} / {start_dt}~{end_dt}")
    print(f"   - 호출 방식: HTTPS + serviceKey를 URL에 직접 부착(인코딩 꼬임 방지)")
    print(f"   - 키워드 호출 유지(서버가 빈조건을 500으로 튕기는 경우 대비)")

    rows = []
    seen_pk_run = set()

    for kw in INCLUDE_KEYWORDS:
        page = 1
        total_count = None

        while True:
            base_params = {
                "numOfRows": NUM_OF_ROWS,
                "pageNo": page,
                "inqryDiv": INQRY_DIV,
                "inqryBgnDt": start_dt,
                "inqryEndDt": end_dt,
                "bidNtceNm": kw,  # 서버 안정성 위해 유지
            }

            res = _call_ppssrch(service_key, base_params)

            if res.status_code != 200:
                print(f"❌ HTTP {res.status_code} / kw={kw} : {(res.text or '')[:200]}")
                break

            payload = res.json()
            if not _is_ok(payload):
                header = payload.get("response", {}).get("header", {})
                print(f"⚠️ resultCode 비정상 / kw={kw} : {header}")
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

                # 포함/제외 최종 확정
                if not _matches_title_rules(title):
                    continue

                # 업종/지역: 응답에서 필터(필드명 변동 대비 후보 여러 개)
                ind_cd = _pick_field(item, "indstrytyCd", "indstryTyCd", "indstryTy")
                rgn_cd = _pick_field(item, "prtcptLmtRgnCd")

                if ind_cd and ind_cd not in MY_INDUSTRIES:
                    continue
                if rgn_cd and rgn_cd not in ALLOWED_REGION_CODES:
                    continue

                bid_no = str(item.get("bidNtceNo", "")).strip()
                bid_ord = str(item.get("bidNtceOrd", "")).strip()
                pk = f"{bid_no}-{bid_ord}" if (bid_no or bid_ord) else f"{title}|{item.get('ntceInstNm','')}|{item.get('bidNtceDt','')}"

                if pk in seen_pk_run:
                    continue
                seen_pk_run.add(pk)

                if READ_EXISTING_PK and pk in existing_pk:
                    continue

                matched = [k for k in INCLUDE_KEYWORDS if k in title]
                matched_kws = ",".join(matched)

                rows.append([
                    pk,
                    title,
                    item.get("ntceInstNm", ""),
                    _format_price(item),
                    rgn_cd,
                    ind_cd,
                    matched_kws,
                    item.get("bidNtceDt", ""),
                    item.get("bidNtceDtlUrl", ""),
                    now.strftime("%Y-%m-%d %H:%M:%S"),
                ])

            if total_count is not None:
                max_page = (total_count + NUM_OF_ROWS - 1) // NUM_OF_ROWS
                if page >= max_page:
                    break

            page += 1

        print(f"✅ 완료 kw={kw} / 신규후보(누적) {len(rows)}건")

    if not rows:
        print("📭 신규 데이터가 없습니다.")
        return

    sheet.append_rows(rows)
    print(f"🎉 최종 저장 완료: {len(rows)}건")


if __name__ == "__main__":
    fetch_and_update()
