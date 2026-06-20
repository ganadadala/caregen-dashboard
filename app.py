"""
케어젠 주식 일일 동향 대시보드 - 백엔드
한국투자증권(KIS) Open API를 사용해 마감 후 주가/수급 데이터를 조회합니다.

실행:
    pip install -r requirements.txt
    .env 파일에 KIS_APPKEY / KIS_APPSECRET 입력
    uvicorn app:app --reload --port 8000
    브라우저에서 http://localhost:8000 접속
"""

import base64
import html as html_lib
import json
import os
import re
import secrets
import threading
import time
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

load_dotenv()

AUTH_USER = os.getenv("AUTH_USERNAME", "caregen")
AUTH_PASS = os.getenv("AUTH_PASSWORD", "")

BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
APPKEY = os.getenv("KIS_APPKEY", "")
APPSECRET = os.getenv("KIS_APPSECRET", "")
DEFAULT_CODE = os.getenv("STOCK_CODE", "214370")  # 케어젠
# KOSDAQ 제약 업종지수 코드(선택). 설정 시 차트에 비교선이 추가됩니다. 미설정 시 종목선만 표시.
PHARM_CODE = os.getenv("KOSDAQ_PHARM_CODE", "")
TOKEN_CACHE = Path(__file__).parent / ".token_cache.json"

# OPEN DART (전자공시) - 무료 인증키. https://opendart.fss.or.kr
DART_KEY = os.getenv("DART_API_KEY", "")
DART_BASE = "https://opendart.fss.or.kr/api"
CORP_CACHE = Path(__file__).parent / ".corp_code_cache.json"

# Anthropic Claude API - 뉴스 자동 요약용. https://console.anthropic.com
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

_news_cache: dict = {"ts": 0.0, "data": None}
NEWS_CACHE_TTL = 1800  # 30분

_ohlc_cache: dict = {"ts": 0.0, "data": None, "code": ""}
OHLC_CACHE_TTL = 300  # 5분

app = FastAPI(title="케어젠 일일 동향 대시보드")


class _BasicAuth(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        if not AUTH_PASS:
            return await call_next(request)
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                user, pw = base64.b64decode(auth[6:]).decode().split(":", 1)
                if secrets.compare_digest(user, AUTH_USER) and secrets.compare_digest(pw, AUTH_PASS):
                    return await call_next(request)
            except Exception:
                pass
        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": 'Basic realm="Caregen Dashboard"'},
        )


app.add_middleware(_BasicAuth)


# ---------------------------------------------------------------------------
# 접근토큰 발급 + 캐싱
# KIS는 토큰 발급 호출에 분당 제한이 있으므로 만료 전까지 재사용한다.
# ---------------------------------------------------------------------------
def get_access_token() -> str:
    if not APPKEY or not APPSECRET:
        raise HTTPException(
            status_code=500,
            detail="KIS_APPKEY / KIS_APPSECRET 가 설정되지 않았습니다. .env 파일을 확인하세요.",
        )

    # 캐시된 토큰이 유효하면 재사용
    if TOKEN_CACHE.exists():
        try:
            cached = json.loads(TOKEN_CACHE.read_text(encoding="utf-8"))
            if cached.get("expires_at", 0) > time.time() + 60:
                return cached["access_token"]
        except (json.JSONDecodeError, KeyError):
            pass

    url = f"{BASE_URL}/oauth2/tokenP"
    body = {
        "grant_type": "client_credentials",
        "appkey": APPKEY,
        "appsecret": APPSECRET,
    }
    res = requests.post(url, json=body, timeout=10)
    if res.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"토큰 발급 실패 ({res.status_code}): {res.text}",
        )
    data = res.json()
    token = data["access_token"]
    # expires_in(초) 기반 만료시각 저장. 보통 약 24시간.
    expires_at = time.time() + int(data.get("expires_in", 86400))
    TOKEN_CACHE.write_text(
        json.dumps({"access_token": token, "expires_at": expires_at}),
        encoding="utf-8",
    )
    return token


def _headers(tr_id: str) -> dict:
    return {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {get_access_token()}",
        "appkey": APPKEY,
        "appsecret": APPSECRET,
        "tr_id": tr_id,
        "custtype": "P",  # 개인. 법인은 B
    }


# ---------------------------------------------------------------------------
# KIS 초당 호출 제한(EGW00201) 회피용 전역 throttle
# 모든 KIS 호출 사이에 최소 간격(MIN_INTERVAL)을 강제한다.
# ---------------------------------------------------------------------------
MIN_INTERVAL = 0.5  # 초 (≈ 2회/초)
_kis_lock = threading.Lock()
_last_call = [0.0]


def _throttle():
    with _kis_lock:
        wait = MIN_INTERVAL - (time.time() - _last_call[0])
        if wait > 0:
            time.sleep(wait)
        _last_call[0] = time.time()


def _get(path: str, tr_id: str, params: dict, retry: int = 4) -> dict:
    """KIS GET 호출. 매 호출 전 throttle, 초당 제한 시 백오프 재시도."""
    url = f"{BASE_URL}{path}"
    last_detail = ""
    for attempt in range(retry + 1):
        _throttle()
        res = requests.get(url, headers=_headers(tr_id), params=params, timeout=10)
        body = res.text
        # 정상 응답
        if res.status_code == 200:
            data = res.json()
            if data.get("rt_cd") in (None, "0"):
                return data
        # 초당 제한이면 더 길게 쉬었다 재시도 (200/500 어느쪽으로 와도 처리)
        if "EGW00201" in body or "초당" in body or "거래건수" in body:
            last_detail = "초당 호출 제한(EGW00201)"
            if attempt < retry:
                time.sleep(1.0 * (attempt + 1))
                continue
            raise HTTPException(status_code=429, detail="KIS 초당 호출 제한이 반복됩니다. 잠시 후 다시 시도하세요.")
        # 그 외 오류
        if res.status_code == 200:
            raise HTTPException(status_code=502, detail=f"KIS 오류: {res.json().get('msg1', body[:150])}")
        last_detail = f"{res.status_code}: {body[:150]}"
        if attempt < retry:
            time.sleep(0.6 * (attempt + 1))
            continue
        raise HTTPException(status_code=502, detail=f"KIS 호출 실패 ({last_detail})")
    raise HTTPException(status_code=502, detail=f"KIS 호출 실패 ({last_detail})")


# ---------------------------------------------------------------------------
# 국내주식 현재가 시세  (TR: FHKST01010100)
# ---------------------------------------------------------------------------
def fetch_quote(code: str, div: str = "J") -> dict:
    # div: "J"=KRX, "NX"=NXT, "UN"=통합(KRX+NXT)
    data = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "FHKST01010100",
        {"FID_COND_MRKT_DIV_CODE": div, "FID_INPUT_ISCD": code},
    )
    return data.get("output", {})


# ---------------------------------------------------------------------------
# 국내주식 투자자별 매매동향  (TR: FHKST01010900)
# 당일 데이터는 장 종료 후 제공된다. output 리스트의 첫 행이 가장 최근일.
# ---------------------------------------------------------------------------
def fetch_investor(code: str) -> dict:
    data = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-investor",
        "FHKST01010900",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
    )
    rows = data.get("output", [])
    return rows[0] if rows else {}


def _to_int(v, default=0):
    try:
        return int(float(str(v).replace(",", "")))
    except (ValueError, TypeError):
        return default


def _to_float(v, default=0.0):
    try:
        return float(str(v).replace(",", ""))
    except (ValueError, TypeError):
        return default


def fetch_day_ohlc(code: str, end_ymd: str, div: str = "J") -> list:
    """선택 날짜(end_ymd, YYYYMMDD)까지의 일별 OHLC를 가져온다(날짜 오름차순).
    마지막 행 = 선택일(또는 그 이전 최근 거래일), 그 앞 행 = 전일.
    div: "J"=KRX, "NX"=NXT."""
    from datetime import datetime, timedelta

    end = datetime.strptime(end_ymd, "%Y%m%d")
    start = end - timedelta(days=20)
    data = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        "FHKST03010100",
        {
            "FID_COND_MRKT_DIV_CODE": div,
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": end_ymd,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
    )
    rows = [r for r in (data.get("output2") or []) if r.get("stck_bsop_date")]
    rows.sort(key=lambda r: r["stck_bsop_date"])
    return rows


@app.get("/api/dashboard")
def dashboard(code: str = DEFAULT_CODE, date: str = ""):
    from datetime import datetime

    quote = fetch_quote(code)          # 현재 스냅샷(시총·52주·외국인비율은 항상 '현재값')
    investor = fetch_investor(code)

    today = datetime.now().strftime("%Y-%m-%d")
    use_daily = bool(date) and date < today  # 과거 날짜면 일별시세에서 그 날짜 종가 사용

    if use_daily:
        rows = fetch_day_ohlc(code, date.replace("-", ""))
    else:
        rows = []

    if rows:
        cur = rows[-1]
        prev = rows[-2] if len(rows) >= 2 else None
        price = _to_int(cur.get("stck_clpr"))
        prev_close = _to_int(prev.get("stck_clpr")) if prev else price
        diff = price - prev_close
        rate = round((diff / prev_close * 100), 2) if prev_close else 0.0
        open_ = _to_int(cur.get("stck_oprc"))
        high = _to_int(cur.get("stck_hgpr"))
        low = _to_int(cur.get("stck_lwpr"))
        volume = _to_int(cur.get("acml_vol"))
        value = _to_int(cur.get("acml_tr_pbmn"))
        price_date = cur.get("stck_bsop_date", "")
    else:
        # 당일/최신 = 실시간 현재가 스냅샷
        sign = quote.get("prdy_vrss_sign", "3")  # 1상한 2상승 3보합 4하한 5하락
        mult = -1 if sign in ("4", "5") else 1
        price = _to_int(quote.get("stck_prpr"))
        diff = _to_int(quote.get("prdy_vrss")) * mult
        rate = round(_to_float(quote.get("prdy_ctrt")) * mult, 2)
        open_ = _to_int(quote.get("stck_oprc"))
        high = _to_int(quote.get("stck_hgpr"))
        low = _to_int(quote.get("stck_lwpr"))
        volume = _to_int(quote.get("acml_vol"))
        value = _to_int(quote.get("acml_tr_pbmn"))
        price_date = ""

    # NXT(넥스트레이드) 가격 — best-effort. 실패하면 None(화면은 기존 수기값 유지)
    nxt_price = None
    try:
        if use_daily:
            nrows = fetch_day_ohlc(code, date.replace("-", ""), div="NX")
            if nrows:
                nxt_price = _to_int(nrows[-1].get("stck_clpr")) or None
        else:
            nq = fetch_quote(code, div="NX")
            nxt_price = _to_int(nq.get("stck_prpr")) or None
    except Exception:
        nxt_price = None

    result = {
        "code": code,
        "name": quote.get("rprs_mrkt_kor_name", ""),
        "sector": quote.get("bstp_kor_isnm", ""),
        "price": {
            "current": price,
            "diff": diff,
            "rate": rate,
            "open": open_,
            "high": high,
            "low": low,
            "volume": volume,                                  # 거래량(주)
            "value": value,                                    # 거래대금(원)
            "price_date": price_date,                          # 실제 적용된 거래일(YYYYMMDD)
            "nxt": nxt_price,                                  # NXT 가격(없으면 null)
            "market_cap": _to_int(quote.get("hts_avls")),      # 시가총액(억원, 현재값)
            "w52_high": _to_int(quote.get("w52_hgpr")),        # 52주 최고(현재값)
            "w52_low": _to_int(quote.get("w52_lwpr")),
            "foreign_ratio": quote.get("hts_frgn_ehrt", ""),   # 외국인 보유비율(현재값)
        },
        "investor": {
            "foreign_qty": _to_int(investor.get("frgn_ntby_qty")),
            "org_qty": _to_int(investor.get("orgn_ntby_qty")),
            "person_qty": _to_int(investor.get("prsn_ntby_qty")),
            "date": investor.get("stck_bsop_date", ""),
        },
    }
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# 국내주식 기간별시세(일봉)  (TR: FHKST03010100)
# 한 번 호출에 약 100행 제한이 있어, 날짜 구간을 나눠 여러 번 호출해 합친다.
# div_code "J": 주식, "U": 업종지수
# ---------------------------------------------------------------------------
def fetch_daily_closes(code: str, div_code: str, days: int = 365) -> list:
    from datetime import datetime, timedelta

    end = datetime.now()
    start = end - timedelta(days=days)
    merged = {}  # 날짜 -> 종가 (중복 제거)
    cursor_end = end
    # 100일 캘린더 구간씩 뒤로 이동하며 수집
    while cursor_end > start:
        cursor_start = max(start, cursor_end - timedelta(days=100))
        data = _get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            {
                "FID_COND_MRKT_DIV_CODE": div_code,
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": cursor_start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": cursor_end.strftime("%Y%m%d"),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
        for row in data.get("output2", []) or []:
            d = row.get("stck_bsop_date")
            c = row.get("stck_clpr")
            if d and c:
                merged[d] = _to_float(c)
        cursor_end = cursor_start - timedelta(days=1)

    return [{"date": d, "close": merged[d]} for d in sorted(merged.keys())]


@app.get("/api/chart")
def chart(code: str = DEFAULT_CODE, days: int = 365):
    series = {"stock": fetch_daily_closes(code, "J", days), "pharm": None}
    if PHARM_CODE:
        try:
            series["pharm"] = fetch_daily_closes(PHARM_CODE, "U", days)
        except Exception:
            series["pharm"] = None  # 지수 코드/권한 문제 시 종목선만 표시
    return JSONResponse(series)


@app.get("/api/ohlc")
def ohlc_data(code: str = DEFAULT_CODE, date: str = ""):
    from datetime import datetime
    end_ymd = date.replace("-", "") if date else datetime.now().strftime("%Y%m%d")
    rows = fetch_day_ohlc(code, end_ymd)
    rows = rows[-5:]  # 최근 5 거래일 일봉
    result = [
        {
            "date": r.get("stck_bsop_date", ""),
            "o": _to_int(r.get("stck_oprc")),
            "h": _to_int(r.get("stck_hgpr")),
            "l": _to_int(r.get("stck_lwpr")),
            "c": _to_int(r.get("stck_clpr")),
            "v": _to_int(r.get("acml_vol")),
        }
        for r in rows
    ]
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# OPEN DART (전자공시) — 공시 목록 자동 조회
# 1) 종목코드 → 회사 고유번호(corp_code) 매핑 (corpCode.zip 최초 1회 다운로드·캐싱)
# 2) list.json 으로 최근 공시 조회
# ---------------------------------------------------------------------------
def _load_corp_map() -> dict:
    if CORP_CACHE.exists():
        try:
            return json.loads(CORP_CACHE.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    return {}


def get_corp_code(stock_code: str):
    cache = _load_corp_map()
    if stock_code in cache:
        return cache[stock_code]
    if not DART_KEY:
        raise HTTPException(status_code=500, detail="DART_API_KEY 가 설정되지 않았습니다. .env 를 확인하세요.")

    import io
    import zipfile
    import xml.etree.ElementTree as ET

    res = requests.get(f"{DART_BASE}/corpCode.xml", params={"crtfc_key": DART_KEY}, timeout=30)
    if res.status_code != 200:
        raise HTTPException(status_code=502, detail=f"DART corpCode 다운로드 실패 ({res.status_code})")
    try:
        zf = zipfile.ZipFile(io.BytesIO(res.content))
        xml_bytes = zf.read(zf.namelist()[0])
    except (zipfile.BadZipFile, IndexError):
        # 키 오류 등으로 zip 대신 에러 메시지가 온 경우
        raise HTTPException(status_code=502, detail=f"DART 응답 오류: {res.text[:200]}")

    root = ET.fromstring(xml_bytes)
    new_map = {}
    for node in root.iter("list"):
        sc = (node.findtext("stock_code") or "").strip()
        cc = (node.findtext("corp_code") or "").strip()
        if sc and cc:  # 상장사만(종목코드 있는 경우)
            new_map[sc] = cc
    CORP_CACHE.write_text(json.dumps(new_map), encoding="utf-8")
    return new_map.get(stock_code)


@app.get("/api/disclosures")
def disclosures(code: str = DEFAULT_CODE, days: int = 30, limit: int = 12):
    if not DART_KEY:
        raise HTTPException(status_code=500, detail="DART_API_KEY 미설정")
    from datetime import datetime, timedelta

    corp = get_corp_code(code)
    if not corp:
        return JSONResponse({"items": [], "msg": "corp_code 매핑 실패(종목코드 확인)"})

    end = datetime.now()
    start = end - timedelta(days=days)
    res = requests.get(
        f"{DART_BASE}/list.json",
        params={
            "crtfc_key": DART_KEY,
            "corp_code": corp,
            "bgn_de": start.strftime("%Y%m%d"),
            "end_de": end.strftime("%Y%m%d"),
            "page_count": 100,
            "page_no": 1,
        },
        timeout=15,
    )
    data = res.json()
    status = data.get("status")
    if status == "013":  # 조회된 데이터 없음
        return JSONResponse({"items": []})
    if status not in ("000", None):
        raise HTTPException(status_code=502, detail=f"DART 오류({status}): {data.get('message','')}")

    items = []
    for r in (data.get("list") or [])[:limit]:
        rcp = r.get("rcept_no", "")
        items.append({
            "date": r.get("rcept_dt", ""),
            "name": r.get("report_nm", ""),
            "filer": r.get("flr_nm", ""),
            "remark": r.get("rm", ""),
            "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp}",
        })
    return JSONResponse({"items": items})


# ---------------------------------------------------------------------------
# 해외지수 (미국) — 다우/나스닥/S&P500  (TR: FHKST03030100, 시장코드 N)
# ※ KIS 앱에 '해외시세' 사용 권한이 켜져 있어야 합니다.
# 응답 필드명이 국내와 달라, 종가/일자 후보 키를 순회해 안전하게 추출한다.
# ---------------------------------------------------------------------------
US_INDICES = {"DOW": ".DJI", "NASDAQ": "COMP", "SP500": "SPX"}


def _row_close(row: dict):
    for k in ("ovrs_nmix_prpr", "stck_clpr", "clos", "ovrs_prpr", "prpr", "clpr"):
        v = row.get(k)
        if v not in (None, "", "0"):
            return _to_float(v)
    return None


def _row_date(row: dict) -> str:
    for k in ("stck_bsop_date", "bsop_date", "xymd", "dt", "bass_dt"):
        v = row.get(k)
        if v:
            return str(v)
    return ""


def fetch_us_index(symbol: str):
    from datetime import datetime, timedelta

    end = datetime.now()
    start = end - timedelta(days=12)
    data = _get(
        "/uapi/overseas-price/v1/quotations/inquire-daily-chartprice",
        "FHKST03030100",
        {
            "FID_COND_MRKT_DIV_CODE": "N",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": end.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": "D",
        },
    )
    rows = data.get("output2") or []
    pairs = [(_row_date(r), _row_close(r)) for r in rows]
    pairs = [(d, c) for d, c in pairs if c]
    if any(d for d, _ in pairs):
        pairs.sort(key=lambda x: x[0])  # 날짜 오름차순
    else:
        pairs = pairs[::-1]  # 날짜 없으면 최신우선 가정 → 뒤집기
    closes = [c for _, c in pairs]
    if len(closes) >= 2:
        last, prev = closes[-1], closes[-2]
        return {"value": last, "rate": round((last - prev) / prev * 100, 2)}
    if closes:
        return {"value": closes[-1], "rate": None}
    # output2가 비면 output1 요약으로 보정
    o1 = data.get("output1") or {}
    rate = o1.get("prdy_ctrt")
    if rate not in (None, ""):
        sign = o1.get("prdy_vrss_sign", "")
        r = _to_float(rate) * (-1 if sign in ("4", "5") else 1)
        return {"value": _to_float(o1.get("ovrs_nmix_prpr")), "rate": round(r, 2)}
    return None


@app.get("/api/us-indices")
def us_indices():
    out = {}
    for name, sym in US_INDICES.items():
        try:
            out[name] = fetch_us_index(sym)
        except Exception:
            out[name] = None  # 권한/심볼 문제 시 해당 지수만 None
    return JSONResponse(out)


# ---------------------------------------------------------------------------
# 뉴스 자동 요약 — Google News RSS 수집 → Claude Haiku 요약
# ---------------------------------------------------------------------------
def fetch_gnews(query: str, max_items: int = 15) -> list:
    """Google News RSS에서 헤드라인+스니펫 수집. 실패 시 빈 리스트 반환."""
    url = "https://news.google.com/rss/search"
    try:
        res = requests.get(
            url,
            params={"q": query, "hl": "ko", "gl": "KR", "ceid": "KR:ko"},
            timeout=8,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        root = ET.fromstring(res.content)
        items = []
        for item in root.findall(".//item")[:max_items]:
            t = html_lib.unescape(item.findtext("title") or "")
            if " - " in t:
                t = t.rsplit(" - ", 1)[0]
            t = t.strip()
            desc = html_lib.unescape(item.findtext("description") or "")
            desc = re.sub(r"<[^>]+>", " ", desc).strip()
            desc = re.sub(r"\s+", " ", desc)[:300]
            if t:
                items.append(f"[제목] {t}" + (f"\n[내용] {desc}" if desc else ""))
        return items
    except Exception:
        return []


@app.get("/api/news")
def news_summary():
    if not ANTHROPIC_KEY:
        raise HTTPException(
            status_code=500,
            detail="ANTHROPIC_API_KEY가 설정되지 않았습니다. Render 환경변수(또는 .env)에 추가하세요.",
        )

    # 30분 캐시
    if time.time() - _news_cache["ts"] < NEWS_CACHE_TTL and _news_cache["data"]:
        return JSONResponse(_news_cache["data"])

    macro_hl = fetch_gnews("코스닥 증시 시황 오늘")
    sector_hl = fetch_gnews("바이오 제약 신약 코스닥")
    company_hl = fetch_gnews("케어젠")

    today_str = time.strftime("%Y-%m-%d")
    macro_lines = "\n".join(macro_hl) if macro_hl else "(없음)"
    sector_lines = "\n".join(sector_hl) if sector_hl else "(없음)"
    company_lines = "\n".join(company_hl) if company_hl else "(없음)"

    prompt = (
        f"오늘({today_str}) 뉴스 헤드라인과 내용을 바탕으로 기관투자자용 한국어 브리핑을 작성하세요.\n\n"
        "작성 규칙:\n"
        "1. 각 카테고리에서 주요 이슈 2~3개를 선별해 각각 하나의 단락으로 작성\n"
        "2. 각 단락은 반드시 4~5문장으로 구성: ① 무슨 일이 있었는지(사실) ② 배경·원인 ③ 시장 반응 또는 관련 수치 ④ 투자자 관점 시사점\n"
        "3. 기업명, 수치, 정책명 등 구체적 정보를 최대한 포함\n"
        "4. 단순 헤드라인 반복 금지 — 맥락과 의미를 풀어서 설명\n"
        "5. 뉴스가 없으면 '특이사항 없음'\n\n"
        f"[증시·매크로 뉴스]\n{macro_lines}\n\n"
        f"[바이오/제약 섹터 뉴스]\n{sector_lines}\n\n"
        f"[케어젠(214370) 관련 뉴스]\n{company_lines}\n\n"
        "아래 JSON만 출력 (다른 텍스트 없이):\n"
        '{"macro":["단락1(4~5문장)","단락2(4~5문장)"],"sector":["단락1","단락2"],"company":["단락1","단락2"]}'
    )

    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 3000,
            "messages": [{"role": "user", "content": prompt}],
        },
        timeout=30,
    )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail=f"Claude API 오류 ({resp.status_code}): {resp.text[:200]}",
        )

    raw = resp.json()["content"][0]["text"].strip()
    m = re.search(r'\{[\s\S]*\}', raw)
    if not m:
        raise HTTPException(status_code=502, detail="Claude 응답에서 JSON을 찾지 못했습니다.")

    try:
        result = json.loads(m.group())
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=502, detail=f"JSON 파싱 오류: {e}")

    for key in ("macro", "sector", "company"):
        if key not in result or not isinstance(result[key], list):
            result[key] = ["데이터 없음"]

    _news_cache["ts"] = time.time()
    _news_cache["data"] = result
    return JSONResponse(result)


def _krx_short_raw(code: str):
    """KRX 공매도 잔고 원시 응답 반환 — 세션 쿠키 선획득 방식"""
    from datetime import datetime, timedelta

    today = datetime.now()
    end_dt = today - timedelta(days=2)
    start_dt = end_dt - timedelta(days=20)
    end_ymd = end_dt.strftime("%Y%m%d")
    start_ymd = start_dt.strftime("%Y%m%d")

    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9",
    })

    # 세션 쿠키 획득
    sess.get("https://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT11001.cmd", timeout=8)

    sess.headers.update({
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT11001.cmd",
    })

    # 종목 검색으로 full_code(ISIN) 확보
    isu_cd = code
    try:
        srch = sess.post(
            "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            data={"bld": "dbms/comm/finder/finder_stkisu", "mktsel": "KSQ", "searchText": code},
            timeout=8,
        )
        items = srch.json().get("block1", [])
        # short_code가 code와 정확히 일치하는 항목 선택
        matched = [i for i in items if i.get("short_code") == code]
        if matched:
            isu_cd = matched[0].get("full_code") or code
        elif items:
            isu_cd = items[0].get("full_code") or code
    except Exception:
        pass

    resp = sess.post(
        "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
        data={
            "bld": "dbms/MDC/STAT/standard/MDCSTAT11001",
            "share": "1",
            "mktId": "KSQ",
            "isuCd": isu_cd,
            "strtDd": start_ymd,
            "endDd": end_ymd,
        },
        timeout=10,
    )
    raw = resp.json()
    rows = raw.get("output") or raw.get("block1") or raw.get("OutBlock_1") or []
    return rows, end_ymd


@app.get("/api/short-balance/debug")
def short_balance_debug(code: str = DEFAULT_CODE):
    """KRX 원시 응답 디버그용"""
    from datetime import datetime, timedelta
    today = datetime.now()
    end_ymd = (today - timedelta(days=2)).strftime("%Y%m%d")
    start_ymd = (today - timedelta(days=20)).strftime("%Y%m%d")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://data.krx.co.kr/contents/MDC/STAT/standard/MDCSTAT11001.cmd",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
    }
    # 1단계: 종목 검색 원문
    try:
        srch = requests.post(
            "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            data={"bld": "dbms/comm/finder/finder_stkisu", "mktsel": "KSQ", "searchText": code},
            headers=headers, timeout=8,
        )
        srch_status = srch.status_code
        srch_text = srch.text[:300]
    except Exception as e:
        srch_status = 0
        srch_text = str(e)

    # 2단계: 공매도 잔고 원문
    try:
        resp = requests.post(
            "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd",
            data={"bld": "dbms/MDC/STAT/standard/MDCSTAT11001", "share": "1",
                  "mktId": "KSQ", "isuCd": code, "strtDd": start_ymd, "endDd": end_ymd},
            headers=headers, timeout=10,
        )
        main_status = resp.status_code
        main_text = resp.text[:500]
    except Exception as e:
        main_status = 0
        main_text = str(e)

    return JSONResponse({
        "search_status": srch_status, "search_body": srch_text,
        "main_status": main_status, "main_body": main_text,
    })


@app.get("/api/short-balance")
def short_balance(code: str = DEFAULT_CODE):
    """KRX 정보데이터시스템에서 공매도 잔고 현황 조회 (T+2)"""
    try:
        rows, end_ymd = _krx_short_raw(code)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"KRX 조회 실패: {e}")

    if not rows:
        raise HTTPException(status_code=404, detail="공매도 잔고 데이터 없음")

    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else None

    def _n(v):
        try:
            return int(str(v).replace(",", ""))
        except Exception:
            return 0

    # 가능한 필드명 순서대로 시도
    def _get_qty(row):
        for k in ("BALANCE_QTY", "BAL_QTY", "CVSRTSELL_BAL_QTY", "SHRTSELL_BAL_QTY", "balance_qty"):
            v = row.get(k)
            if v not in (None, "", "0", 0):
                return _n(v)
        return 0

    bal = _get_qty(latest)
    bal_prev = _get_qty(prev) if prev else None
    bal_diff = (bal - bal_prev) if bal_prev is not None else None

    date_raw = (latest.get("TRD_DD") or latest.get("BAS_DD") or
                latest.get("trd_dd") or latest.get("bas_dd") or end_ymd)
    date_raw = str(date_raw).replace("/", "").replace("-", "")
    date_fmt = f"{date_raw[4:6]}/{date_raw[6:8]}" if len(date_raw) == 8 else date_raw

    return JSONResponse({
        "date": date_fmt,
        "balance_qty": bal,
        "balance_diff": bal_diff,
        "raw_date": date_raw,
    })


@app.get("/api/health")
def health():
    return {"ok": True, "configured": bool(APPKEY and APPSECRET), "default_code": DEFAULT_CODE}


# 정적 대시보드 서빙 (맨 마지막에 마운트)
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
