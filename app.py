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

# 환경변수에 실수로 끼어든 양끝 공백·줄바꿈 제거 (Render 대시보드 붙여넣기 시 흔함)
AUTH_USER = os.getenv("AUTH_USERNAME", "caregen").strip()
AUTH_PASS = os.getenv("AUTH_PASSWORD", "").strip()

BASE_URL = os.getenv("KIS_BASE_URL", "https://openapi.koreainvestment.com:9443")
APPKEY = os.getenv("KIS_APPKEY", "")
APPSECRET = os.getenv("KIS_APPSECRET", "")
DEFAULT_CODE = os.getenv("STOCK_CODE", "214370")  # 케어젠
# KOSDAQ 제약 업종지수 코드. 차트에 비교선(점선)으로 표시. 기본 1024=KIS 업종 '제약'(코스닥).
PHARM_CODE = (os.getenv("KOSDAQ_PHARM_CODE", "").strip() or "1024")
# 환경변수에 실수로 종목코드가 들어간 경우(업종코드가 아님) 기본값으로 자가 교정
if PHARM_CODE == os.getenv("STOCK_CODE", "214370"):
    PHARM_CODE = "1024"
TOKEN_CACHE = Path(__file__).parent / ".token_cache.json"

# 코스닥 제약/바이오 섹터 종목군(≈ 코스닥 제약지수 구성종목 기준).
# KRX 무료 API·KIS 모두 '지수 구성종목'을 제공하지 않아, 구성종목 코드를 정적으로 두고
# 실시간 시총(KRX)으로 정렬해 top10을 뽑는다. 리밸런싱 시 env(KRX_PHARMA_CODES)로 교체.
# 코드가 상장폐지·이전상장 등으로 당일 KRX 행에 없으면 자동 제외(무해).
_PHARMA_DEFAULT = (
    "196170,028300,141080,298380,068760,214150,214450,145020,310210,087010,"  # 알테오젠·HLB·리가켐바이오·에이비엘바이오·셀트리온제약·클래시스·파마리서치·휴젤·보로노이·펩트론
    "000250,086900,214370,085660,237690,243070,950160,064550,039200,082270,"  # 삼천당제약·메디톡스·케어젠·차바이오텍·에스티팜·휴온스·코오롱티슈진·바이오니아·오스코텍·젬백스
    "183490,323990,358570,288330,206650,007390,174900,226950,294090,067080,"  # 엔지켐생명과학·박셀바이오·지아이이노베이션·브릿지바이오·유바이오로직스·네이처셀·앱클론·올릭스·이오플로우·대화제약
    "096530"                                                                     # 씨젠(분자진단)
)
PHARMA_CODES = {
    c.strip().lstrip("0")
    for c in os.getenv("KRX_PHARMA_CODES", _PHARMA_DEFAULT).split(",")
    if c.strip()
}

# OPEN DART (전자공시) - 무료 인증키. https://opendart.fss.or.kr
DART_KEY = os.getenv("DART_API_KEY", "")
DART_BASE = "https://opendart.fss.or.kr/api"
CORP_CACHE = Path(__file__).parent / ".corp_code_cache.json"

# Anthropic Claude API - 뉴스 자동 요약용. https://console.anthropic.com
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# KRX OpenAPI — 일별매매정보(전 종목 시총). 키는 Render 환경변수로만 주입.
KRX_API_KEY = os.getenv("KRX_API_KEY", "").strip()
KRX_BASE = "https://data-dbg.krx.co.kr/svc/apis/sto"

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


def fetch_investor_days(code: str, n: int = 3) -> list:
    """투자자별 매매동향 최근 n일 (당일 포함). 각 행: 종가·전일비·거래량·외국인·기관·개인 순매수."""
    data = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-investor",
        "FHKST01010900",
        {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
    )
    rows = (data.get("output") or [])[:n]  # output[0] = 가장 최근일
    out = []
    for r in rows:
        sign = r.get("prdy_vrss_sign", "3")  # 1상한 2상승 3보합 4하한 5하락
        mult = -1 if sign in ("4", "5") else 1
        out.append({
            "date": r.get("stck_bsop_date", ""),
            "close": _to_int(r.get("stck_clpr")),
            "diff": abs(_to_int(r.get("prdy_vrss"))) * mult,
            "volume": _to_int(r.get("acml_vol")),
            "foreign_qty": _to_int(r.get("frgn_ntby_qty")),
            "org_qty": _to_int(r.get("orgn_ntby_qty")),
            "person_qty": _to_int(r.get("prsn_ntby_qty")),
        })
    return out


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


# ---------------------------------------------------------------------------
# KOSDAQ 시가총액 순위  (TR: FHPST01740000)
# 순위 API는 상위 30위까지만 반환한다(페이지네이션 없음).
# 종목이 30위 밖이면 못 찾으므로 None 반환 → 화면은 기존 수기값 유지.
# (KRX 통합 순위는 케어젠이 ~140위라 상위30 응답에 안 잡혀 자동조회 불가 → 수기입력)
# ---------------------------------------------------------------------------
def fetch_kosdaq_rank(code: str) -> "int | None":
    try:
        data = _get(
            "/uapi/domestic-stock/v1/ranking/market-cap",
            "FHPST01740000",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "20174",
                "FID_DIV_CLS_CODE": "0",
                "FID_INPUT_ISCD": "1001",   # 1001 = KOSDAQ 전체
                "FID_TRGT_CLS_CODE": "0",
                "FID_TRGT_EXLS_CLS_CODE": "0",
                "FID_INPUT_PRICE_1": "",
                "FID_INPUT_PRICE_2": "",
                "FID_VOL_CNT": "",
            },
        )
        rows = data.get("output") or []
        target = code.lstrip("0")
        for r in rows:
            if r.get("mksc_shrn_iscd", "").lstrip("0") == target:
                return _to_int(r.get("data_rank")) or None
    except Exception:
        pass
    return None


# --- KRX 일별매매정보 행 필드 추출 헬퍼 (필드명이 조금씩 달라 후보키 순회) ---
def _krx_cap(row: dict) -> int:
    for k in ("MKTCAP", "MKT_CAP"):
        if k in row:
            return _to_int(row[k])
    for k, v in row.items():
        if "CAP" in k.upper():
            return _to_int(v)
    return 0


def _krx_code(row: dict) -> str:
    for k in ("ISU_SRT_CD", "ISU_CD", "SHRN_ISU_CD"):
        if k in row:
            return str(row.get(k, ""))
    return ""


def _krx_name(row: dict) -> str:
    for k in ("ISU_ABBRV", "ISU_NM", "ISU_KOR_NM", "KOR_ABBRV"):
        if row.get(k):
            return str(row.get(k))
    return ""


def _krx_close(row: dict) -> int:
    for k in ("TDD_CLSPRC", "CLSPRC", "CLPR", "TDD_CLPR"):
        if k in row:
            return _to_int(row[k])
    return 0


def _krx_chg_pct(row: dict) -> float:
    for k in ("FLUC_RT", "CMPPREVDD_RT", "FLT_RT"):
        if k in row:
            return _to_float(row[k])
    return 0.0


def _krx_top10(rows: list, target: str = "", code_set: "set | None" = None) -> list:
    """시총 내림차순 상위 10종목 → [{name, code, close, chg}]. target 종목은 표시용 플래그.
    code_set 지정 시 해당 종목코드(선행 0 제거)만 대상으로 필터."""
    pool = rows if code_set is None else [r for r in rows if _krx_code(r).lstrip("0") in code_set]
    out = []
    for row in sorted(pool, key=_krx_cap, reverse=True)[:10]:
        out.append({
            "name": _krx_name(row),
            "code": _krx_code(row).lstrip("0"),
            "close": _krx_close(row),
            "chg": round(_krx_chg_pct(row), 2),
            "self": bool(target) and _krx_code(row).lstrip("0") == target,
        })
    return out


# KRX 일별매매정보는 전 종목 조회라 무거움. dashboard(순위)·market(top10)가
# 짧은 간격으로 동일 조회를 두 번 하면 rate-limit로 두 번째가 빌 수 있어 캐시로 dedup.
_krx_market_cache: dict = {}
KRX_MARKET_TTL = 300  # 5분


def fetch_krx_market(code: str, basDd: str = "") -> dict:
    """KRX 일별매매정보 조회 결과(순위+top10) — (code, basDd)별 5분 캐시."""
    key = f"{code}:{basDd}"
    ent = _krx_market_cache.get(key)
    if ent and (time.time() - ent[0] < KRX_MARKET_TTL):
        return ent[1]
    data = _fetch_krx_market_impl(code, basDd)
    # 실제 데이터가 있었던 경우만 캐시(빈 응답은 캐시 안 해 다음 조회 때 재시도)
    if data.get("kosdaq_top") or data.get("kosdaq_rank") or data.get("kospi_top"):
        _krx_market_cache[key] = (time.time(), data)
    return data


def _fetch_krx_market_impl(code: str, basDd: str = "") -> dict:
    """KRX 일별매매정보 1회 조회로 순위 + 시총 top10을 함께 산출.
    KRX_API_KEY 미설정 시 값 없음. basDd 지정 시 그 날짜(또는 직전 거래일),
    미지정 시 KST 오늘부터 과거로 훑어 데이터 있는 첫 거래일 사용.
    """
    from datetime import datetime, timedelta, timezone

    empty = {"kosdaq_rank": None, "krx_rank": None,
             "kosdaq_top": [], "kospi_top": [], "basDd": ""}
    if not KRX_API_KEY:
        return empty

    KST = timezone(timedelta(hours=9))
    hdrs = {"AUTH_KEY": KRX_API_KEY}

    def _fetch_rows(path: str, dd: str) -> list:
        try:
            r = requests.get(f"{KRX_BASE}/{path}", headers=hdrs, params={"basDd": dd}, timeout=15)
            if r.status_code != 200:
                return []
            j = r.json()
            rows = j.get("OutBlock_1") or []
            if not rows:
                for v in j.values():
                    if isinstance(v, list):
                        rows = v
                        break
            return rows
        except Exception:
            return []

    # KST 오늘(또는 지정일)부터 하루씩 과거로 훑어 데이터 있는 첫 거래일 채택.
    start = (
        datetime.strptime(basDd, "%Y%m%d")
        if basDd
        else datetime.now(KST).replace(tzinfo=None)
    )
    ksq_rows: list = []
    stk_rows: list = []
    used_dd = ""
    d = start
    for _ in range(10):
        if d.weekday() >= 5:          # 주말 건너뜀
            d -= timedelta(days=1)
            continue
        dd = d.strftime("%Y%m%d")
        rows = _fetch_rows("ksq_bydd_trd", dd)
        if rows:
            ksq_rows = rows
            stk_rows = _fetch_rows("stk_bydd_trd", dd)
            used_dd = dd
            break
        d -= timedelta(days=1)

    if not ksq_rows:
        return empty

    target = code.lstrip("0")

    kosdaq_rank = None
    for i, row in enumerate(sorted(ksq_rows, key=_krx_cap, reverse=True), start=1):
        if _krx_code(row).lstrip("0") == target:
            kosdaq_rank = i
            break

    krx_rank = None
    for i, row in enumerate(sorted(ksq_rows + stk_rows, key=_krx_cap, reverse=True), start=1):
        if _krx_code(row).lstrip("0") == target:
            krx_rank = i
            break

    return {
        "kosdaq_rank": kosdaq_rank,
        "krx_rank": krx_rank,
        "kosdaq_top": _krx_top10(ksq_rows, target),
        "kospi_top": _krx_top10(stk_rows, target),
        "pharma_top": _krx_top10(ksq_rows + stk_rows, target, PHARMA_CODES),
        "basDd": used_dd,
    }


def fetch_krx_ranks(code: str, basDd: str = "") -> dict:
    """순위만 필요한 기존 호출부용 얇은 래퍼."""
    m = fetch_krx_market(code, basDd)
    return {"kosdaq_rank": m["kosdaq_rank"], "krx_rank": m["krx_rank"]}


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


def fetch_avg_volume(code: str, end_ymd: str, n: int = 20) -> int:
    """end_ymd(포함)까지 최근 n거래일 평균 거래량(주). 실패 시 0."""
    from datetime import datetime, timedelta
    try:
        end = datetime.strptime(end_ymd, "%Y%m%d")
    except Exception:
        return 0
    start = end - timedelta(days=n * 2 + 15)  # 주말·공휴일 감안 넉넉히
    try:
        data = _get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            "FHKST03010100",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": end_ymd,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
        )
    except Exception:
        return 0
    rows = [r for r in (data.get("output2") or []) if r.get("stck_bsop_date")]
    rows.sort(key=lambda r: r["stck_bsop_date"])
    vols = [_to_int(r.get("acml_vol")) for r in rows[-n:] if _to_int(r.get("acml_vol")) > 0]
    return round(sum(vols) / len(vols)) if vols else 0


@app.get("/api/dashboard")
def dashboard(code: str = DEFAULT_CODE, date: str = ""):
    from datetime import datetime

    quote = fetch_quote(code)          # 현재 스냅샷(시총·52주·외국인비율은 항상 '현재값')
    investor = fetch_investor(code)
    try:
        inv_all = fetch_investor_days(code, 20)   # 지분율 변화 계산용 20거래일
    except Exception:
        inv_all = []
    investor_days = inv_all[:10]                   # 표에는 최근 10거래일만

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
        diff = abs(_to_int(quote.get("prdy_vrss"))) * mult
        rate = round(abs(_to_float(quote.get("prdy_ctrt"))) * mult, 2)
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

    # KOSDAQ 순위 + KRX 통합 순위 (KRX 일별매매정보, 전 종목 시총 기반)
    _ranks: dict = {}
    try:
        basDd = date.replace("-", "") if (date and date < today) else ""
        _ranks = fetch_krx_ranks(code, basDd)
    except Exception:
        pass
    kosdaq_rank = _ranks.get("kosdaq_rank")
    krx_rank = _ranks.get("krx_rank")

    # 거래량 20일 평균 대비(%) — 실패 시 None
    vol_avg20 = None
    vol_vs_avg = None
    try:
        _end_ymd = date.replace("-", "") if use_daily else datetime.now().strftime("%Y%m%d")
        _avg = fetch_avg_volume(code, _end_ymd, 20)
        if _avg > 0:
            vol_avg20 = _avg
            vol_vs_avg = round((volume - _avg) / _avg * 100, 1)
    except Exception:
        pass

    # 외국인 지분율 N거래일 대비(%p) — 외국인 순매매 누적 ÷ 추정 상장주식수
    # (KIS가 일자별 보유비율 이력을 안 줘서 순매매로 역산; 브라우저 무관하게 서버 계산)
    frgn_ratio_delta = None
    frgn_delta_days = 0
    try:
        if not use_daily and price > 0 and inv_all:
            mcap_eok = _to_int(quote.get("hts_avls"))     # 시가총액(억원)
            shares = (mcap_eok * 1e8) / price if mcap_eok > 0 else 0  # 추정 상장주식수
            if shares > 0:
                net = sum(_to_int(r.get("foreign_qty")) for r in inv_all)  # N일 외국인 순매매 합
                frgn_ratio_delta = round(net / shares * 100, 2)
                frgn_delta_days = len(inv_all)
    except Exception:
        pass

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
            "vol_avg20": vol_avg20,                            # 최근 20거래일 평균 거래량(없으면 null)
            "vol_vs_avg": vol_vs_avg,                          # 20일 평균 대비 %(없으면 null)
            "value": value,                                    # 거래대금(원)
            "price_date": price_date,                          # 실제 적용된 거래일(YYYYMMDD)
            "nxt": nxt_price,                                  # NXT 가격(없으면 null)
            "market_cap": _to_int(quote.get("hts_avls")),      # 시가총액(억원, 현재값)
            "w52_high": _to_int(quote.get("w52_hgpr")),        # 52주 최고(현재값)
            "w52_low": _to_int(quote.get("w52_lwpr")),
            "foreign_ratio": quote.get("hts_frgn_ehrt", ""),   # 외국인 보유비율(현재값)
            "foreign_ratio_delta": frgn_ratio_delta,           # 외인 지분율 N거래일 대비(%p, 순매매 역산)
            "foreign_delta_days": frgn_delta_days,             # 위 계산에 쓰인 거래일 수
            "kosdaq_rank": kosdaq_rank,                         # KOSDAQ 시총 순위(없으면 null)
            "krx_rank": krx_rank,                               # KRX 통합 시총 순위(없으면 null)
        },
        "investor": {
            "foreign_qty": _to_int(investor.get("frgn_ntby_qty")),
            "org_qty": _to_int(investor.get("orgn_ntby_qty")),
            "person_qty": _to_int(investor.get("prsn_ntby_qty")),
            "date": investor.get("stck_bsop_date", ""),
        },
        "investor_days": investor_days,
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


# ---------------------------------------------------------------------------
# 국내 업종지수 기간별시세  (TR: FHKUP03500100)
# 업종지수는 주식용 itemchartprice가 아니라 전용 indexchartprice를 써야 한다.
# 종가 필드는 bstp_nmix_prpr(업종 지수). div "U" + 업종코드.
# ---------------------------------------------------------------------------
def fetch_index_closes(code: str, days: int = 20) -> list:
    from datetime import datetime, timedelta

    end = datetime.now()
    start = end - timedelta(days=days)
    data = _get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice",
        "FHKUP03500100",
        {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
            "FID_INPUT_DATE_2": end.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": "D",
        },
    )
    out = []
    for row in data.get("output2", []) or []:
        d = (row.get("stck_bsop_date") or row.get("bsop_date")
             or row.get("stck_bsop_ymd") or row.get("bsop_ymd"))
        c = (row.get("bstp_nmix_prpr") or row.get("stck_clpr")
             or row.get("nmix_prpr") or row.get("prpr"))
        if d and c:
            out.append({"date": d, "close": _to_float(c)})
    return sorted(out, key=lambda x: x["date"])


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
    rows = rows[-10:]  # 최근 10 거래일 일봉

    # 코스닥 제약 업종지수 같은 기간 종가 — 비교선용(실패 시 종목선만)
    pharm_map = {}
    if PHARM_CODE:
        try:
            for it in fetch_index_closes(PHARM_CODE, days=20):
                pharm_map[it["date"]] = it["close"]
        except Exception:
            pharm_map = {}

    result = [
        {
            "date": r.get("stck_bsop_date", ""),
            "o": _to_int(r.get("stck_oprc")),
            "h": _to_int(r.get("stck_hgpr")),
            "l": _to_int(r.get("stck_lwpr")),
            "c": _to_int(r.get("stck_clpr")),
            "v": _to_int(r.get("acml_vol")),
            "p": pharm_map.get(r.get("stck_bsop_date", "")),  # 제약지수 종가(없으면 null)
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
def disclosures(code: str = DEFAULT_CODE, limit: int = 12):
    if not DART_KEY:
        raise HTTPException(status_code=500, detail="DART_API_KEY 미설정")
    from datetime import datetime

    corp = get_corp_code(code)
    if not corp:
        return JSONResponse({"items": [], "msg": "corp_code 매핑 실패(종목코드 확인)"})

    start = _news_window_start()
    end = datetime.now(tz=start.tzinfo)
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
        r = abs(_to_float(rate)) * (-1 if sign in ("4", "5") else 1)
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
# 시장 데이터 — 국내지수(값+등락+10일 라인) · USD환율 · KRX 시총 top10
# ---------------------------------------------------------------------------
# KIS 국내지수 코드 (inquire-daily-indexchartprice, 시장 U)
KOSPI_CODE = os.getenv("KOSPI_INDEX_CODE", "0001").strip() or "0001"
KOSDAQ_CODE = os.getenv("KOSDAQ_INDEX_CODE", "1001").strip() or "1001"


def fetch_index_snapshot(code: str, days: int = 20, tail: int = 10) -> dict:
    """지수 현재값 + 전일대비 등락률 + 최근 tail일 종가 시리즈.
    fetch_index_closes 재사용 — 실패 시 값 없음."""
    try:
        series = fetch_index_closes(code, days=days)
    except Exception:
        series = []
    if not series:
        return {"value": None, "rate": None, "series": []}
    closes = [s["close"] for s in series]
    value = closes[-1]
    rate = round((closes[-1] - closes[-2]) / closes[-2] * 100, 2) if len(closes) >= 2 and closes[-2] else None
    return {"value": value, "rate": rate, "series": series[-tail:]}


def fetch_usdkrw() -> dict:
    """USD/KRW 환율 + 전일대비(원). KIS가 원달러 현물을 안 줘서 무료 FX 소스 사용(키 불필요).
    frankfurter 기간조회로 rate+전일대비를 함께 구하고, 실패 시 현재값만 폴백."""
    from datetime import datetime, timedelta, timezone

    # 1) frankfurter 최근 10일 범위 → 최신값·직전 영업일값으로 전일대비 산출
    try:
        kst = timezone(timedelta(hours=9))
        end = datetime.now(kst).date()
        start = end - timedelta(days=10)
        r = requests.get(
            f"https://api.frankfurter.app/{start}..{end}?from=USD&to=KRW", timeout=8
        )
        if r.status_code == 200:
            rates = r.json().get("rates") or {}
            days = sorted(rates.keys())
            if days:
                last = _to_float((rates[days[-1]] or {}).get("KRW"))
                if last:
                    diff = None
                    if len(days) >= 2:
                        prev = _to_float((rates[days[-2]] or {}).get("KRW"))
                        if prev:
                            diff = round(last - prev, 2)
                    return {"rate": round(last, 2), "diff": diff}
    except Exception:
        pass

    # 2) 폴백: 현재값만
    for url in ("https://open.er-api.com/v6/latest/USD",
                "https://api.exchangerate.host/latest?base=USD&symbols=KRW"):
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue
            krw = (r.json().get("rates") or {}).get("KRW")
            if krw:
                return {"rate": round(_to_float(krw), 2), "diff": None}
        except Exception:
            continue
    return {"rate": None, "diff": None}


@app.get("/api/market")
def market(code: str = DEFAULT_CODE, date: str = ""):
    """헤더 지수/환율 + 시장·섹터 top10 + 지수 라인차트용 통합 데이터.
    외부 호출(지수3·KRX·환율)을 병렬로 실행해 지연 최소화."""
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    today = datetime.now().strftime("%Y-%m-%d")
    basDd = date.replace("-", "") if (date and date < today) else ""
    empty_idx = {"value": None, "rate": None, "series": []}

    def _safe(fn, *a, default=None):
        try:
            return fn(*a)
        except Exception:
            return default

    with ThreadPoolExecutor(max_workers=5) as ex:
        f_kosdaq = ex.submit(_safe, fetch_index_snapshot, KOSDAQ_CODE, default=empty_idx)
        f_kospi = ex.submit(_safe, fetch_index_snapshot, KOSPI_CODE, default=empty_idx)
        f_pharm = ex.submit(_safe, fetch_index_snapshot, PHARM_CODE, default=empty_idx) if PHARM_CODE else None
        f_krx = ex.submit(_safe, fetch_krx_market, code, basDd,
                          default={"kosdaq_top": [], "kospi_top": [], "pharma_top": [], "basDd": ""})
        f_fx = ex.submit(_safe, fetch_usdkrw, default={"rate": None, "diff": None})

        indices = {
            "kosdaq": f_kosdaq.result() or empty_idx,
            "kospi": f_kospi.result() or empty_idx,
            "pharm": (f_pharm.result() or empty_idx) if f_pharm else empty_idx,
        }
        km = f_krx.result() or {}
        fx = f_fx.result() or {"rate": None, "diff": None}

    return JSONResponse({
        "indices": indices,
        "fx": {"usdkrw": fx},
        "tops": {
            "kosdaq": km.get("kosdaq_top", []),
            "kospi": km.get("kospi_top", []),
            "pharma": km.get("pharma_top", []),   # 코스닥 제약지수 구성종목군 중 시총 top10
        },
        "basDd": km.get("basDd", ""),
    })


# ---------------------------------------------------------------------------
# 뉴스 자동 요약 — Google News RSS 수집 → Claude Haiku 요약
# ---------------------------------------------------------------------------

# 개인 블로그·저품질 매체 소스 키워드 차단 목록
_BLOCKED_SOURCE_KW = [
    "블로그", "blog", "tistory", "티스토리", "브런치", "velog",
    "카페", "cafe", "네이버 포스트", "naver post", "인플루언서",
    "daum cafe", "다음 카페",
]


def _news_window_start() -> "datetime":
    """한국 영업일 기준 뉴스 수집 시작 시각(KST) 반환.

    화~금 : 전일 오후 4시
    월    : 직전 금요일 오후 4시
    공휴일 연속 시 : 연휴 직전 마지막 영업일 오후 4시
    """
    import holidays as kor_holidays
    from datetime import datetime, timedelta, timezone, date

    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    today = now_kst.date()
    kr_holidays = kor_holidays.KR(years=[today.year - 1, today.year, today.year + 1])

    def is_biz_day(d: date) -> bool:
        return d.weekday() < 5 and d not in kr_holidays  # 월~금 & 비공휴일

    # 오늘 포함하지 않고 가장 최근 영업일을 찾아 거슬러 올라감
    cursor = today - timedelta(days=1)
    while not is_biz_day(cursor):
        cursor -= timedelta(days=1)

    return datetime(cursor.year, cursor.month, cursor.day, 16, 0, 0, tzinfo=kst)

def _is_good_source(source: str) -> bool:
    sl = source.lower()
    return not any(kw in sl for kw in _BLOCKED_SOURCE_KW)


def fetch_gnews(query: str, max_items: int = 15, after_dt=None) -> list:
    """Google News RSS에서 헤드라인+스니펫 수집. 실패 시 빈 리스트 반환.
    각 항목: {"text": "[제목]...", "source": "매체명", "date": "YYYY-MM-DD", "pub_dt": datetime}
    after_dt: timezone-aware datetime — 이 시각 이후 기사만 포함 (pubDate 파싱 실패 시 포함)
    """
    from email.utils import parsedate_to_datetime
    from datetime import datetime, timezone, timedelta

    url = "https://news.google.com/rss/search"
    kst = timezone(timedelta(hours=9))
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
            source = ""
            if " - " in t:
                source = t.rsplit(" - ", 1)[1].strip()
                t = t.rsplit(" - ", 1)[0]
            t = t.strip()

            # 저품질 소스 제외
            if source and not _is_good_source(source):
                continue

            desc = html_lib.unescape(item.findtext("description") or "")
            desc = re.sub(r"<[^>]+>", " ", desc).strip()
            desc = re.sub(r"\s+", " ", desc)[:300]

            pub_dt = None
            date_str = ""
            time_str = ""
            pub_raw = item.findtext("pubDate") or ""
            if pub_raw:
                try:
                    pub_dt = parsedate_to_datetime(pub_raw).astimezone(kst)
                    date_str = pub_dt.strftime("%Y-%m-%d")
                    time_str = pub_dt.strftime("%H:%M")
                except Exception:
                    pass

            # 시간 범위 필터: pubDate 파싱 성공했을 때만 적용 (실패 시 포함)
            if after_dt and pub_dt and pub_dt < after_dt:
                continue

            link = (item.findtext("link") or "").strip()

            if t:
                items.append({
                    "text": f"[제목] {t}" + (f"\n[내용] {desc}" if desc else ""),
                    "source": source,
                    "date": date_str,
                    "time": time_str,
                    "link": link,
                })
        return items
    except Exception:
        return []


@app.get("/api/news")
def news_summary(force: bool = False, px: str = "", rate: str = "",
                 kospi: str = "", kosdaq: str = "", pharm: str = ""):
    if not ANTHROPIC_KEY:
        raise HTTPException(
            status_code=500,
            detail="ANTHROPIC_API_KEY가 설정되지 않았습니다. Render 환경변수(또는 .env)에 추가하세요.",
        )

    # 30분 캐시 — 단, 시황 수치(context)가 바뀌면 핵심요약 갱신 위해 재생성
    _ctx_sig = f"{px}|{rate}|{kospi}|{kosdaq}|{pharm}"
    if (not force and _news_cache["data"]
            and time.time() - _news_cache["ts"] < NEWS_CACHE_TTL
            and _news_cache.get("ctx") == _ctx_sig):
        return JSONResponse(_news_cache["data"])

    # 뉴스 수집 시작 시각 (한국 영업일 기준: 가장 최근 영업일 오후 4시 KST)
    win_start = _news_window_start()
    # Google News after: 파라미터용 날짜 문자열 (시작일 하루 전까지 허용해 경계값 보완)
    after_date = (win_start.date()).strftime("%Y-%m-%d")

    def _collect(queries: list, per_query: int = 5, cap: int = 15) -> list:
        """여러 쿼리를 순서대로 수집, 제목 중복 제거 후 cap개까지 반환."""
        seen, result = set(), []
        for q in queries:
            full_q = f"{q} after:{after_date}"
            for item in fetch_gnews(full_q, max_items=per_query, after_dt=win_start):
                title = item["text"].split("\n")[0]
                if title not in seen:
                    seen.add(title)
                    result.append(item)
            if len(result) >= cap:
                break
        return result

    # 매크로: 지수·거시·해외 3개 쿼리 (AND 조건)
    macro_hl = _collect([
        "코스피 코스닥",
        "금리 환율 경기",
        "미국증시 나스닥",
    ])

    # 섹터: 바이오·제약 (AND 조건)
    sector_hl = _collect([
        "바이오 제약 임상",
        "신약 의약품 허가",
    ])

    # 케어젠: "케어젠"이 제목/본문에 실제 포함된 기사만 (느슨한 연관 기사 배제)
    def _is_caregen(item) -> bool:
        return "케어젠" in item["text"]

    company_hl = [it for it in _collect(["케어젠", "케어젠 214370"], per_query=10) if _is_caregen(it)]
    if not company_hl:
        seen_cg: set = set()
        for q in ["케어젠", "케어젠 214370"]:
            for item in fetch_gnews(q, max_items=10):
                if not _is_caregen(item):
                    continue
                title = item["text"].split("\n")[0]
                if title not in seen_cg:
                    seen_cg.add(title)
                    company_hl.append(item)
            if len(company_hl) >= 10:
                break

    today_str = time.strftime("%Y-%m-%d")
    macro_lines = "\n".join(i["text"] for i in macro_hl) if macro_hl else "(없음)"
    sector_lines = "\n".join(i["text"] for i in sector_hl) if sector_hl else "(없음)"
    company_lines = "\n".join(i["text"] for i in company_hl) if company_hl else "(없음)"

    # 주요 뉴스 헤드라인(원본) — 케어젠 > 섹터 > 매크로 순, 제목 중복 제거 후 최대 8건
    def _title_of(item) -> str:
        return item["text"].split("\n")[0].replace("[제목]", "").strip()

    headlines = []
    _seen_h: set = set()
    for it in company_hl + sector_hl + macro_hl:
        title = _title_of(it)
        if title and title not in _seen_h:
            _seen_h.add(title)
            headlines.append({
                "title": title, "source": it.get("source", ""),
                "date": it.get("date", ""), "time": it.get("time", ""),
                "link": it.get("link", ""),
            })
        if len(headlines) >= 8:
            break

    # 프론트가 조회로 확보한 시황 수치(선택) — 핵심요약을 수치에 근거해 작성
    _ctx = []
    if px:
        _ctx.append(f"케어젠 종가 {px}원" + (f" (전일대비 {rate}%)" if rate else ""))
    if kospi:
        _ctx.append(f"코스피 {kospi}%")
    if kosdaq:
        _ctx.append(f"코스닥 {kosdaq}%")
    if pharm:
        _ctx.append(f"코스닥 제약지수 {pharm}%")
    market_ctx = " · ".join(_ctx) if _ctx else "(제공된 시황 수치 없음 — 뉴스 기반으로 정성 서술)"

    prompt = (
        f"오늘({today_str}) 아래 시황 수치와 뉴스를 바탕으로 기관투자자용 한국어 IR 브리핑 2개 항목을 작성하세요.\n\n"
        "작성 규칙:\n"
        "A. summary(핵심 요약): '금일 시황 핵심요약' 정확히 3개 불릿 — "
        "① 당사(케어젠) 종가·전일대비와 그 배경(수급 쏠림·섹터 이슈 등) ② 코스피 시황 ③ 코스닥 시황. "
        "각 불릿 2~4문장, [시황 데이터]의 수치를 반드시 반영하고 뉴스에서 원인·배경 근거를 찾아 서술.\n"
        "B. sec_trend(당사 및 섹터 시장 동향): 2~3개 불릿 — 당사(케어젠) 주가 배경과 "
        "바이오/제약 섹터의 금일 주요 이슈(구체 종목명·사건, 예: 임상 결과·허가·하한가 등)를 뉴스 근거로 서술. "
        "IR 내부 정보(전화 문의 등)는 지어내지 말고, 시장에서 관찰되는 사실 위주로.\n"
        "공통: 구체적 기업명·수치·정책명 포함, 단순 헤드라인 반복 금지, 단정 대신 '~로 풀이/보임' 등 완곡 표현. "
        "관련 뉴스가 없으면 수치 기반으로만 간결히.\n\n"
        f"[시황 데이터]\n{market_ctx}\n\n"
        f"[증시·매크로 뉴스]\n{macro_lines}\n\n"
        f"[바이오/제약 섹터 뉴스]\n{sector_lines}\n\n"
        f"[케어젠(214370) 관련 뉴스]\n{company_lines}\n\n"
        "아래 JSON만 출력 (다른 텍스트 없이):\n"
        '{"summary":["당사 불릿","코스피 불릿","코스닥 불릿"],"sec_trend":["불릿1","불릿2"]}'
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

    for key in ("summary", "sec_trend"):
        if key not in result or not isinstance(result[key], list):
            result[key] = ["데이터 없음"]

    result["headlines"] = headlines

    from datetime import datetime, timezone, timedelta
    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    result["window"] = {
        "start": win_start.strftime("%m/%d %H:%M"),
        "end": now_kst.strftime("%m/%d %H:%M"),
    }

    _news_cache["ts"] = time.time()
    _news_cache["data"] = result
    _news_cache["ctx"] = _ctx_sig
    return JSONResponse(result)



@app.get("/api/health")
def health():
    return {"ok": True, "configured": bool(APPKEY and APPSECRET), "default_code": DEFAULT_CODE}


# 정적 대시보드 서빙 (맨 마지막에 마운트)
app.mount("/", StaticFiles(directory=Path(__file__).parent / "static", html=True), name="static")
