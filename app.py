import streamlit as st
import yfinance as yf
import pandas as pd
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET

st.set_page_config(
    page_title="케어젠(214370) 주식 대시보드",
    page_icon="📊",
    layout="wide",
)

# ── CSS ──────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #f8f9fa;
        border-radius: 10px;
        padding: 16px 20px;
        border: 1px solid #e9ecef;
    }
    .metric-label { font-size: 12px; color: #6c757d; margin-bottom: 4px; }
    .metric-value { font-size: 22px; font-weight: 600; color: #212529; }
    .metric-change-up { font-size: 13px; color: #c0392b; }
    .metric-change-down { font-size: 13px; color: #2563eb; }
    .news-item {
        padding: 10px 0;
        border-bottom: 1px solid #f0f0f0;
        font-size: 14px;
    }
    .news-badge {
        display: inline-block;
        font-size: 10px;
        padding: 2px 7px;
        border-radius: 4px;
        margin-right: 6px;
        font-weight: 500;
    }
    .badge-news { background: #dbeafe; color: #1d4ed8; }
    .badge-dart { background: #fef9c3; color: #92400e; }
    .badge-pr   { background: #dcfce7; color: #166534; }
    .section-title {
        font-size: 15px;
        font-weight: 600;
        color: #374151;
        margin-bottom: 12px;
        padding-bottom: 6px;
        border-bottom: 2px solid #e5e7eb;
    }
</style>
""", unsafe_allow_html=True)

TICKER = "214370.KQ"
DART_API_KEY = st.secrets.get("DART_API_KEY", "")  # secrets.toml에 입력

# ── 데이터 로딩 ───────────────────────────────────────
@st.cache_data(ttl=60)
def load_quote():
    tk = yf.Ticker(TICKER)
    info = tk.info
    hist = tk.history(period="1d", interval="1m")
    return info, hist

@st.cache_data(ttl=300)
def load_history(period="1mo"):
    tk = yf.Ticker(TICKER)
    return tk.history(period=period)

@st.cache_data(ttl=300)
def load_dart_disclosures():
    if not DART_API_KEY:
        return []
    url = "https://opendart.fss.or.kr/api/list.xml"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": "00120030",  # 케어젠 고유번호
        "bgn_de": (datetime.now() - timedelta(days=90)).strftime("%Y%m%d"),
        "end_de": datetime.now().strftime("%Y%m%d"),
        "page_count": 10,
    }
    try:
        r = requests.get(url, params=params, timeout=5)
        root = ET.fromstring(r.content)
        items = []
        for li in root.findall(".//list"):
            items.append({
                "title": li.findtext("report_nm", ""),
                "date": li.findtext("rcept_dt", ""),
                "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={li.findtext('rcept_no','')}",
            })
        return items
    except:
        return []

@st.cache_data(ttl=300)
def load_naver_news():
    url = "https://search.naver.com/search.naver"
    params = {"where": "news", "query": "케어젠 214370", "sort": "1"}
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=5)
        # 네이버 뉴스 검색 URL 반환 (직접 파싱 대신 링크 제공)
        return r.url
    except:
        return "https://search.naver.com/search.naver?where=news&query=케어젠+214370&sort=1"

# ── 헤더 ─────────────────────────────────────────────
col_h1, col_h2 = st.columns([3, 1])
with col_h1:
    st.markdown("## 📊 케어젠 (214370) 주식 대시보드")
    st.caption(f"코스닥 · 의약품 · 마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col_h2:
    if st.button("🔄 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ── 주가 데이터 로딩 ──────────────────────────────────
with st.spinner("주가 데이터 불러오는 중..."):
    info, hist_1d = load_quote()

# ── 핵심 지표 ─────────────────────────────────────────
price       = info.get("currentPrice") or info.get("regularMarketPrice", 0)
prev_close  = info.get("previousClose", 0)
change      = round(price - prev_close, 0) if price and prev_close else 0
change_pct  = round((change / prev_close) * 100, 2) if prev_close else 0
volume      = info.get("volume") or info.get("regularMarketVolume", 0)
mkt_cap     = info.get("marketCap", 0)
high_52     = info.get("fiftyTwoWeekHigh", 0)
low_52      = info.get("fiftyTwoWeekLow", 0)
open_p      = info.get("open") or info.get("regularMarketOpen", 0)
day_high    = info.get("dayHigh") or info.get("regularMarketDayHigh", 0)
day_low     = info.get("dayLow") or info.get("regularMarketDayLow", 0)

is_up = change >= 0
arrow = "▲" if is_up else "▼"
color_cls = "metric-change-up" if is_up else "metric-change-down"

c1, c2, c3, c4, c5 = st.columns(5)

def metric_card(col, label, value, sub="", sub_class=""):
    col.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="{sub_class}">{sub}</div>
    </div>""", unsafe_allow_html=True)

metric_card(c1, "현재가",
    f"{int(price):,}원" if price else "—",
    f"{arrow} {int(abs(change)):,}원 ({change_pct:+.2f}%)",
    color_cls)
metric_card(c2, "거래량",
    f"{int(volume):,}주" if volume else "—",
    f"전일 종가 {int(prev_close):,}원" if prev_close else "")
metric_card(c3, "시가총액",
    f"{int(mkt_cap/1e8):,}억원" if mkt_cap else "—", "")
metric_card(c4, "52주 고가",
    f"{int(high_52):,}원" if high_52 else "—",
    f"현재가 대비 {round((price/high_52-1)*100,1):+.1f}%" if high_52 and price else "")
metric_card(c5, "52주 저가",
    f"{int(low_52):,}원" if low_52 else "—",
    f"현재가 대비 {round((price/low_52-1)*100,1):+.1f}%" if low_52 and price else "")

st.markdown("<br>", unsafe_allow_html=True)

# ── 추가 지표 ─────────────────────────────────────────
c6, c7, c8, c9 = st.columns(4)
metric_card(c6, "당일 시가", f"{int(open_p):,}원" if open_p else "—", "")
metric_card(c7, "당일 고가", f"{int(day_high):,}원" if day_high else "—", "")
metric_card(c8, "당일 저가", f"{int(day_low):,}원" if day_low else "—", "")
metric_card(c9, "PER", f"{info.get('trailingPE', '—'):.1f}배" if info.get('trailingPE') else "—", "")

st.markdown("<br>", unsafe_allow_html=True)
st.divider()

# ── 차트 + 수급 ───────────────────────────────────────
col_chart, col_supply = st.columns([2, 1])

with col_chart:
    st.markdown('<div class="section-title">📈 주가 차트</div>', unsafe_allow_html=True)
    period_opt = st.radio("기간", ["1주", "1개월", "3개월", "6개월", "1년"],
                          horizontal=True, index=1)
    period_map = {"1주": "5d", "1개월": "1mo", "3개월": "3mo", "6개월": "6mo", "1년": "1y"}

    with st.spinner("차트 로딩 중..."):
        hist = load_history(period_map[period_opt])

    if not hist.empty:
        fig = go.Figure()
        fig.add_trace(go.Candlestick(
            x=hist.index,
            open=hist["Open"], high=hist["High"],
            low=hist["Low"], close=hist["Close"],
            name="케어젠",
            increasing_line_color="#c0392b",
            decreasing_line_color="#2563eb",
        ))
        fig.add_trace(go.Bar(
            x=hist.index, y=hist["Volume"],
            name="거래량", yaxis="y2",
            marker_color="rgba(100,100,200,0.25)",
        ))
        fig.update_layout(
            height=420,
            xaxis_rangeslider_visible=False,
            yaxis=dict(title="주가(원)", side="left"),
            yaxis2=dict(title="거래량", side="right", overlaying="y", showgrid=False),
            legend=dict(orientation="h", y=1.05),
            margin=dict(l=0, r=0, t=30, b=0),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("차트 데이터를 불러올 수 없습니다.")

with col_supply:
    st.markdown('<div class="section-title">📊 수급 동향 (최근 20일)</div>', unsafe_allow_html=True)
    with st.spinner("수급 데이터 로딩 중..."):
        hist_supply = load_history("1mo")

    if not hist_supply.empty:
        df = hist_supply.tail(20)[["Close", "Volume"]].copy()
        df.index = df.index.strftime("%m/%d")
        df.columns = ["종가(원)", "거래량(주)"]
        df["종가(원)"] = df["종가(원)"].apply(lambda x: f"{int(x):,}")
        df["거래량(주)"] = df["거거량(주)"] if "거거량(주)" in df.columns else df["거래량(주)"].apply(lambda x: f"{int(x):,}")
        st.dataframe(df[::-1], use_container_width=True, height=380)
    else:
        st.warning("수급 데이터를 불러올 수 없습니다.")

st.divider()

# ── 뉴스 + 공시 ───────────────────────────────────────
col_news, col_dart = st.columns(2)

with col_news:
    st.markdown('<div class="section-title">📰 관련 뉴스</div>', unsafe_allow_html=True)
    naver_url = load_naver_news()
    st.markdown(f"""
    <div class="news-item">
        <span class="news-badge badge-news">뉴스</span>
        <a href="{naver_url}" target="_blank">🔗 케어젠 최신 뉴스 보기 (네이버)</a>
    </div>
    <div class="news-item">
        <span class="news-badge badge-news">뉴스</span>
        <a href="https://search.naver.com/search.naver?where=news&query=케어젠+주가&sort=1" target="_blank">🔗 케어젠 주가 뉴스 (네이버)</a>
    </div>
    <div class="news-item">
        <span class="news-badge badge-pr">보도자료</span>
        <a href="https://www.caregen.co.kr/kor/pr/news.php" target="_blank">🔗 케어젠 공식 보도자료</a>
    </div>
    <div class="news-item">
        <span class="news-badge badge-news">뉴스</span>
        <a href="https://finance.naver.com/item/news.naver?code=214370" target="_blank">🔗 네이버 종목 뉴스 (214370)</a>
    </div>
    """, unsafe_allow_html=True)

with col_dart:
    st.markdown('<div class="section-title">📋 공시 자료 (DART)</div>', unsafe_allow_html=True)
    dart_items = load_dart_disclosures()

    if dart_items:
        for item in dart_items[:8]:
            date_fmt = f"{item['date'][:4]}.{item['date'][4:6]}.{item['date'][6:]}" if len(item['date']) == 8 else item['date']
            st.markdown(f"""
            <div class="news-item">
                <span class="news-badge badge-dart">공시</span>
                <a href="{item['url']}" target="_blank">{item['title']}</a>
                <span style="font-size:11px;color:#9ca3af;margin-left:8px;">{date_fmt}</span>
            </div>""", unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="news-item">
            <span class="news-badge badge-dart">공시</span>
            <a href="https://dart.fss.or.kr/dsab001/search.ax?textCrpNm=케어젠" target="_blank">🔗 DART 케어젠 공시 전체 보기</a>
        </div>
        <div style="font-size:12px;color:#9ca3af;margin-top:8px;">
            ※ 실시간 공시를 보려면 <b>DART API 키</b>를 secrets.toml에 입력하세요.
        </div>""", unsafe_allow_html=True)

st.divider()

# ── 기본 정보 ─────────────────────────────────────────
st.markdown('<div class="section-title">🏢 기업 기본 정보</div>', unsafe_allow_html=True)
ci1, ci2, ci3, ci4 = st.columns(4)
ci1.metric("업종", "의약품")
ci2.metric("상장시장", "코스닥")
ci3.metric("결산월", "12월")
ci4.metric("주요제품", "바이오 성장인자")

st.caption("데이터 출처: Yahoo Finance · DART 금융감독원 · 네이버 금융 | 본 대시보드는 투자 권유 목적이 아닙니다.")
