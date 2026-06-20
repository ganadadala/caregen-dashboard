# 케어젠 Dash Board — 주식 일일 동향 대시보드

한국투자증권(KIS) Open API로 주가·시총·52주 차트를 자동으로 불러오고,
IR·공시·PR 업무와 주가 동향 내러티브를 채워 대표 보고용 1페이지로 출력하는 대시보드입니다.
업로드한 샘플 양식과 동일한 레이아웃으로 구성되어 있습니다.

## 자동 / 수기 구분
**자동 조회 (KIS API, "조회" 버튼)**
- At close(KRX 종가), Market Cap(시가총액 → T 단위 환산), Highest(52주 최고가, KRX)
- 외국인 지분율
- Market 52주 차트(CG 일별 종가). 제약 업종지수 비교선은 `.env`에 코드 설정 시 추가.
- **공시 목록(DART)**: 최근 30일 공시 제목·날짜·원문 링크 자동 표시(대량보유 등 지분공시 포함). `DART_API_KEY` 설정 시 활성화.
- **미국 지수**: 다우·나스닥·S&P500 전일 대비 변동률(해외증시 스트립). KIS '해외시세' 사용 권한 필요.

**수기 입력 (분석자가 작성 · 날짜별 자동 저장)**
- Investor Relations / Corporate Disclosure / Public Relations 업무 내용
- 공매도 순잔고, CFD 잔고, NXT 가격, KOSDAQ/KRX 순위, KOSDAQ 지수
- 하단 [주가 변동 및 투자자 동향] 내러티브
- → 화면의 거의 모든 텍스트를 클릭해 바로 수정할 수 있고, 입력값은 브라우저에 날짜별로 저장됩니다.

> 공시·뉴스·업무 내용은 KIS 시세 API 범위 밖이라(DART·뉴스·내부 업무) 수기로 채웁니다.

## 준비 — KIS API 키 발급
1. https://apiportal.koreainvestment.com 로그인 (한국투자증권 계좌 필요)
2. "앱 등록" → APP Key / APP Secret 발급
3. `.env.example` 을 `.env` 로 복사 후 키 입력

```bash
cp .env.example .env   # .env 에 KIS_APPKEY / KIS_APPSECRET 입력
```

### (선택) DART 공시 자동 연동
1. https://opendart.fss.or.kr → 인증키 신청/관리에서 무료 인증키 발급
2. `.env` 의 `DART_API_KEY` 에 입력 → 조회 시 공시 카드에 자동 표시
3. 미설정 시 공시 목록만 비고, 나머지는 정상 동작

## 실행
```bash
pip install -r requirements.txt
uvicorn app:app --port 8000
```
브라우저에서 http://localhost:8000 접속 → **조회** 클릭.

## 매일 보고 루틴
1. 날짜 확인 → **조회**: 종가·시총·52주 최고가·외국인 지분율·차트 자동 채움
2. NXT·순위·KOSDAQ 지수 등 수기 항목 입력
3. IR / 공시 / PR 업무, 주가 동향 내러티브 작성 (클릭 후 바로 수정)
4. **인쇄 / PDF** → A4 1페이지로 저장해 대표 보고

## 참고
- 접근토큰은 약 24시간 유효하며 자동 캐싱됩니다(`.token_cache.json`).
- 투자자별 매매동향·당일 데이터는 장 종료 후 제공됩니다.
- 제약 업종지수 비교선: `KOSDAQ_PHARM_CODE` 에 업종코드 입력 시 활성화(미입력 시 종목선만 표시).
- 미국 지수(다우 `.DJI` / 나스닥 `COMP` / S&P500 `SPX`): KIS 앱에서 **해외시세 사용 권한**이 켜져 있어야 표시됩니다(미국 마감 후 갱신). 권한/심볼 문제 시 해당 값은 수기 입력으로 대체.
- `.env` 와 `.token_cache.json` 은 `.gitignore` 로 커밋에서 제외됩니다 — 키는 절대 GitHub에 올리지 마세요.

## 사용된 KIS API
| 용도 | 엔드포인트 | TR_ID |
|---|---|---|
| 접근토큰 | `POST /oauth2/tokenP` | — |
| 현재가 시세 | `GET .../quotations/inquire-price` | FHKST01010100 |
| 투자자별 매매동향 | `GET .../quotations/inquire-investor` | FHKST01010900 |
| 기간별시세(일봉) | `GET .../quotations/inquire-daily-itemchartprice` | FHKST03010100 |
| 해외지수(미국) | `GET /uapi/overseas-price/v1/quotations/inquire-daily-chartprice` | FHKST03030100 |

| (DART) 용도 | 엔드포인트 |
|---|---|
| 회사 고유번호 | `GET opendart.fss.or.kr/api/corpCode.xml` |
| 공시 목록 | `GET opendart.fss.or.kr/api/list.json` |
