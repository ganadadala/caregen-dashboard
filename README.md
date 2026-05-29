# 케어젠(214370) 주식 대시보드

케어젠 주식 담당자용 실시간 모니터링 대시보드입니다.

## 기능
- 📈 실시간 주가 (현재가, 등락률, 거래량, 시총, 52주 고/저)
- 🕯 캔들차트 + 거래량 (1주/1개월/3개월/6개월/1년)
- 📊 수급 동향 (최근 20일)
- 📰 네이버 뉴스 직접 링크
- 📋 DART 공시 실시간 (API 키 필요)
- 🏢 기업 기본 정보

## 로컬 실행

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Streamlit Cloud 배포 (무료, URL 공유 가능)

1. 이 폴더를 GitHub에 올리기
   ```bash
   git init
   git add .
   git commit -m "첫 커밋"
   git remote add origin https://github.com/본인계정/caregen-dashboard.git
   git push -u origin main
   ```

2. https://share.streamlit.io 접속 → GitHub 연결

3. Repository 선택 → `app.py` 선택 → Deploy!

4. `https://본인계정-caregen-dashboard.streamlit.app` URL 생성됨

## DART API 키 설정 (공시 실시간 조회)

1. https://opendart.fss.or.kr 회원가입 (무료)
2. API 키 발급
3. Streamlit Cloud → Settings → Secrets에 입력:
   ```
   DART_API_KEY = "발급받은키"
   ```

## 데이터 출처
- 주가: Yahoo Finance (yfinance)
- 공시: DART 금융감독원 OpenAPI
- 뉴스: 네이버 금융
