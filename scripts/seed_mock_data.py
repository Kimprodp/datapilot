"""
Mock 데이터 생성 스크립트

DuckDB 파일에 도메인별 mock 을 생성한다.
- 게임 (data/mock/game.db): 12 테이블 + Pizza Ready 30일치 데이터.
  2개 시나리오: Android UI 변경 (3/28) / PG 장애 PagSeguro (3/31).
- 이커머스 (data/mock/ecommerce.db): 6 테이블 빈 스키마.
  데이터 매립은 후속 task (이커머스 mock seed) 에서.

실행: uv run python scripts/seed_mock_data.py
"""

import random
from datetime import date, datetime, timedelta

import duckdb
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data" / "mock"
DB_PATH = DATA_DIR / "game.db"
ECOMMERCE_DB_PATH = DATA_DIR / "ecommerce.db"
BASE_DATE = date(2026, 3, 31)   # 데이터 기간 마지막 날
PERIOD_DAYS = 30                # 30일치
NUM_USERS = 3000                # Mock 유저 수 (D7 코호트 일 ~25명, 실행 시간 절충)
START_DATE = BASE_DATE - timedelta(days=PERIOD_DAYS - 1)  # 2026-03-02

# 시나리오 핵심 시점
UI_CHANGE_DATE = BASE_DATE - timedelta(days=3)     # D-3: Android UI 변경 (2026-03-28)
PG_OUTAGE_DATE = BASE_DATE                         # D-0: PG 장애 (2026-03-31)

# 상품 가격 매핑 (원)
PRODUCT_PRICES = {
    "p_001": 100,  "p_002": 500,  "p_003": 900,    # low
    "p_004": 2500, "p_005": 2000,                  # mid
    "p_006": 5000, "p_007": 9900, "p_008": 7900,   # premium
}
RANDOM_SEED = 42
PREMIUM_PRODUCTS = {"p_006", "p_007", "p_008"}  # set: O(1) lookup
NON_PREMIUM_PRODUCTS = ["p_001", "p_002", "p_003", "p_004", "p_005"]
ALL_PRODUCTS = list(PRODUCT_PRICES.keys())


def create_tables(conn: duckdb.DuckDBPyConnection):
    """12개 테이블 생성 (DROP IF EXISTS → CREATE)"""

    # 기존 테이블 제거
    conn.execute("DROP TABLE IF EXISTS daily_kpi")
    conn.execute("DROP TABLE IF EXISTS users")
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("DROP TABLE IF EXISTS payments")
    conn.execute("DROP TABLE IF EXISTS shop_impressions")
    conn.execute("DROP TABLE IF EXISTS releases")
    conn.execute("DROP TABLE IF EXISTS events")
    conn.execute("DROP TABLE IF EXISTS sessions")
    conn.execute("DROP TABLE IF EXISTS content_releases")
    conn.execute("DROP TABLE IF EXISTS gateways")
    conn.execute("DROP TABLE IF EXISTS payment_attempts")
    conn.execute("DROP TABLE IF EXISTS payment_errors")

    # ── 집계 ──

    conn.execute("""
        CREATE TABLE daily_kpi (
            date                 DATE    PRIMARY KEY,
            dau                  INTEGER NOT NULL,
            mau                  INTEGER NOT NULL,
            revenue              DECIMAL NOT NULL,
            arppu                DECIMAL NOT NULL,
            d1_retention         DECIMAL NOT NULL,  -- 0~1
            d7_retention         DECIMAL NOT NULL,  -- 0~1
            sessions             INTEGER NOT NULL,
            avg_session_sec      INTEGER NOT NULL,
            payment_success_rate DECIMAL NOT NULL,  -- 0~1
            new_installs         INTEGER NOT NULL
        )
    """)

    # ── 공통 ──

    conn.execute("""
        CREATE TABLE users (
            user_id       VARCHAR PRIMARY KEY,
            platform      VARCHAR NOT NULL,   -- 'android' / 'ios'
            country       VARCHAR NOT NULL,   -- 'brazil', 'usa', 'korea', ...
            user_type     VARCHAR NOT NULL,   -- 'new' / 'existing'
            install_date  DATE    NOT NULL,
            device_model  VARCHAR NOT NULL    -- 'low' / 'mid' / 'high'
        )
    """)

    # ── 시나리오 1: 매출 (Android UI 변경) ──

    conn.execute("""
        CREATE TABLE products (
            product_id  VARCHAR PRIMARY KEY,
            name        VARCHAR NOT NULL,
            price_tier  VARCHAR NOT NULL,  -- 'low' / 'mid' / 'premium'
            category    VARCHAR NOT NULL   -- 'consumable' / 'package' / 'subscription'
        )
    """)

    conn.execute("""
        CREATE TABLE payments (
            payment_id  VARCHAR   PRIMARY KEY,
            user_id     VARCHAR   NOT NULL,
            product_id  VARCHAR   NOT NULL,
            amount      DECIMAL   NOT NULL,
            timestamp   TIMESTAMP NOT NULL,
            status      VARCHAR   NOT NULL   -- 'success' / 'failed' / 'refunded'
        )
    """)

    conn.execute("""
        CREATE TABLE shop_impressions (
            impression_id    VARCHAR   PRIMARY KEY,
            user_id          VARCHAR   NOT NULL,
            product_id       VARCHAR   NOT NULL,
            impression_time  TIMESTAMP NOT NULL,
            slot_order       INTEGER   NOT NULL   -- 1=최상단. UI 변경 후 premium이 하단으로
        )
    """)

    conn.execute("""
        CREATE TABLE releases (
            release_id   VARCHAR   PRIMARY KEY,
            version      VARCHAR   NOT NULL,
            platform     VARCHAR   NOT NULL,   -- 'android' / 'ios' / 'both'
            released_at  TIMESTAMP NOT NULL,
            build_notes  VARCHAR   NOT NULL
        )
    """)

    # ── 시나리오 2: 리텐션 (이벤트 종료) ──

    conn.execute("""
        CREATE TABLE events (
            event_id    VARCHAR   PRIMARY KEY,
            user_id     VARCHAR   NOT NULL,
            event_type  VARCHAR   NOT NULL,   -- 'login' / 'stage_clear' / 'purchase' / 'event_participate'
            event_time  TIMESTAMP NOT NULL,
            metadata    VARCHAR               -- JSON 문자열
        )
    """)

    conn.execute("""
        CREATE TABLE sessions (
            session_id     VARCHAR   PRIMARY KEY,
            user_id        VARCHAR   NOT NULL,
            session_start  TIMESTAMP NOT NULL,
            session_end    TIMESTAMP NOT NULL,
            platform       VARCHAR   NOT NULL  -- 'android' / 'ios'
        )
    """)

    conn.execute("""
        CREATE TABLE content_releases (
            content_id    VARCHAR PRIMARY KEY,
            content_type  VARCHAR NOT NULL,   -- 'season_event' / 'update' / 'promotion'
            name          VARCHAR NOT NULL,
            start_date    DATE    NOT NULL,
            end_date      DATE    NOT NULL
        )
    """)

    # ── 시나리오 3: 결제 성공률 (PG 장애) ──

    conn.execute("""
        CREATE TABLE gateways (
            gateway_id  VARCHAR PRIMARY KEY,
            name        VARCHAR NOT NULL,
            region      VARCHAR NOT NULL,  -- 'global' / 'brazil' / ...
            status      VARCHAR NOT NULL   -- 'active' / 'degraded' / 'down'
        )
    """)

    conn.execute("""
        CREATE TABLE payment_attempts (
            attempt_id    VARCHAR   PRIMARY KEY,
            user_id       VARCHAR   NOT NULL,
            gateway       VARCHAR   NOT NULL,
            attempt_time  TIMESTAMP NOT NULL,
            status        VARCHAR   NOT NULL,  -- 'success' / 'failed'
            error_code    VARCHAR              -- 실패 시 에러 코드, 성공이면 NULL
        )
    """)

    conn.execute("""
        CREATE TABLE payment_errors (
            error_code     VARCHAR   PRIMARY KEY,
            error_message  VARCHAR   NOT NULL,
            gateway        VARCHAR   NOT NULL,
            first_seen     TIMESTAMP NOT NULL
        )
    """)

    print("12개 테이블 생성 완료")


def seed_users(conn: duckdb.DuckDBPyConnection):
    """유저 마스터 데이터 생성 (3000명)"""

    # 분포 가중치 리스트 — random.choice가 이 리스트에서 하나를 뽑으면 자연스럽게 비율이 맞음
    # Random.nextInt로 뽑는 것과 동일
    platforms = ["android"] * 60 + ["ios"] * 40                         # 60:40
    countries = (["brazil"] * 15 + ["usa"] * 25 + ["korea"] * 20
                 + ["japan"] * 15 + ["india"] * 10 + ["others"] * 15)   # 비율 합 100
    devices = ["low"] * 30 + ["mid"] * 40 + ["high"] * 30               # 30:40:30

    users = []
    for i in range(NUM_USERS):
        # install_date 를 90일 균등 분포로 → new_installs 일별 안정 (~33명/일)
        # user_type 은 install_date 기반 자동 결정 (7일 이내 = new)
        install = BASE_DATE - timedelta(days=random.randint(0, 89))
        user_type = "new" if (BASE_DATE - install).days <= 6 else "existing"

        users.append((
            f"u_{i:05d}",           # "u_00000", "u_00001", ...
            random.choice(platforms),
            random.choice(countries),
            user_type,
            install,
            random.choice(devices),
        ))

    conn.executemany("INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)", users)
    print(f"users: {len(users)}건 삽입")


def seed_products(conn: duckdb.DuckDBPyConnection):
    """상품 마스터 데이터 (8개)"""

    products = [
        ("p_001", "Mini Coins",         "low",     "consumable"),
        ("p_002", "Coin Pack 100",      "low",     "consumable"),
        ("p_003", "Daily Deal",         "low",     "consumable"),
        ("p_004", "Booster Pack",       "mid",     "package"),
        ("p_005", "Coin Pack 500",      "mid",     "consumable"),
        ("p_006", "Pizza Starter Pack", "premium", "package"),
        ("p_007", "Premium Chef Pack",  "premium", "package"),
        ("p_008", "VIP Pass",           "premium", "subscription"),
    ]

    conn.executemany("INSERT INTO products VALUES (?, ?, ?, ?)", products)
    print(f"products: {len(products)}건 삽입")


def seed_gateways(conn: duckdb.DuckDBPyConnection):
    """PG사 정보 (4개). pagseguro만 degraded 상태."""

    gateways = [
        ("google_play", "Google Play",  "global", "active"),
        ("apple_pay",   "Apple Pay",    "global", "active"),
        ("pagseguro",   "PagSeguro",    "brazil", "degraded"),
        ("stripe",      "Stripe",       "global", "active"),
    ]

    conn.executemany("INSERT INTO gateways VALUES (?, ?, ?, ?)", gateways)
    print(f"gateways: {len(gateways)}건 삽입")


def seed_content_releases(conn: duckdb.DuckDBPyConnection):
    """컨텐츠/이벤트 스케줄. 시즌 이벤트가 D-14(3/17)에 종료."""

    content_releases = [
        ("c_001", "season_event", "Pizza Festival Season 3",  date(2026, 2, 10), date(2026, 4, 15)),
        ("c_002", "update",       "Spring Content Update",    date(2026, 2, 20), date(2026, 4, 15)),
        ("c_003", "promotion",    "New Player Welcome Bonus", date(2026, 3, 1),  date(2026, 3, 31)),
    ]

    conn.executemany("INSERT INTO content_releases VALUES (?, ?, ?, ?, ?)", content_releases)
    print(f"content_releases: {len(content_releases)}건 삽입")


def seed_releases(conn: duckdb.DuckDBPyConnection):
    """빌드 배포 이력. D-3(3/28)에 Android 배포."""

    releases = [
        ("r_001", "v1.2.0", "both",    "2026-02-20 10:00:00", "Monthly content update"),
        ("r_002", "v1.2.1", "android", "2026-03-08 14:00:00", "Bug fixes and stability improvements"),
        ("r_003", "v1.2.2", "ios",     "2026-03-12 11:00:00", "Performance improvements"),
        ("r_004", "v1.2.3", "android", "2026-03-28 09:00:00", "Shop UI refresh (featured slot reordering)"),
        ("r_005", "v1.2.4", "ios",     "2026-03-29 15:00:00", "Minor bug fixes"),
    ]

    conn.executemany("INSERT INTO releases VALUES (?, ?, ?, ?, ?)", releases)
    print(f"releases: {len(releases)}건 삽입")


# ── 시나리오 1 트랜잭션: 매출 -8% (Android UI 변경) ──

def seed_payments(conn: duckdb.DuckDBPyConnection):
    """결제 데이터 30일치. 상점에서 노출된 상품 중에서만 구매 발생.
    shop_impressions가 먼저 생성되어 있어야 한다.
    D-3 이후 Android는 premium 노출이 줄어들어 자연스럽게 premium 구매 감소."""

    # shop_impressions에서 날짜별 → {유저: 노출 상품 set} 매핑 구성
    impression_rows = conn.execute("""
        SELECT CAST(impression_time AS DATE) as d, user_id, product_id
        FROM shop_impressions
    """).fetchall()

    # 날짜 → {유저: {상품 set}} 구조
    daily_impressions: dict[str, dict[str, set]] = {}
    for d, uid, pid in impression_rows:
        ds = str(d)
        if ds not in daily_impressions:
            daily_impressions[ds] = {}
        if uid not in daily_impressions[ds]:
            daily_impressions[ds][uid] = set()
        daily_impressions[ds][uid].add(pid)

    payments = []
    pay_id = 0

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        ds = str(current_date)

        # 해당 날짜에 상점 방문한 유저만 구매 대상
        visitors = daily_impressions.get(ds, {})

        for user_id, visible_products in visitors.items():
            # 방문 유저 중 28%가 구매 (전체 유저 대비 ~7%: 방문율 25% × 28%)
            if random.random() > 0.28:
                continue

            # 노출된 상품 중 랜덤 선택 → premium이 안 보였으면 살 수 없음
            product = random.choice(list(visible_products))
            hour = random.randint(8, 23)
            minute = random.randint(0, 59)

            # 결제 상태: 97% success, 2% failed, 1% refunded
            r = random.random()
            if r < 0.02:
                status = "failed"
            elif r < 0.03:
                status = "refunded"
            else:
                status = "success"

            payments.append((
                f"pay_{pay_id:06d}",
                user_id,
                product,
                PRODUCT_PRICES[product],
                f"{current_date} {hour:02d}:{minute:02d}:00",
                status,
            ))
            pay_id += 1

    conn.executemany("INSERT INTO payments VALUES (?, ?, ?, ?, ?, ?)", payments)
    print(f"payments: {len(payments)}건 삽입")


def seed_shop_impressions(conn: duckdb.DuckDBPyConnection):
    """상점 노출 로그 30일치. 유저별 스크롤 깊이(3~8)만큼만 노출 기록.
    D-3 이후 Android는 premium이 하단(4~6)으로 밀려서 자연스럽게 노출 급감."""

    users = conn.execute("SELECT user_id, platform FROM users").fetchall()

    impressions = []
    iid = 0
    # 매일 25%만 상점 방문 → 방문자만 미리 추출해서 75% 루프 절약
    n_visitors = int(len(users) * 0.25)

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        is_after_change = current_date >= UI_CHANGE_DATE
        date_str = str(current_date)

        visitors = random.sample(users, n_visitors)

        for user_id, platform in visitors:
            # 각 상품의 slot_order 결정
            product_slots = []
            for product_id in ALL_PRODUCTS:
                is_premium = product_id in PREMIUM_PRODUCTS

                if is_after_change and platform == "android" and is_premium:
                    # UI 변경 후: 하단으로 깊이 밀림 (scroll_depth 3~8 안에 거의 안 들어옴)
                    # 매출 영향 -9% → -18% 수준으로 강화 (① BottleneckDetector 가 noise 안 분류)
                    slot = random.randint(7, 10)
                elif is_premium:
                    slot = random.randint(1, 3)     # 정상: 상단
                else:
                    slot = random.randint(4, 8)     # 일반 상품: 중하단

                product_slots.append((product_id, slot))

            # slot_order 순 정렬 → 스크롤 깊이만큼만 노출
            product_slots.sort(key=lambda x: x[1])
            scroll_depth = random.randint(3, 8)
            visible = product_slots[:scroll_depth]

            hour = random.randint(9, 22)
            minute = random.randint(0, 59)

            for product_id, slot_order in visible:
                impressions.append((
                    f"imp_{iid:07d}",
                    user_id,
                    product_id,
                    f"{date_str} {hour:02d}:{minute:02d}:00",
                    slot_order,
                ))
                iid += 1

    conn.executemany("INSERT INTO shop_impressions VALUES (?, ?, ?, ?, ?)", impressions)
    print(f"shop_impressions: {len(impressions)}건 삽입")


# ── 세션/이벤트 데이터 (이상 신호 매립 없음) ──

def seed_sessions(conn: duckdb.DuckDBPyConnection):
    """세션 로그 30일치. 모든 유저 동일 복귀 확률(30%)."""

    users = conn.execute(
        "SELECT user_id, platform, user_type, install_date FROM users"
    ).fetchall()

    sessions = []
    sid = 0

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)

        for user_id, platform, user_type, install_date in users:
            login_rate = 0.30

            if random.random() > login_rate:
                continue

            hour = random.randint(7, 23)
            minute = random.randint(0, 59)
            duration_min = random.randint(3, 30)  # 3~30분

            # 세션 종료 시간: datetime으로 정확하게 계산
            start_dt = datetime(current_date.year, current_date.month, current_date.day, hour, minute)
            end_dt = start_dt + timedelta(minutes=duration_min)

            sessions.append((
                f"s_{sid:06d}",
                user_id,
                start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                platform,
            ))
            sid += 1

    conn.executemany("INSERT INTO sessions VALUES (?, ?, ?, ?, ?)", sessions)
    print(f"sessions: {len(sessions)}건 삽입")


def seed_events(conn: duckdb.DuckDBPyConnection):
    """인게임 이벤트 로그. 세션 기반으로 생성.
    - stage_clear: 세션당 50% 확률로 1회
    - event_participate: 이벤트 진행 중, 세션당 40% 확률"""

    sessions_data = conn.execute(
        "SELECT session_id, user_id, session_start FROM sessions"
    ).fetchall()

    events = []
    eid = 0

    for session_id, user_id, session_start in sessions_data:
        # login (세션마다 1회)
        events.append((
            f"e_{eid:07d}", user_id, "login",
            str(session_start), None,
        ))
        eid += 1

        # stage_clear (세션당 50% 확률로 1회)
        if random.random() < 0.5:
            events.append((
                f"e_{eid:07d}", user_id, "stage_clear",
                str(session_start), None,
            ))
            eid += 1

        # purchase (세션당 10% 확률 — 결제 이벤트 로그)
        if random.random() < 0.1:
            events.append((
                f"e_{eid:07d}", user_id, "purchase",
                str(session_start), '{"source": "shop"}',
            ))
            eid += 1

        # event_participate (이벤트 진행 중, 세션당 40% 확률)
        if random.random() < 0.4:
            events.append((
                f"e_{eid:07d}", user_id, "event_participate",
                str(session_start), '{"event": "pizza_festival_s3"}',
            ))
            eid += 1

    conn.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?)", events)
    print(f"events: {len(events)}건 삽입")


# ── 시나리오 3 트랜잭션: 결제 성공률 -25% (PG 장애) ──

def seed_payment_errors(conn: duckdb.DuckDBPyConnection):
    """결제 에러 코드 마스터. pagseguro 에러가 D-0에 집중 발생."""

    payment_errors = [
        ("E001", "Gateway timeout",      "pagseguro",   f"{PG_OUTAGE_DATE} 18:00:00"),
        ("E002", "Connection refused",   "pagseguro",   f"{PG_OUTAGE_DATE} 18:15:00"),
        ("E003", "Service unavailable",  "pagseguro",   f"{PG_OUTAGE_DATE} 18:30:00"),
        ("E004", "Transaction declined", "google_play", "2026-03-05 10:00:00"),
        ("E005", "Insufficient funds",   "stripe",      "2026-03-08 14:00:00"),
    ]

    conn.executemany("INSERT INTO payment_errors VALUES (?, ?, ?, ?)", payment_errors)
    print(f"payment_errors: {len(payment_errors)}건 삽입")


def seed_payment_attempts(conn: duckdb.DuckDBPyConnection):
    """결제 시도 로그 30일치. D-0에 pagseguro(브라질) 실패율 급등.
    - 브라질 유저: 70% pagseguro, 나머지 google_play/apple_pay/stripe
    - 비브라질 유저: android=google_play, ios=apple_pay
    - 정상 실패율 2%, D-0 pagseguro + 18시 이후만 60% 실패"""

    users = conn.execute(
        "SELECT user_id, platform, country FROM users"
    ).fetchall()

    pg_error_codes = ["E001", "E002", "E003"]    # pagseguro 에러
    general_error_codes = ["E004", "E005"]        # 일반 에러

    attempts = []
    aid = 0
    # 매일 유저 7%만 결제 시도 → 해당 유저만 미리 추출해서 93% 루프 절약
    n_payers = int(len(users) * 0.07)

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        is_outage_day = current_date == PG_OUTAGE_DATE
        date_str = str(current_date)

        payers = random.sample(users, n_payers)

        for user_id, platform, country in payers:
            # gateway 배정: 브라질 70% pagseguro, 비브라질은 플랫폼 기반
            if country == "brazil":
                r = random.random()
                if r < 0.70:
                    gateway = "pagseguro"
                elif r < 0.85:
                    gateway = "google_play" if platform == "android" else "apple_pay"
                else:
                    gateway = "stripe"
            else:
                if random.random() < 0.95:
                    gateway = "google_play" if platform == "android" else "apple_pay"
                else:
                    gateway = "stripe"

            hour = random.randint(8, 23)
            minute = random.randint(0, 59)

            # 실패 여부: 정상 2%, D-0 pagseguro 하루 전체 60%
            if is_outage_day and gateway == "pagseguro":
                failed = random.random() < 0.60
            else:
                failed = random.random() < 0.02

            status = "failed" if failed else "success"
            error_code = None
            if failed:
                error_code = (random.choice(pg_error_codes) if gateway == "pagseguro"
                              else random.choice(general_error_codes))

            attempts.append((
                f"att_{aid:06d}",
                user_id,
                gateway,
                f"{date_str} {hour:02d}:{minute:02d}:00",
                status,
                error_code,
            ))
            aid += 1

    conn.executemany("INSERT INTO payment_attempts VALUES (?, ?, ?, ?, ?, ?)", attempts)
    print(f"payment_attempts: {len(attempts)}건 삽입")


# ── daily_kpi 집계 (raw 테이블 기반) ──

def seed_daily_kpi(conn: duckdb.DuckDBPyConnection):
    """raw 테이블에서 일별 KPI를 집계해 daily_kpi에 삽입.
    단일 SQL로 30일치를 한번에 계산하므로 빠르고, raw ↔ daily_kpi 수치가 반드시 일치한다."""

    conn.execute(f"""
        INSERT INTO daily_kpi
        WITH dates AS (
            -- 30일치 날짜 시퀀스 생성
            SELECT CAST('{START_DATE}' AS DATE) + INTERVAL (i) DAY AS date
            FROM generate_series(0, {PERIOD_DAYS - 1}) AS t(i)
        ),
        dau AS (
            -- DAU: 해당 날짜에 세션이 있는 유니크 유저
            SELECT CAST(session_start AS DATE) AS date,
                   COUNT(DISTINCT user_id) AS dau
            FROM sessions GROUP BY 1
        ),
        mau AS (
            -- MAU: 전체 기간 유니크 유저 (프로토타입 단순화 — 30일 윈도우 JOIN 병목 회피)
            SELECT d.date, (SELECT COUNT(DISTINCT user_id) FROM sessions) AS mau
            FROM dates d
        ),
        rev AS (
            -- Revenue + 결제 유저 수 (ARPPU 계산용)
            SELECT CAST(timestamp AS DATE) AS date,
                   COALESCE(SUM(amount), 0) AS revenue,
                   COUNT(DISTINCT user_id) AS paying_users
            FROM payments WHERE status = 'success'
            GROUP BY 1
        ),
        d1 AS (
            -- D1 Retention: 어제 설치 유저 중 오늘 접속 비율
            SELECT d.date,
                   COUNT(DISTINCT u.user_id) FILTER (
                       WHERE s.user_id IS NOT NULL
                   ) AS returned,
                   COUNT(DISTINCT u.user_id) AS total
            FROM dates d
            JOIN users u ON u.install_date = CAST(d.date AS DATE) - 1
            LEFT JOIN sessions s
              ON u.user_id = s.user_id
             AND CAST(s.session_start AS DATE) = d.date
            GROUP BY d.date
        ),
        d7 AS (
            -- D7 Retention: 7일 전 설치 유저 중 오늘 접속 비율
            SELECT d.date,
                   COUNT(DISTINCT u.user_id) FILTER (
                       WHERE s.user_id IS NOT NULL
                   ) AS returned,
                   COUNT(DISTINCT u.user_id) AS total
            FROM dates d
            JOIN users u ON u.install_date = CAST(d.date AS DATE) - 7
            LEFT JOIN sessions s
              ON u.user_id = s.user_id
             AND CAST(s.session_start AS DATE) = d.date
            GROUP BY d.date
        ),
        sess AS (
            -- 총 세션 수 + 평균 세션 길이 (초)
            SELECT CAST(session_start AS DATE) AS date,
                   COUNT(*) AS total_sessions,
                   AVG(EXTRACT(EPOCH FROM (
                       session_end::TIMESTAMP - session_start::TIMESTAMP
                   ))) AS avg_sec
            FROM sessions GROUP BY 1
        ),
        att AS (
            -- 결제 시도 성공률 (payment_attempts 기반)
            SELECT CAST(attempt_time AS DATE) AS date,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE status = 'success') AS success
            FROM payment_attempts GROUP BY 1
        ),
        installs AS (
            -- 신규 설치 수
            SELECT install_date AS date, COUNT(*) AS cnt
            FROM users GROUP BY 1
        )
        SELECT
            d.date,
            COALESCE(dau.dau, 0),                                          -- dau
            COALESCE(mau.mau, 0),                                          -- mau
            COALESCE(rev.revenue, 0),                                      -- revenue
            CASE WHEN COALESCE(rev.paying_users, 0) > 0                    -- arppu
                 THEN ROUND(rev.revenue / rev.paying_users, 2)
                 ELSE 0 END,
            CASE WHEN COALESCE(d1.total, 0) > 0                            -- d1_retention
                 THEN ROUND(d1.returned * 1.0 / d1.total, 4)
                 ELSE 0 END,
            CASE WHEN COALESCE(d7.total, 0) > 0                            -- d7_retention
                 THEN ROUND(d7.returned * 1.0 / d7.total, 4)
                 ELSE 0 END,
            COALESCE(sess.total_sessions, 0),                              -- sessions
            COALESCE(CAST(sess.avg_sec AS INTEGER), 0),                    -- avg_session_sec
            CASE WHEN COALESCE(att.total, 0) > 0                           -- payment_success_rate
                 THEN ROUND(att.success * 1.0 / att.total, 4)
                 ELSE 1.0 END,
            COALESCE(installs.cnt, 0)                                      -- new_installs
        FROM dates d
        LEFT JOIN dau ON dau.date = d.date
        LEFT JOIN mau ON mau.date = d.date
        LEFT JOIN rev ON rev.date = d.date
        LEFT JOIN d1 ON d1.date = d.date
        LEFT JOIN d7 ON d7.date = d.date
        LEFT JOIN sess ON sess.date = d.date
        LEFT JOIN att ON att.date = d.date
        LEFT JOIN installs ON installs.date = d.date
        ORDER BY d.date
    """)

    count = conn.execute("SELECT COUNT(*) FROM daily_kpi").fetchone()[0]
    print(f"daily_kpi: {count}건 삽입")


# ──────────────────────────────────────────────────────────────────
# 이커머스 mock — 시나리오 2 개 매립
# ──────────────────────────────────────────────────────────────────

ECOMMERCE_BASE_DATE = date(2026, 3, 31)
ECOMMERCE_PERIOD_DAYS = 30
ECOMMERCE_START = ECOMMERCE_BASE_DATE - timedelta(days=ECOMMERCE_PERIOD_DAYS - 1)
NUM_CUSTOMERS = 1000

#: 재고 부족 시나리오 — D-7 부터 kitchen 카테고리 인기 상품 품절
INVENTORY_OUT_DATE = ECOMMERCE_BASE_DATE - timedelta(days=7)  # 2026-03-24
TOP_KITCHEN_PRODUCT = "p_kitchen_01"

ECOMMERCE_CATEGORIES = ["kitchen", "fashion", "electronics", "beauty"]
PRODUCTS_PER_CATEGORY = 5

ECOMMERCE_COUNTRIES = ["korea", "usa", "japan", "germany"]
ECOMMERCE_CUSTOMER_TYPES = ["new", "returning", "vip"]
ECOMMERCE_DEVICES = ["mobile", "desktop", "tablet"]


def create_ecommerce_tables(conn: duckdb.DuckDBPyConnection):
    """이커머스 mock 의 6 테이블 빈 스키마 생성.

    스키마는 docs/features/domain-extension/tech-spec.md §4 정의를 따른다.
    """

    conn.execute("DROP TABLE IF EXISTS daily_kpi")
    conn.execute("DROP TABLE IF EXISTS customers")
    conn.execute("DROP TABLE IF EXISTS orders")
    conn.execute("DROP TABLE IF EXISTS products")
    conn.execute("DROP TABLE IF EXISTS category_daily_revenue")
    conn.execute("DROP TABLE IF EXISTS inventory_changes")

    conn.execute("""
        CREATE TABLE daily_kpi (
            date                 DATE    PRIMARY KEY,
            gmv                  DECIMAL NOT NULL,
            orders               INTEGER NOT NULL,
            conversion           DECIMAL NOT NULL,  -- 0~1
            visitors             INTEGER NOT NULL,
            payment_success_rate DECIMAL NOT NULL   -- 0~1
        )
    """)

    conn.execute("""
        CREATE TABLE customers (
            customer_id   VARCHAR PRIMARY KEY,
            country       VARCHAR,
            customer_type VARCHAR,  -- new / returning / vip
            device        VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE products (
            product_id        VARCHAR PRIMARY KEY,
            category          VARCHAR NOT NULL,
            inventory_status  VARCHAR NOT NULL,  -- in_stock / out_of_stock / discontinued
            name              VARCHAR
        )
    """)

    conn.execute("""
        CREATE TABLE orders (
            order_id      VARCHAR PRIMARY KEY,
            customer_id   VARCHAR,
            category      VARCHAR,
            product_id    VARCHAR,
            amount        DECIMAL NOT NULL,
            paid_at       TIMESTAMP NOT NULL
        )
    """)

    conn.execute("""
        CREATE TABLE category_daily_revenue (
            date     DATE NOT NULL,
            category VARCHAR NOT NULL,
            gmv      DECIMAL NOT NULL,
            orders   INTEGER NOT NULL,
            PRIMARY KEY (date, category)
        )
    """)

    # 시점별 재고 상태 변경 이력 — ④ Validator 가 "D-7 부터 품절" 같은
    # 시점별 SQL 검증을 할 수 있도록.
    conn.execute("""
        CREATE TABLE inventory_changes (
            change_id   VARCHAR PRIMARY KEY,
            product_id  VARCHAR NOT NULL,
            changed_at  TIMESTAMP NOT NULL,
            status      VARCHAR NOT NULL,  -- in_stock / out_of_stock / discontinued
            note        VARCHAR
        )
    """)


def seed_ecommerce_customers(conn: duckdb.DuckDBPyConnection):
    """1000 명 customer. country/customer_type/device 자연 분포."""
    rows = []
    for i in range(NUM_CUSTOMERS):
        country = random.choices(
            ECOMMERCE_COUNTRIES, weights=[0.4, 0.3, 0.2, 0.1], k=1,
        )[0]
        customer_type = random.choices(
            ECOMMERCE_CUSTOMER_TYPES, weights=[0.3, 0.5, 0.2], k=1,
        )[0]
        device = random.choices(
            ECOMMERCE_DEVICES, weights=[0.6, 0.3, 0.1], k=1,
        )[0]
        rows.append((f"c_{i:04d}", country, customer_type, device))
    conn.executemany(
        "INSERT INTO customers (customer_id, country, customer_type, device) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    print(f"customers: {len(rows)}건 삽입")


def seed_ecommerce_products(conn: duckdb.DuckDBPyConnection):
    """20 개 상품 (4 카테고리 × 5). p_kitchen_01 만 out_of_stock."""
    rows = []
    for category in ECOMMERCE_CATEGORIES:
        for i in range(1, PRODUCTS_PER_CATEGORY + 1):
            pid = f"p_{category}_{i:02d}"
            # 재고 부족 시나리오: kitchen 카테고리 인기 상품 (p_kitchen_01) 만 품절
            inventory = (
                "out_of_stock" if pid == TOP_KITCHEN_PRODUCT else "in_stock"
            )
            name = f"{category.capitalize()} Item {i}"
            rows.append((pid, category, inventory, name))
    conn.executemany(
        "INSERT INTO products (product_id, category, inventory_status, name) "
        "VALUES (?, ?, ?, ?)",
        rows,
    )
    print(f"products: {len(rows)}건 삽입 (p_kitchen_01 = out_of_stock)")


def seed_ecommerce_orders(conn: duckdb.DuckDBPyConnection):
    """30 일치 일별 주문. 재고 부족 시나리오 시점 (D-7) 에 변동.

    - 평소: 일평균 ~500 건. 카테고리 균등 분포 (각 25%, 약 125 건/일/카테고리).
    - D-7 부터: kitchen 카테고리에서 p_kitchen_01 (해당 카테고리의 인기 1위) 50%
      차지하던 주문이 0 건 → kitchen 일별 주문 ~50% 감소 → 전체 ~12.5% 감소
    """
    rows = []
    customer_ids = [f"c_{i:04d}" for i in range(NUM_CUSTOMERS)]
    product_pool: dict[str, list[str]] = {
        cat: [
            f"p_{cat}_{i:02d}"
            for i in range(1, PRODUCTS_PER_CATEGORY + 1)
        ]
        for cat in ECOMMERCE_CATEGORIES
    }

    order_seq = 0
    for day_offset in range(ECOMMERCE_PERIOD_DAYS):
        d = ECOMMERCE_START + timedelta(days=day_offset)
        # 평소 일별 약 500 건
        base_count = 500
        # 재고 부족 시나리오: D-7 부터 kitchen 주문 약 50% ↓
        kitchen_after_outage = d >= INVENTORY_OUT_DATE

        for category in ECOMMERCE_CATEGORIES:
            cat_count = base_count // 4  # 125 건/카테고리
            if category == "kitchen" and kitchen_after_outage:
                cat_count = int(cat_count * 0.5)  # ~62 건

            for _ in range(cat_count):
                customer_id = random.choice(customer_ids)
                pids = product_pool[category]
                # kitchen 카테고리에서는 p_kitchen_01 못 사니까 (out_of_stock)
                # 다른 4 개 상품 중 균등 선택
                if category == "kitchen" and kitchen_after_outage:
                    pid = random.choice(
                        [p for p in pids if p != TOP_KITCHEN_PRODUCT]
                    )
                else:
                    pid = random.choice(pids)

                # 일정 가격 분포 (재고 부족 시나리오만 매립 — 카테고리별 주문수 변동만)
                amount = random.uniform(3000, 5000)

                rows.append((
                    f"o_{order_seq:06d}",
                    customer_id,
                    category,
                    pid,
                    round(amount, 2),
                    datetime.combine(d, datetime.min.time())
                    + timedelta(seconds=random.randint(0, 86399)),
                ))
                order_seq += 1

    conn.executemany(
        "INSERT INTO orders "
        "(order_id, customer_id, category, product_id, amount, paid_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    print(f"orders: {len(rows)}건 삽입")


def seed_ecommerce_inventory_changes(conn: duckdb.DuckDBPyConnection):
    """시점별 재고 변경 이력. 재고 부족 시나리오의 핵심 시점 신호.

    초기 상태 (D-30 시점) 모든 상품 in_stock 등록 + p_kitchen_01 의 D-7 시점
    out_of_stock 변경 기록. ④ Validator 가 ``WHERE changed_at >= ?`` 시점별
    SQL 로 재고 부족의 결정적 증거 잡을 수 있게 함.
    """
    rows = []
    seq = 0
    # 초기 등록 — 모든 상품이 데이터 시작일에 in_stock
    init_at = datetime.combine(ECOMMERCE_START, datetime.min.time())
    for category in ECOMMERCE_CATEGORIES:
        for i in range(1, PRODUCTS_PER_CATEGORY + 1):
            pid = f"p_{category}_{i:02d}"
            rows.append((
                f"ic_{seq:05d}",
                pid,
                init_at,
                "in_stock",
                "초기 등록",
            ))
            seq += 1

    # 재고 부족 시나리오 핵심: D-7 (2026-03-24) 에 p_kitchen_01 out_of_stock 전환
    rows.append((
        f"ic_{seq:05d}",
        TOP_KITCHEN_PRODUCT,
        datetime.combine(INVENTORY_OUT_DATE, datetime.min.time()),
        "out_of_stock",
        "재고 소진 (보충 일정 미정)",
    ))

    conn.executemany(
        "INSERT INTO inventory_changes "
        "(change_id, product_id, changed_at, status, note) "
        "VALUES (?, ?, ?, ?, ?)",
        rows,
    )
    print(
        f"inventory_changes: {len(rows)}건 삽입 "
        f"(p_kitchen_01 out_of_stock = {INVENTORY_OUT_DATE})"
    )


def seed_ecommerce_category_daily_revenue(conn: duckdb.DuckDBPyConnection):
    """orders 집계 → category_daily_revenue."""
    conn.execute("""
        INSERT INTO category_daily_revenue (date, category, gmv, orders)
        SELECT
            CAST(paid_at AS DATE) AS date,
            category,
            SUM(amount) AS gmv,
            COUNT(*) AS orders
        FROM orders
        GROUP BY date, category
        ORDER BY date, category
    """)
    count = conn.execute(
        "SELECT COUNT(*) FROM category_daily_revenue"
    ).fetchone()[0]
    print(f"category_daily_revenue: {count}건 삽입")


def seed_ecommerce_daily_kpi(conn: duckdb.DuckDBPyConnection):
    """orders 집계 + 모의 visitors 로 daily_kpi 생성.

    visitors 는 orders 와 **독립적으로** 일정 유지 (~15000) — 재고 부족
    시나리오의 진짜 원인이 "방문자 감소" 가 아닌 "전환율 하락 (재고 없음)"
    임을 LLM 이 추론하게 하기 위함.
    """
    # orders 집계
    rows = conn.execute("""
        SELECT
            CAST(paid_at AS DATE) AS date,
            SUM(amount) AS gmv,
            COUNT(*) AS orders,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM orders
        GROUP BY date
        ORDER BY date
    """).fetchall()

    insert_rows = []
    for d, gmv, orders, _unique in rows:
        # 방문자 = 일정 (~15000). orders 변동과 무관 — 전환율 변동으로 시나리오
        # 신호가 노출되도록 함.
        visitors = 15000 + random.randint(-500, 500)
        conversion = orders / visitors if visitors else 0.0
        # payment_success_rate — 양 도메인 안정 KPI (~0.97). 본 mock 에선 영향 X
        psr = round(0.965 + random.uniform(-0.005, 0.010), 4)
        insert_rows.append((
            d,
            round(float(gmv), 2),
            int(orders),
            round(conversion, 4),
            visitors,
            psr,
        ))

    conn.executemany(
        "INSERT INTO daily_kpi "
        "(date, gmv, orders, conversion, visitors, payment_success_rate) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        insert_rows,
    )
    print(f"daily_kpi (이커머스): {len(insert_rows)}건 삽입")


def main():
    random.seed(RANDOM_SEED)  # 재현성 보장: 매번 같은 데이터 생성

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # ── 게임 DB ─────────────────────────────────────────────────
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"기존 게임 DB 삭제: {DB_PATH}")

    conn = duckdb.connect(str(DB_PATH))
    try:
        create_tables(conn)
        seed_users(conn)
        seed_products(conn)
        seed_gateways(conn)
        seed_content_releases(conn)
        seed_releases(conn)
        seed_shop_impressions(conn)   # 노출 먼저
        seed_payments(conn)           # 노출된 상품 중에서만 구매
        seed_sessions(conn)
        seed_events(conn)
        seed_payment_errors(conn)
        seed_payment_attempts(conn)
        seed_daily_kpi(conn)
        print(f"\n게임 DB 생성 완료: {DB_PATH}")
    finally:
        conn.close()

    # ── 이커머스 DB (빈 스키마) ──────────────────────────────────
    if ECOMMERCE_DB_PATH.exists():
        ECOMMERCE_DB_PATH.unlink()
        print(f"기존 이커머스 DB 삭제: {ECOMMERCE_DB_PATH}")

    conn = duckdb.connect(str(ECOMMERCE_DB_PATH))
    try:
        create_ecommerce_tables(conn)
        seed_ecommerce_customers(conn)
        seed_ecommerce_products(conn)
        seed_ecommerce_orders(conn)
        seed_ecommerce_inventory_changes(conn)
        seed_ecommerce_category_daily_revenue(conn)
        seed_ecommerce_daily_kpi(conn)
        print(f"\n이커머스 DB 생성 완료: {ECOMMERCE_DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
