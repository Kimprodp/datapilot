"""
Mock 데이터 생성 스크립트

DuckDB 파일(data/datapilot_mock.db)에 12개 테이블을 생성하고
Pizza Ready 30일치 Mock 데이터를 삽입한다.
3개 이상 시나리오가 매립되어 있다.

실행: uv run python scripts/seed_mock_data.py
"""

import random
from datetime import date, timedelta

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "datapilot_mock.db"
BASE_DATE = date(2026, 3, 31)   # 데이터 기간 마지막 날
PERIOD_DAYS = 30                # 30일치
NUM_USERS = 2000                # Mock 유저 수 (DAU 규모를 축소한 데모용)
START_DATE = BASE_DATE - timedelta(days=PERIOD_DAYS - 1)  # 2026-03-02

# 시나리오 핵심 시점
UI_CHANGE_DATE = BASE_DATE - timedelta(days=3)     # D-3: Android UI 변경 (2026-03-28)
EVENT_END_DATE = BASE_DATE - timedelta(days=14)    # D-14: 시즌 이벤트 종료 (2026-03-17)
PG_OUTAGE_DATE = BASE_DATE                         # D-0: PG 장애 (2026-03-31)

# 상품 가격 매핑 (원)
PRODUCT_PRICES = {
    "p_001": 100,  "p_002": 500,  "p_003": 900,    # low
    "p_004": 2500, "p_005": 2000,                  # mid
    "p_006": 5000, "p_007": 9900, "p_008": 7900,   # premium
}
PREMIUM_PRODUCTS = ["p_006", "p_007", "p_008"]
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
    """유저 마스터 데이터 생성 (2000명)"""

    # 분포 가중치 리스트 — random.choice가 이 리스트에서 하나를 뽑으면 자연스럽게 비율이 맞음
    # Random.nextInt로 뽑는 것과 동일
    platforms = ["android"] * 60 + ["ios"] * 40                         # 60:40
    countries = (["brazil"] * 15 + ["usa"] * 25 + ["korea"] * 20
                 + ["japan"] * 15 + ["india"] * 10 + ["others"] * 15)   # 비율 합 100
    devices = ["low"] * 30 + ["mid"] * 40 + ["high"] * 30               # 30:40:30

    users = []
    for i in range(NUM_USERS):
        # new = 최근 7일 이내 설치, existing = 그 이전
        is_new = random.random() < 0.3  # 30% 확률로 new
        if is_new:
            install = BASE_DATE - timedelta(days=random.randint(0, 6))
            user_type = "new"
        else:
            install = BASE_DATE - timedelta(days=random.randint(7, 90))
            user_type = "existing"

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
        ("c_001", "season_event", "Pizza Festival Season 3",  date(2026, 2, 10), date(2026, 3, 17)),
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
    """결제 데이터 30일치. D-3 이후 Android premium 결제 80% 감소."""

    users = conn.execute("SELECT user_id, platform FROM users").fetchall()

    payments = []
    pid = 0

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        is_after_change = current_date >= UI_CHANGE_DATE

        for user_id, platform in users:
            if random.random() > 0.07:  # 매일 유저의 7%가 결제
                continue

            # 상품 선택: 정상 시 premium 30%, D-3 이후 Android는 premium 6%로 감소
            if is_after_change and platform == "android":
                product = (random.choice(PREMIUM_PRODUCTS) if random.random() < 0.06
                           else random.choice(NON_PREMIUM_PRODUCTS))
            else:
                product = (random.choice(PREMIUM_PRODUCTS) if random.random() < 0.30
                           else random.choice(NON_PREMIUM_PRODUCTS))

            hour = random.randint(8, 23)
            minute = random.randint(0, 59)

            payments.append((
                f"pay_{pid:06d}",
                user_id,
                product,
                PRODUCT_PRICES[product],
                f"{current_date} {hour:02d}:{minute:02d}:00",
                "success",
            ))
            pid += 1

    conn.executemany("INSERT INTO payments VALUES (?, ?, ?, ?, ?, ?)", payments)
    print(f"payments: {len(payments)}건 삽입")


def seed_shop_impressions(conn: duckdb.DuckDBPyConnection):
    """상점 노출 로그 30일치. 유저별 스크롤 깊이(3~8)만큼만 노출 기록.
    D-3 이후 Android는 premium이 하단(10~15)으로 밀려서 자연스럽게 노출 급감."""

    users = conn.execute("SELECT user_id, platform FROM users").fetchall()

    impressions = []
    iid = 0

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        is_after_change = current_date >= UI_CHANGE_DATE

        for user_id, platform in users:
            if random.random() > 0.25:  # 매일 유저의 25%가 상점 방문
                continue

            # 각 상품의 slot_order 결정
            product_slots = []
            for product_id in ALL_PRODUCTS:
                is_premium = product_id in PREMIUM_PRODUCTS

                if is_after_change and platform == "android" and is_premium:
                    slot = random.randint(10, 15)   # UI 변경 후: 하단으로 밀림
                elif is_premium:
                    slot = random.randint(1, 3)     # 정상: 상단
                else:
                    slot = random.randint(4, 8)     # 일반 상품: 중하단

                product_slots.append((product_id, slot))

            # slot_order 순 정렬 → 스크롤 깊이만큼만 노출
            product_slots.sort(key=lambda x: x[1])
            scroll_depth = random.randint(3, 8)  # 3개는 무조건, 최대 8개
            visible = product_slots[:scroll_depth]

            hour = random.randint(9, 22)
            minute = random.randint(0, 59)

            for product_id, slot_order in visible:
                impressions.append((
                    f"imp_{iid:07d}",
                    user_id,
                    product_id,
                    f"{current_date} {hour:02d}:{minute:02d}:00",
                    slot_order,
                ))
                iid += 1

    conn.executemany("INSERT INTO shop_impressions VALUES (?, ?, ?, ?, ?)", impressions)
    print(f"shop_impressions: {len(impressions)}건 삽입")


# ── 시나리오 2 트랜잭션: D7 리텐션 -12% (이벤트 종료) ──

def seed_sessions(conn: duckdb.DuckDBPyConnection):
    """세션 로그 30일치. D-14 이후 기존 유저 접속률 30% 감소."""

    users = conn.execute("SELECT user_id, platform, user_type FROM users").fetchall()

    sessions = []
    sid = 0

    for day_offset in range(PERIOD_DAYS):
        current_date = START_DATE + timedelta(days=day_offset)
        is_after_event_end = current_date >= EVENT_END_DATE

        for user_id, platform, user_type in users:
            # 접속 확률: 정상 30%, D-14 이후 기존 유저만 21%로 감소 (30% 하락)
            login_rate = 0.30
            if is_after_event_end and user_type == "existing":
                login_rate = 0.21

            if random.random() > login_rate:
                continue

            hour = random.randint(7, 23)
            minute = random.randint(0, 59)
            duration_min = random.randint(3, 30)  # 3~30분

            sessions.append((
                f"s_{sid:06d}",
                user_id,
                f"{current_date} {hour:02d}:{minute:02d}:00",
                f"{current_date} {hour:02d}:{min(minute + duration_min, 59):02d}:00",
                platform,
            ))
            sid += 1

    conn.executemany("INSERT INTO sessions VALUES (?, ?, ?, ?, ?)", sessions)
    print(f"sessions: {len(sessions)}건 삽입")


def seed_events(conn: duckdb.DuckDBPyConnection):
    """인게임 이벤트 로그. 세션 기반으로 생성.
    - stage_clear: 세션당 50% 확률로 1회
    - event_participate: 이벤트 기간(~D-14) 중에만 발생, 이후 0건으로 급감"""

    sessions_data = conn.execute(
        "SELECT session_id, user_id, session_start FROM sessions"
    ).fetchall()

    events = []
    eid = 0

    for session_id, user_id, session_start in sessions_data:
        session_date_str = str(session_start)[:10]

        # stage_clear (세션당 50% 확률로 1회)
        if random.random() < 0.5:
            events.append((
                f"e_{eid:07d}", user_id, "stage_clear",
                str(session_start), None,
            ))
            eid += 1

        # event_participate (이벤트 기간 중에만, 세션당 40% 확률)
        if session_date_str <= str(EVENT_END_DATE) and random.random() < 0.4:
            events.append((
                f"e_{eid:07d}", user_id, "event_participate",
                str(session_start), '{"event": "pizza_festival_s3"}',
            ))
            eid += 1

    conn.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?)", events)
    print(f"events: {len(events)}건 삽입")


def main():
    # 기존 DB 파일 있으면 삭제 후 재생성
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"기존 DB 삭제: {DB_PATH}")

    conn = duckdb.connect(str(DB_PATH))

    try:
        create_tables(conn)
        seed_users(conn)
        seed_products(conn)
        seed_gateways(conn)
        seed_content_releases(conn)
        seed_releases(conn)
        seed_payments(conn)
        seed_shop_impressions(conn)
        seed_sessions(conn)
        seed_events(conn)
        # TODO: 시나리오 3 트랜잭션 (payment_attempts, payment_errors)
        # TODO: daily_kpi 집계
        print(f"\nDB 생성 완료: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
