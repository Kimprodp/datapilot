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
            metadata    VARCHAR              -- JSON 문자열
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
    devices = ["low"] * 30 + ["mid"] * 40 + ["high"] * 30              # 30:40:30

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


def main():
    # 기존 DB 파일 있으면 삭제 후 재생성
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"기존 DB 삭제: {DB_PATH}")

    conn = duckdb.connect(str(DB_PATH))

    try:
        create_tables(conn)
        seed_users(conn)
        # TODO: 마스터 데이터 삽입
        # TODO: 트랜잭션 데이터 삽입
        # TODO: daily_kpi 집계
        print(f"\nDB 생성 완료: {DB_PATH}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()