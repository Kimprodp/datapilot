"""환경 변수 로드 및 모델 상수 정의.

Java 비유:
    @Configuration
    public class AppConfig {
        @Value("${ANTHROPIC_API_KEY}") private String apiKey;
    }

python-dotenv가 프로젝트 루트의 `.env` 파일을 읽어
`os.environ`에 주입한다. `.env`가 없어도 에러 없이 넘어간다.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")

# ──────────────────────────────────────────────────────────────────
# 에이전트별 모델 배정
# ──────────────────────────────────────────────────────────────────

#: ①②⑥ — 수치 비교·분류 중심. Sonnet이면 충분.
SONNET_MODEL = "claude-sonnet-4-5-20250514"

#: ③⑤ — 깊은 인과 추론 필요. Opus 사용.
OPUS_MODEL = "claude-opus-4-20250115"