"""캐싱 작동 + 메트릭 추출 정확도 검증 (저비용).

같은 system block 으로 LLM 2회 호출 (1회차 cache_creation, 2회차 cache_read).
raw usage_metadata 구조도 출력해 키 이름을 확인한다.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langchain_anthropic import ChatAnthropic  # noqa: E402
from langchain_core.messages import HumanMessage, SystemMessage  # noqa: E402

from datapilot.config import ANTHROPIC_API_KEY, MAX_TOKENS, SONNET_MODEL  # noqa: E402

# 1024 토큰 충족하는 충분히 긴 system 텍스트 (DataPilot SYSTEM_PROMPT 흉내)
LONG_SYSTEM_TEXT = ("너는 게임 데이터 분석 전문가다. " * 200)  # 약 1200~1400 토큰


def main() -> None:
    llm = ChatAnthropic(
        model=SONNET_MODEL,
        api_key=ANTHROPIC_API_KEY,
        max_tokens=200,
        temperature=0.3,
    )
    system = SystemMessage(content=[
        {
            "type": "text",
            "text": LONG_SYSTEM_TEXT,
            "cache_control": {"type": "ephemeral"},
        }
    ])
    user = HumanMessage(content="아무 숫자 하나만 답해.")

    for i in (1, 2):
        print(f"\n=== run {i} ===")
        resp = llm.invoke([system, user])
        meta = resp.usage_metadata or {}
        print(f"  usage_metadata 전체:")
        print(f"  {json.dumps(meta, ensure_ascii=False, indent=2)}")
        print(f"  response_metadata.usage:")
        raw = (resp.response_metadata or {}).get("usage", {})
        print(f"  {json.dumps(raw, ensure_ascii=False, indent=2)}")


if __name__ == "__main__":
    main()
