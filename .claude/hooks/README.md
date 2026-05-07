# .claude/hooks/ — Claude Code Hooks

이 폴더는 Claude Code 의 **Hook 이벤트** 에 등록되는 스크립트들을 담는다.
Hook 이란 Claude Code 내부의 특정 이벤트(사용자 프롬프트 제출, 도구 실행 후, 응답 종료 등) 시점에 **자동으로 실행되는 셸 명령** 이다.

등록은 프로젝트 루트의 `.claude/settings.json` 에서 한다.

---

## 지원 이벤트 (Claude Code)

| 이벤트 | 발동 시점 | 하니스 기본 활용 |
|---|---|---|
| `UserPromptSubmit` | 사용자가 메시지 보낸 직후 | (Phase 3 예정: auto-skill) |
| `PreToolUse` | 도구 실행 직전 | (미사용) |
| `PostToolUse` | 도구 실행 직후 | **post-edit 슬롯** (사용자 프로젝트별 린터/포맷) |
| `Stop` | AI 응답 종료 시 | **freshness** (docs/ 수정 시 Last Updated 자동 갱신) |
| `SubagentStop` | 서브에이전트 종료 시 | (미사용) |
| `Notification` | 사용자 확인 필요 알림 | (미사용) |
| `PreCompact` | 대화 압축 직전 | (향후 활용 여지) |

---

## 현재 등록된 Hook

| 파일 | 이벤트 | 역할 | 상태 |
|---|---|---|---|
| `freshness.js` | `Stop` | 이번 응답에서 수정된 `docs/*.md`, `CLAUDE.md` 의 `Last Updated` 를 오늘 날짜로 자동 갱신 | Phase 2-3 에서 활성화 |
| `post-edit.example.js` | `PostToolUse` | (예시) Edit/Write 후 원하는 린터/포맷 실행. **실사용 시 복사해서 프로젝트에 맞게 커스터마이즈** | 예시 파일 (미등록) |

---

## 사용자 프로젝트별 커스터마이즈

하니스를 새 프로젝트에 복사한 뒤, 해당 프로젝트에서:

### 1. 기본 Hook (freshness) 그대로 사용
추가 설정 불필요. `settings.json` 에 이미 등록됨.

### 2. post-edit 커스터마이즈 (선택)

`post-edit.example.js` 를 복사:

```bash
cp .claude/hooks/post-edit.example.js .claude/hooks/post-edit.js
```

파일을 열어 프로젝트에 맞는 린터/포맷 명령을 채운다. 예:

- TypeScript 프로젝트 → `npx prettier --write <파일>`
- Python 프로젝트 → `black <파일>`
- Flutter 프로젝트 → `dart format <파일>`

그리고 `.claude/settings.json` 의 `PostToolUse` 섹션을 활성화 (주석 해제 또는 복사).

### 3. 완전 커스텀 Hook 추가

새 `.js` 또는 `.sh` 파일을 이 폴더에 만들고 `settings.json` 에 등록.

---

## 런타임 요구

Hook 스크립트는 기본적으로 **Node.js** 로 작성 (크로스 플랫폼).
Bash/PowerShell 스크립트도 가능하나, 하니스 기본 제공 Hook 은 모두 Node.js.

- **필요 버전**: Node.js 18+ (네이티브 `fs.readFileSync` 등 표준 API 만 사용)
- **외부 의존성**: 없음 (`npm install` 불필요)

---

## 주의사항

1. **Hook 은 동기적**. 느린 스크립트는 AI 응답 지연을 유발.
2. **표준 출력** 은 AI 컨텍스트에 주입될 수 있음. 민감 정보 출력 금지.
3. **종료 코드 0 이 아니면** Claude Code 가 경고. 에러는 stderr 로.
4. **경로는 프로젝트 루트 기준**. Hook 은 프로젝트 루트에서 실행됨.

---

## 참고

- Claude Code 공식 문서: <https://docs.claude.com/en/docs/claude-code/hooks>
- 설계 기준: diet103/claude-code-infrastructure-showcase (u/JokeGold5455)
