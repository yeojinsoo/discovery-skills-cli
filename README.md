# discovery-skills

Claude Code 커스텀 스킬 매니저 CLI.

[discovery-skills-registry](https://github.com/yeojinsoo/discovery-skills-registry)에 등록된 스킬을 `~/.claude/skills/`에 설치하고 관리합니다.

## Install

```bash
# macOS / Linux
curl -fsSL https://github.com/yeojinsoo/discovery-skills-cli/releases/latest/download/discovery-skills-installer.sh | sh
```

> Rust 설치 불필요. 사전 빌드된 바이너리가 다운로드됩니다.

## Usage

```bash
# 사용 가능한 스킬 전체 설치
discovery-skills install

# 특정 스킬 설치
discovery-skills install logical-analysis

# 설치된 스킬 목록
discovery-skills list

# 스킬 업데이트
discovery-skills update

# 특정 스킬 업데이트
discovery-skills update project-planner

# 스킬 삭제
discovery-skills uninstall logical-analysis

# 도움말
discovery-skills --help
```

## Available Skills

| 스킬 | 설명 |
|------|------|
| `logical-analysis` | 개념이나 시스템을 논리적으로 완전 해체하는 분석 스킬 |
| `project-planner` | 실행 계획 생성/실행/수정 통합 스킬 |

## How It Works

```
discovery-skills-registry (GitHub)
  registry.toml ──GET──→ CLI가 최신 버전 확인
  Release tar.gz ──GET──→ ~/.claude/skills/{name}/ 에 설치
                          ~/.claude/skills/.skill-manager.toml 에 기록
```

1. `registry.toml`에서 스킬 목록과 버전 확인
2. GitHub Release에서 스킬별 `tar.gz` 다운로드
3. `~/.claude/skills/`에 압축 해제
4. `.skill-manager.toml`에 설치 정보 기록

## Build from Source

```bash
# Rust 필요
cargo install --git https://github.com/yeojinsoo/discovery-skills-cli
```

## License

MIT
