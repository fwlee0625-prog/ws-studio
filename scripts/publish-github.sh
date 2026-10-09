#!/usr/bin/env bash
# 在本地一次性创建 GitHub 仓库，并推送 WS Studio V2 源码。
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v gh >/dev/null 2>&1; then
  echo '错误：未安装 GitHub CLI。请先安装：https://cli.github.com/' >&2
  exit 1
fi
if ! gh auth status >/dev/null 2>&1; then
  echo '错误：GitHub CLI 尚未登录。请先运行：gh auth login' >&2
  exit 1
fi
if ! command -v git >/dev/null 2>&1; then
  echo '错误：未安装 Git。' >&2
  exit 1
fi

OWNER="${GITHUB_OWNER:-$(gh api user --jq .login)}"
REPO="${GITHUB_REPO:-ws-studio-v2}"
VISIBILITY="${GITHUB_VISIBILITY:-private}"
if [[ "$VISIBILITY" != private && "$VISIBILITY" != public ]]; then
  echo 'GITHUB_VISIBILITY 必须为 private 或 public。' >&2
  exit 1
fi

if [[ ! -d .git ]]; then
  git init -b main
fi
git branch -M main
git add .
if ! git diff --cached --quiet; then
  git commit -m 'feat: initial WS Studio V2 recorder and publisher'
fi

URL="https://github.com/${OWNER}/${REPO}"
if gh repo view "${OWNER}/${REPO}" >/dev/null 2>&1; then
  echo "仓库已存在，准备推送：${URL}"
  if git remote get-url origin >/dev/null 2>&1; then
    EXISTING_REMOTE="$(git remote get-url origin)"
    case "$EXISTING_REMOTE" in
      "${URL}"|"${URL}.git"|"git@github.com:${OWNER}/${REPO}.git"|"git@github.com:${OWNER}/${REPO}") ;;
      *) echo "错误：origin 已经指向另一个仓库：${EXISTING_REMOTE}" >&2; exit 1 ;;
    esac
  else
    git remote add origin "${URL}.git"
  fi
  git push -u origin main
else
  echo "创建 ${VISIBILITY} 仓库：${URL}"
  gh repo create "${OWNER}/${REPO}" "--${VISIBILITY}" --source=. --remote=origin --push
fi
printf '\n发布完成：%s\n' "$URL"
