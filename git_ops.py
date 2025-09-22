#!/usr/bin/env python3
"""
Lightweight Git operations using GitPython.

Provides programmatic functions to stage, commit, and push changes.
"""

from typing import Optional, List
from pathlib import Path
from git import Repo, RemoteProgress, GitCommandError


class _SimpleProgress(RemoteProgress):
    def update(self, op_code, cur_count, max_count=None, message=''):
        # Intentionally minimal; hook for logging if needed
        pass


def open_repo(repo_path: str | Path = '.') -> Repo:
    path = Path(repo_path).resolve()
    if not (path / '.git').exists():
        # Initialize if missing
        repo = Repo.init(path)
    else:
        repo = Repo(path)
    return repo


def ensure_remote(repo: Repo, name: str, url: Optional[str]) -> None:
    try:
        repo.remote(name)
    except ValueError:
        if not url:
            raise ValueError(f"Remote '{name}' does not exist and no URL provided to create it.")
        repo.create_remote(name, url)


def stage_all(repo: Repo, patterns: Optional[List[str]] = None) -> None:
    if patterns:
        repo.git.add(*patterns)
    else:
        repo.git.add('-A')


def commit(repo: Repo, message: str) -> bool:
    if not message or not message.strip():
        raise ValueError("Commit message is required")
    # Commit only if there are staged or unstaged changes
    if repo.is_dirty(untracked_files=True):
        # Ensure staged
        try:
            repo.index.commit(message)
            return True
        except GitCommandError as e:
            raise RuntimeError(f"Git commit failed: {e}")
    return False


def push(repo: Repo, remote_name: str = 'origin', refspec: Optional[str] = None, set_upstream: bool = True) -> str:
    if refspec is None:
        # Default to current HEAD
        head = repo.active_branch.name if not repo.head.is_detached else 'HEAD'
        refspec = f"{head}:{head}" if head != 'HEAD' else 'HEAD'

    remote = repo.remote(remote_name)
    kwargs = {}
    if set_upstream and not repo.active_branch.tracking_branch():
        kwargs['set_upstream'] = True

    results = remote.push(refspec, progress=_SimpleProgress(), **kwargs)
    summaries = [str(r.summary) for r in results]
    return "\n".join(summaries)


def stage_commit_push(
    repo_path: str | Path = '.',
    message: str = 'chore: update',
    remote: str = 'origin',
    url: Optional[str] = None,
    refspec: Optional[str] = None,
    patterns: Optional[List[str]] = None,
) -> str:
    repo = open_repo(repo_path)
    ensure_remote(repo, remote, url)
    stage_all(repo, patterns)
    committed = commit(repo, message)
    return push(repo, remote_name=remote, refspec=refspec)


if __name__ == '__main__':
    # Simple manual test
    print(stage_commit_push('.', 'chore: test push'))




