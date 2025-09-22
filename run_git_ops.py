#!/usr/bin/env python3
"""
CLI wrapper for git_ops: stage, commit, and push using GitPython.
"""

import argparse
from pathlib import Path
from git_ops import stage_commit_push


def main():
    parser = argparse.ArgumentParser(description='Stage, commit, and push changes using GitPython')
    parser.add_argument('--repo', default='.', help='Repository path (default: current directory)')
    parser.add_argument('--message', required=True, help='Commit message')
    parser.add_argument('--remote', default='origin', help='Remote name (default: origin)')
    parser.add_argument('--url', help='Remote URL (if remote does not exist)')
    parser.add_argument('--refspec', help='Refspec to push (default: current branch)')
    parser.add_argument('--patterns', nargs='*', help='Optional path patterns to add instead of -A')

    args = parser.parse_args()

    output = stage_commit_push(
        repo_path=Path(args.repo),
        message=args.message,
        remote=args.remote,
        url=args.url,
        refspec=args.refspec,
        patterns=args.patterns,
    )
    print(output)


if __name__ == '__main__':
    main()




