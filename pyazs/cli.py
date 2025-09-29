import sys
import subprocess
from pathlib import Path


COMMANDS = {
    "export": "pyazs.export",
    "import": "pyazs.imp",
    "compare": "pyazs.compare",
    "copy": "pyazs.copy",
    "web": "pyazs.web",
    "migrate": "pyazs.migrate",
    "sync": "pyazs.sync",
}


def _print_usage() -> None:
    print("usage: azs [export|import|compare|copy|web] ...")


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help", "help"}:
        _print_usage()
        return 0

    cmd = sys.argv[1]
    args = sys.argv[2:]

    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}\n")
        _print_usage()
        return 2

    module_name = COMMANDS[cmd]
    # Re-exec the module as a script to preserve original CLI behavior
    proc = subprocess.run([sys.executable, "-m", module_name, *args])
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())


