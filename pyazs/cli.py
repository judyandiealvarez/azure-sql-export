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
    print(
        "\n".join(
            [
                "usage:",
                "  azs <command> [args]",
                "  azs help <command>",
                "  azs -h | --help",
                "",
                "commands:",
                "  export    Export schema and optionally data to files",
                "  import    Import schema/data from exported files into a DB",
                "  compare   Compare exported files against a live database",
                "  copy      Copy table data from a source DB to a target DB",
                "  web       Launch the web UI for export/import/compare",
                "  sync      Sync DB objects to local .sql files for a schema",
                "  migrate   Generate migration script from DB vs local files",
            ]
        )
    )


def main() -> int:
    if len(sys.argv) < 2 or sys.argv[1] in {"-h", "--help", "help"}:
        if len(sys.argv) == 3 and sys.argv[1] == "help":
            cmd = sys.argv[2]
        else:
            _print_usage()
            return 0
    else:
        cmd = sys.argv[1]

    args = sys.argv[2:]

    if cmd not in COMMANDS:
        print(f"Unknown command: {cmd}\n")
        _print_usage()
        return 2

    module_name = COMMANDS[cmd]

    # If invoked as `azs help <cmd>`, replace args with -h
    if len(sys.argv) >= 2 and sys.argv[1] == "help":
        args = ["-h"]

    # Re-exec the module as a script to preserve original CLI behavior
    proc = subprocess.run([sys.executable, "-m", module_name, *args])
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())


