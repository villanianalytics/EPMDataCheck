from app.core.logging_engine import *
import sys
from app.api.initializeFastAPI import *
from cli.initializeCLI import run_cli


def main():
    if len(sys.argv) > 1:
        # If there are command-line arguments, assume we want to run the CLI
        run_cli()
    else:
        # If no command-line arguments, run the FastAPI server
        init_fastapi()


if __name__ == '__main__':
    main()