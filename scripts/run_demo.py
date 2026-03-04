import argparse
from pathlib import Path

try:
    from .clara_pipeline import configure_logging, run_pipeline_a
except ImportError:  # pragma: no cover - direct script execution
    from clara_pipeline import configure_logging, run_pipeline_a


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Pipeline A: demo transcript -> v1 memo + agent spec."
    )
    parser.add_argument(
        "--demo-dir",
        default="inputs/demo",
        help="Directory containing demo transcript .txt files.",
    )
    parser.add_argument(
        "--accounts-root",
        default="outputs/accounts",
        help="Directory where per-account outputs are stored.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    run_pipeline_a(
        demo_dir=Path(args.demo_dir),
        accounts_root=Path(args.accounts_root),
    )


if __name__ == "__main__":
    main()
