import argparse
from pathlib import Path

try:
    from .clara_pipeline import configure_logging, run_all
except ImportError:  # pragma: no cover - direct script execution
    from clara_pipeline import configure_logging, run_all


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Clara automation pipeline on demo + onboarding transcripts."
    )
    parser.add_argument(
        "--demo-input",
        "--demo-dir",
        dest="demo_input",
        default="inputs/demo",
        help="Directory containing demo transcript .txt files.",
    )
    parser.add_argument(
        "--onboarding-input",
        "--onboarding-dir",
        dest="onboarding_input",
        default="inputs/onboarding",
        help="Directory containing onboarding transcript .txt/.json files.",
    )
    parser.add_argument(
        "--output",
        "--accounts-root",
        dest="output_dir",
        default="outputs/accounts",
        help="Directory where per-account outputs are stored.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild outputs even if v1/v2 artifacts already exist.",
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
    run_all(
        demo_dir=Path(args.demo_input),
        onboarding_dir=Path(args.onboarding_input),
        accounts_root=Path(args.output_dir),
        force=args.force,
    )


if __name__ == "__main__":
    main()
