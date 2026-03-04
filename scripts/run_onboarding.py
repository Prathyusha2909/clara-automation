import argparse
from pathlib import Path

try:
    from .clara_pipeline import configure_logging, run_pipeline_b
except ImportError:  # pragma: no cover - direct script execution
    from clara_pipeline import configure_logging, run_pipeline_b


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Pipeline B: onboarding transcript -> v2 memo + agent spec + changes."
    )
    parser.add_argument(
        "--input",
        "--onboarding-dir",
        dest="input_dir",
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
        help="Rebuild v2 outputs even if they already exist.",
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
    run_pipeline_b(
        onboarding_dir=Path(args.input_dir),
        accounts_root=Path(args.output_dir),
        force=args.force,
    )


if __name__ == "__main__":
    main()
