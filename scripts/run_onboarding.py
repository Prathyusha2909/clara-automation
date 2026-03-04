from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from run_onboarding import run_onboarding_pipeline


if __name__ == "__main__":
    run_onboarding_pipeline()
