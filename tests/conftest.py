import sys
from pathlib import Path

# Import the plugin under the same package name that owns its relative imports.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
