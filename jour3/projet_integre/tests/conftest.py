import sys
from pathlib import Path

# Les scripts du projet s'importent entre eux par leur nom (comme lorsqu'ils sont lancés directement)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
