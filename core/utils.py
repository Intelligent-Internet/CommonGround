from pathlib import Path
import sys, asyncio
import unicodedata
import re
from typing import Any, Optional

REPO_ROOT = Path(__file__).resolve().parents[1]
_TARGET_SAFE_RE = re.compile(r"[^A-Za-z0-9_-]+")

def set_loop_policy():
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def set_python_path():
    repo_root = str(REPO_ROOT)
    if repo_root not in sys.path:
        sys.path.append(repo_root)

    packages_path = REPO_ROOT / "packages"
    cardbox_path = REPO_ROOT / "card-box-cg"

    for path in (packages_path, cardbox_path):
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.append(path_str)
            
def safe_str(obj: Any, attr_name: Optional[str] = None) -> Optional[str]:
    """
    Safely convert an attribute of an object or the object itself to a string.
    
    If attr_name is provided:
        Returns str(obj.attr_name) if obj.attr_name is not None.
        Returns None if obj.attr_name is None or obj is None.
        
    If attr_name is NOT provided:
        Returns str(obj) if obj is not None.
        Returns None if obj is None.
    """
    if obj is None:
        return None
        
    if attr_name:
        val = getattr(obj, attr_name, None)
    else:
        val = obj
        
    return str(val) if val is not None else None

def normalize_label(raw: Any) -> str:
    """
    Normalizes a label string. 
    Returns an empty string if input is None or whitespace-only.
    Raises ValueError if the label contains control characters.
    """
    val = safe_str(raw)
    if not val:
        return ""
    val = val.strip()
    
    # Check for control characters
    for ch in val:
        cat = unicodedata.category(ch)
        if cat in ("Cc", "Cs"):
            raise ValueError(f"Label contains control characters")
            
    return val

def safe_target_label(value: Any) -> str:
    """
    Normalize a target label for NATS queue/durable names.
    Non [A-Za-z0-9_-] chars are replaced with "_".
    """
    text = "" if value is None else str(value)
    safe = _TARGET_SAFE_RE.sub("_", text)
    return safe or "_"
