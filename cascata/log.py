import logging
import sys
import contextvars
import hashlib
from .component import Component

# ANSI escape codes
RESET = "\033[0m"
LEVEL_COLORS = {
    'debug': "\033[34m",   # blue
    'info': "\033[36m",    # cyan
    'warn': "\033[33m",    # yellow
    'warning': "\033[33m", # alias for warning
    'error': "\033[31m",   # red
}

# Generate a bright ANSI 256-color code deterministically from component class name
def _get_component_color(class_name: str) -> str:
    """
    Generate a bright, bold ANSI 256-color code deterministically from the component class name using hash.
    """
    digest = hashlib.md5(class_name.encode('utf-8')).digest()
    # Use two bytes to spread across the 216-color cube (starting at 16)
    idx = (digest[0] << 8 | digest[1]) % 216  # range 0-215
    code = 16 + idx  # ANSI 256-color code: 16-231 are the color cube
    return f"\033[38;5;{code}m"

# Context var to track current component
_current_component = contextvars.ContextVar('current_component', default=None)

# Monkey-patch Component.run to set context for logging
_original_run = Component.run
async def _run_with_context(self, *args, **kwargs):
    token = _current_component.set(self)
    try:
        return await _original_run(self, *args, **kwargs)
    finally:
        _current_component.reset(token)
Component.run = _run_with_context

class ContextAwareHandler(logging.StreamHandler):
    def emit(self, record):
        try:
            msg = record.getMessage()
            level = record.levelname.lower()
            level_color = LEVEL_COLORS.get(level, "")
            reset = RESET
            comp = _current_component.get()
            if comp is not None:
                comp_name = comp.group_name if getattr(comp, 'group_name', None) is not None else comp.name
                comp_class = comp.__class__.__name__
                comp_color = _get_component_color(comp_class)
                prefix = f"{level_color}{record.levelname}{reset} - {comp_name}{comp_color}@{comp_class}{reset}: "
            else:
                prefix = f"{level_color}{record.levelname}{reset} - Cascata: "
            stream = self.stream or sys.stdout
            stream.write(f"{prefix}{msg}\n")
            self.flush()
        except Exception:
            self.handleError(record)

# Configure module-level logger
log = logging.getLogger('cascata')
log.setLevel(logging.DEBUG)
log.handlers.clear()
handler = ContextAwareHandler()
log.addHandler(handler)
log.propagate = False
