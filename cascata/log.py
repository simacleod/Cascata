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

# Generate a bright ANSI color code deterministically from component class name
def _get_component_color(class_name: str) -> str:
    # ANSI bright colors range from 91 to 97
    digest = hashlib.md5(class_name.encode('utf-8')).digest()
    # Use first byte to pick 1-7
    idx = (digest[0] % 7) + 1
    code = 90 + idx
    return f"\033[{code}m"

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
            level_name = record.levelname.lower()
            level_color = LEVEL_COLORS.get(level_name, "")
            reset = RESET
            comp = _current_component.get()
            if comp is not None:
                comp_name = comp.name
                comp_class = comp.__class__.__name__
                comp_color = _get_component_color(comp_class)
                prefix = f"{level_color}{record.levelname}{reset} - {comp_name}@{comp_color}{comp_class}{reset} : "
            else:
                prefix = f"{level_color}{record.levelname}{reset} - Cascata: "
            stream = self.stream or sys.stdout
            stream.write(f"{prefix}{msg}\n")
            self.flush()
        except Exception:
            self.handleError(record)

# Configure module-level logger\
log = logging.getLogger('cascata')
log.setLevel(logging.DEBUG)
log.handlers.clear()
handler = ContextAwareHandler()
log.addHandler(handler)
log.propagate = False
