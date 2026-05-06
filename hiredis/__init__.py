# This file prevents Python from treating the vendored hiredis C library
# source directory as a Python namespace package. The actual hiredis C
# source is in this directory; install hiredis from pip for the Python
# extension module.
raise ImportError(
    "This is the vendored hiredis C source directory, not the Python hiredis package. "
    "Install hiredis via pip for Python bindings."
)
