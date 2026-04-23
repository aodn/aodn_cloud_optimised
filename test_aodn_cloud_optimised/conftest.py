import os

# On macOS with Homebrew, libudunits2 lives in /opt/homebrew/lib which is not
# in the default ctypes search path.  Set DYLD_LIBRARY_PATH before any test
# module is imported so that cfunits (imported transitively by dataset_config)
# can find the library at collection time.
if os.uname().sysname == "Darwin":
    existing = os.environ.get("DYLD_LIBRARY_PATH", "")
    homebrew_lib = "/opt/homebrew/lib"
    if homebrew_lib not in existing.split(":"):
        os.environ["DYLD_LIBRARY_PATH"] = (
            f"{homebrew_lib}:{existing}" if existing else homebrew_lib
        )
