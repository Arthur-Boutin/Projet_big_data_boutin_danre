import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_dir = os.path.join(current_dir, 'lib')
if lib_dir not in sys.path:
    sys.path.append(lib_dir)

print("Importing lbc_fetcher...")
try:
    from lib.lbc_fetcher import fetch_lbc_data
    print("Import check: Success")
except ImportError as e:
    sys.path.append(os.path.join(current_dir, 'lib')) 
    from lbc_fetcher import fetch_lbc_data
    print("Import check with fix: Success")

print("Running fetch_lbc_data...")
try:
    fetch_lbc_data()
    print("Execution Success")
except Exception as e:
    print(f"CRASH CAUGHT: {e}")
    import traceback
    traceback.print_exc()
