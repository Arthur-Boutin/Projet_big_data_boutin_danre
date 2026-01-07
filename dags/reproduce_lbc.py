import sys
import os

# Add paths to simulate Airflow environment
current_dir = os.path.dirname(os.path.abspath(__file__))
# dags/lib/reproduce... -> dags/lib
lib_dir = os.path.join(current_dir, 'lib')
if lib_dir not in sys.path:
    sys.path.append(lib_dir)

# Add lbc path as my fix did logic inside fetcher, but here I need it globally or need to mock the structure
# The fix in lbc_fetcher.py handles sys.path insertion itself when imported/run.

print("Importing lbc_fetcher...")
try:
    from lib.lbc_fetcher import fetch_lbc_data
    print("Import check: Success")
except ImportError as e:
    # If standard import fails (because we are running from dags root usually), try adjusting path here
    # My script will be in dags/
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
