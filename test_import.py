# test_import.py
import sys
print("Python path:")
for p in sys.path:
    print(f"  - {p}")

print("\nTrying to import utils...")
try:
    import utils
    print("Success! utils module found")
    print(f"utils location: {utils.__file__}")
except ImportError as e:
    print(f"Failed: {e}")

print("\nDirectory listing:")
import os
print(os.listdir('.'))
if os.path.exists('utils'):
    print("\nContents of utils directory:")
    print(os.listdir('utils'))