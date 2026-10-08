import platform
import time

print("Running hello.py with Python", platform.python_version())
squares = [i * i for i in range(1, 6)]
print("squares:", squares)
time.sleep(1)
