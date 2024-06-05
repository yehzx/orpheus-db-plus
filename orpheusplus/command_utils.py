import subprocess
import time
import pydoc


YELLOW = "\033[93m"
CYAN = "\033[96m"
OFF = "\033[00m"


def execute(cmd, raise_error=False):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, encoding="utf-8")
    # Some systems may raise UnicodeDecodeError when encoding is not utf-8
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if raise_error:
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)


def command(stmt, sleep=0):
    print(f"{YELLOW}{stmt}{OFF}")
    for output in execute(stmt):
        print(f"{output}", end="")
    time.sleep(sleep)


def page_print(text):
    pydoc.pager(text)