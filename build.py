"""
Build pipeline — PyArmor obfuscation + Nuitka single-exe compilation.

Usage:
  python build.py                    # full build
  python build.py --no-pyarmor       # skip obfuscation
  python build.py --no-nuitka        # skip compilation
  python build.py --version 1.2.0    # override version

Output:
  dist/datavalidation-agent.exe
  dist/datavalidation-agent-v<VERSION>.zip   <- ship this to the client
"""

import argparse
import os
import shutil
import subprocess
import sys
import zipfile

ROOT = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(ROOT, "dist")
TMP = os.path.join(ROOT, "_build_tmp")
DEFAULT_VERSION = "1.0.0"

SOURCES = ["main.py", "agent", "workspace", "license"]
INCLUDE_PACKAGES = [
    "agent", "workspace", "license", "fastapi", "uvicorn", "starlette",
    "pandas", "numpy", "openpyxl", "pyarrow", "boto3", "botocore", "chardet",
    "jinja2", "dotenv", "jwt", "apscheduler", "pdfplumber",
]


def run(cmd):
    print(">", " ".join(cmd))
    subprocess.check_call(cmd)


def step_pyarmor(src_dir):
    out = os.path.join(TMP, "obfuscated")
    if os.path.exists(out):
        shutil.rmtree(out)
    run([sys.executable, "-m", "pyarmor", "gen", "--output", out, "--recursive",
         *[os.path.join(src_dir, s) for s in SOURCES]])
    return out


def step_nuitka(src_dir, version):
    os.makedirs(DIST, exist_ok=True)
    cmd = [
        sys.executable, "-m", "nuitka",
        "--standalone", "--onefile",
        f"--output-dir={DIST}",
        "--output-filename=datavalidation-agent.exe",
        "--windows-console-mode=disable",
        '--product-name=Data Validation Agent',
        f"--product-version={version}",
        "--python-flag=no_docstrings",
        "--python-flag=no_asserts",
        "--include-data-dir=" + os.path.join(ROOT, "templates") + "=templates",
        "--assume-yes-for-downloads",
    ]
    for pkg in INCLUDE_PACKAGES:
        cmd.append(f"--include-package={pkg}")
    cmd.append(os.path.join(src_dir, "main.py"))
    run(cmd)


def step_package(version):
    pkg_dir = os.path.join(DIST, f"datavalidation-agent-v{version}")
    if os.path.exists(pkg_dir):
        shutil.rmtree(pkg_dir)
    os.makedirs(pkg_dir)

    exe = os.path.join(DIST, "datavalidation-agent.exe")
    if os.path.exists(exe):
        shutil.copy(exe, pkg_dir)
    shutil.copytree(os.path.join(ROOT, "templates"),
                    os.path.join(pkg_dir, "templates"))
    for f in ("INSTALL.md", ".env.template"):
        src = os.path.join(ROOT, f)
        if os.path.exists(src):
            shutil.copy(src, pkg_dir)
    web_config = os.path.join(ROOT, "deploy", "web.config.compiled")
    if os.path.exists(web_config):
        shutil.copy(web_config, os.path.join(pkg_dir, "web.config"))

    zip_path = os.path.join(DIST, f"datavalidation-agent-v{version}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for base, _, files in os.walk(pkg_dir):
            for name in files:
                full = os.path.join(base, name)
                zf.write(full, os.path.relpath(full, pkg_dir))
    print(f"\nPackaged: {zip_path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-pyarmor", action="store_true")
    ap.add_argument("--no-nuitka", action="store_true")
    ap.add_argument("--version", default=DEFAULT_VERSION)
    args = ap.parse_args()

    os.makedirs(TMP, exist_ok=True)
    src_dir = ROOT
    if not args.no_pyarmor:
        src_dir = step_pyarmor(ROOT)
    else:
        print("Skipping PyArmor.")

    if not args.no_nuitka:
        step_nuitka(src_dir, args.version)
    else:
        print("Skipping Nuitka.")

    step_package(args.version)
    print("\nBuild complete.")


if __name__ == "__main__":
    main()
