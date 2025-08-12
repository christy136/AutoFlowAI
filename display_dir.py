# display_dir.py
import os
import sys
from datetime import datetime

# Directories and files to skip
# Directories and files to skip — tailored to your tree
SKIP_DIRS = {
    "__pycache__",
    "venv",     # your in-repo virtualenv
    ".venv",    # you sometimes run with a .venv too
    "logs",
    "output",
}

SKIP_FILES = {
    # root/log artifacts
    "feedback_logs.json",
    "successful_pipelines.json",
    "successful_pipelines.jsonl",  # logs/successful_pipelines.jsonl
    # script output (avoid self-inclusion)
    "scan_report.txt",
    # sensitive / local config you likely don’t want in the report
    "precheck_payload.json",
    # non-source readme (you already had this)
    "readme.md",
    # present under venv (redundant because venv is skipped, but harmless)
    "pyvenv.cfg",
}

# File types to include — expanded for your repo
DEFAULT_EXTS = [
    ".py",
    ".json",
    ".jsonl",   # you have *.jsonl under logs
    ".yaml", ".yml",
    ".j2",
    ".txt",
    ".html",    # templates/*.html
    ".sh",      # precheck_builder.sh
]

DEFAULT_OUTFILE = "scan_report.txt"


def parse_args(argv):
    """
    usage: python display_dir.py [--out report.txt] <path1> [<path2> ...]
    """
    out_file = DEFAULT_OUTFILE
    paths = []
    i = 0
    while i < len(argv):
        arg = argv[i]
        if arg in ("--out", "-o"):
            if i + 1 >= len(argv):
                sys.stderr.write("Missing value for --out\n")
                sys.exit(2)
            out_file = argv[i + 1]
            i += 2
        else:
            paths.append(arg)
            i += 1
    if not paths:
        sys.stderr.write("Usage: python display_dir.py [--out report.txt] <directory_or_file> [more_paths...]\n")
        sys.exit(1)
    return out_file, paths


def gather_files(targets, extensions):
    """
    Build a flat list of files to process, respecting skip rules.
    Returns (files, warnings)
    """
    files = []
    warnings = []
    for target in targets:
        if not os.path.exists(target):
            warnings.append(f"❌ Path not found: {target}")
            continue

        if os.path.isfile(target):
            if os.path.basename(target) not in SKIP_FILES and any(target.endswith(ext) for ext in extensions):
                files.append(os.path.abspath(target))
            continue

        # Directory walk
        for root, dirs, fns in os.walk(target):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for fn in sorted(fns):
                if fn in SKIP_FILES:
                    continue
                full = os.path.join(root, fn)
                if any(full.endswith(ext) for ext in extensions):
                    files.append(os.path.abspath(full))
    return files, warnings


def write_report(out_file, files, warnings):
    """
    Write the report from scratch (overwrite each run).
    """
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as fout:
        # Header
        fout.write("# Auto Flow AI Repo Scan\n")
        fout.write(f"# Generated: {datetime.utcnow().isoformat()}Z\n")
        fout.write(f"# Total files: {len(files)}\n")
        if warnings:
            fout.write("# Warnings:\n")
            for w in warnings:
                fout.write(f"#   {w}\n")
        fout.write("\n")

        # Body
        for fp in files:
            fout.write(f"\n📄 File: {fp}\n{'='*80}\n")
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    fout.write(f.read())
            except Exception as e:
                fout.write(f"⚠️ Could not read {fp}: {e}\n")
            fout.write("\n" + "="*80 + "\n")


def progress_iter(iterable, total):
    """
    Progress bar using tqdm if available; otherwise a tiny in-place fallback.
    Only the bar is shown in console; no file contents are printed.
    """
    try:
        from tqdm import tqdm  # lightweight; install via: pip install tqdm
        return tqdm(iterable, total=total, unit="file", ncols=80, dynamic_ncols=True, leave=True, desc="Scanning")
    except Exception:
        # Minimal fallback progress (no extra dependency)
        def generator():
            processed = 0
            last_pct = -1
            for item in iterable:
                processed += 1
                pct = int(processed * 100 / total) if total else 100
                if pct != last_pct:
                    sys.stdout.write(f"\rScanning: {pct:3d}%")
                    sys.stdout.flush()
                    last_pct = pct
                yield item
            sys.stdout.write("\n")
        return generator()


def main():
    out_file, targets = parse_args(sys.argv[1:])
    # Which extensions to scan
    extensions = DEFAULT_EXTS

    files, warnings = gather_files(targets, extensions)

    # Build the report with a progress indicator (no console prints besides the bar)
    # We stream file processing status internally; the report is written atomically per run.
    os.makedirs(os.path.dirname(out_file) or ".", exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as fout:
        # Header first
        fout.write("# Auto Flow AI Repo Scan\n")
        fout.write(f"# Generated: {datetime.utcnow().isoformat()}Z\n")
        fout.write(f"# Total files: {len(files)}\n")
        if warnings:
            fout.write("# Warnings:\n")
            for w in warnings:
                fout.write(f"#   {w}\n")
        fout.write("\n")

        # Process files with progress bar
        for fp in progress_iter(files, total=len(files)):
            fout.write(f"\n📄 File: {fp}\n{'='*80}\n")
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    fout.write(f.read())
            except Exception as e:
                fout.write(f"⚠️ Could not read {fp}: {e}\n")
            fout.write("\n" + "="*80 + "\n")


if __name__ == "__main__":
    main()
