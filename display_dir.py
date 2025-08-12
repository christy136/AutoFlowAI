import os
import sys

# Directories and files to skip
SKIP_DIRS = {"__pycache__", "venv", "logs", "output"}
SKIP_FILES = {"feedback_logs.json", "successful_pipelines.json", "readme.md"}

def display_all_files(directory, extensions=None):
    if not os.path.exists(directory):
        print(f"❌ Directory not found: {directory}")
        return

    if extensions is None:
        extensions = [".py", ".json", ".yaml", ".yml", ".j2", ".txt"]

    print(f"\n📁 Scanning directory: {directory}\n")

    for root, dirs, files in os.walk(directory):
        # Modify dirs in-place to skip excluded directories
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]

        for file in sorted(files):
            if file in SKIP_FILES:
                continue
            if not any(file.endswith(ext) for ext in extensions):
                continue

            file_path = os.path.join(root, file)
            print(f"\n📄 File: {file_path}\n{'='*80}")
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    print(f.read())
            except Exception as e:
                print(f"⚠️ Could not read {file_path}: {e}")
            print('='*80)

    print("\n✅ All matching files displayed.\n")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ Usage: python display_dir.py <directory_path or file_path>")
    else:
        for arg in sys.argv[1:]:
            display_all_files(arg)
