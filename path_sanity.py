from pathlib import Path
p = Path.home() / "Documents" / "AmazonNotes"
print("Local path:", p)
print("exists:", p.exists(), "is_dir:", p.is_dir())
if p.exists():
    try:
        print("sample:", [x.name for x in list(p.iterdir())[:5]])
    except Exception as e:
        print("iterdir error:", e)