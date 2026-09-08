"""Prepare an empty local runtime and a private generated database password."""
import os,secrets
from pathlib import Path
root=Path(__file__).resolve().parents[1]
for folder in ["incidents","reports","tmp"]:
    (root/".runtime"/folder).mkdir(parents=True,exist_ok=True)
target=root/".env"
if not target.exists():
    fd=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,"w") as out:
        out.write("YXWOOF_DB_PASSWORD="+secrets.token_hex(24)+"\nYXWOOF_PORT=8780\n")
print("Runtime prepared; private settings stay in .env. No business data was imported.")
