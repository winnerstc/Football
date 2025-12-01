import os

scripts = [
    "incremental_players.py",
    "incremental_kicking.py",
    "incremental_returns.py",
    "incremental_receiving.py",
    "incremental_passing.py",
    "incremental_rushing.py",
    "incremental_defensive.py"
]

for s in scripts:
    print(f"Running {s}...")
    os.system(f"spark-submit {s}")
