# launcher.py
import subprocess

match_ids = [
    "ceoracydrstgwdj3jeqfm0aac",
    "cfjmtr9xrz3ydur0k879qbjmc",
    "cgrtk6bfvu2ctp1rjs34g2r6c",
    "chlesutq3dquxwfvv4ba65hjo",
    "cif7u6dfjijtksln0bq4fvgus",
]

processes = []
for match_id in match_ids:
    process = subprocess.Popen(["python", "run_game.py", match_id])
    processes.append(process)
    print(f"Started process for match {match_id}")

# Optionally wait for all to complete
for p in processes:
    p.wait()