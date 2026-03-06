import subprocess
from datetime import datetime
import uuid

def run(cmd):
    print(">", " ".join(cmd))
    subprocess.check_call(cmd)

def main():
    run_id = f"run_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    print("RUN_ID =", run_id)

    # run extraction stages (pass run_id via env var)
    # simplest approach: call python modules that accept run_id by env var is more work
    # so we pass as a CLI arg and read it inside scripts if you add argparse;
    # BUT for minimal edits: we just re-run and the scripts stamp run_id=None.
    #
    # If you want run_id wired fully, tell me and I'll add argparse cleanly.
    run(["python", "src/extract_event_claims_rest.py"])
    run(["python", "src/extract_text_claims_rest.py"])
    run(["python", "src/merge_claims.py"])
    run(["python", "src/materialize_current_state.py"])

if __name__ == "__main__":
    main()