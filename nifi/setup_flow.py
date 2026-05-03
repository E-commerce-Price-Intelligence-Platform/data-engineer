"""
Creates the Price Intelligence NiFi flow via REST API.
Run once after NiFi starts:
    python nifi/setup_flow.py

Requirements: pip install requests
NiFi must be running at https://localhost:8443
"""
import json, os, time, sys
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

NIFI_URL  = os.environ.get("NIFI_URL",      "https://localhost:8443")
USERNAME  = os.environ.get("NIFI_USERNAME", "admin")
PASSWORD  = os.environ.get("NIFI_PASSWORD", "adminadminadmin")

# Paths inside the NiFi container
OUTPUT_DIR    = "/opt/nifi/price_intelligence/output"
WRITER_SCRIPT = "/opt/nifi/price_intelligence/price_intelligence/bigtable/bigtable_nifi_writer.py"
WORK_DIR      = "/opt/nifi/price_intelligence"
GCP_ENV       = (
    "GCP_PROJECT=regal-unfolding-490222-g5;"
    "BIGTABLE_INSTANCE_ID=price-intel-instance;"
    "BIGTABLE_TABLE_ID=smartphones;"
    "GOOGLE_APPLICATION_CREDENTIALS=/opt/nifi/price_intelligence/gcp-credentials.json"
)


def cleanup_processors(token: str, group_id: str):
    """Stop and delete all existing processors in root group."""
    r = requests.get(
        f"{NIFI_URL}/nifi-api/process-groups/{group_id}/processors",
        headers=hdrs(token), verify=False,
    )
    processors = r.json().get("processors", [])
    if not processors:
        return
    print(f"  Cleaning up {len(processors)} existing processor(s)...")
    for p in processors:
        pid = p["id"]
        ver = p["revision"]["version"]
        # Stop first
        requests.put(
            f"{NIFI_URL}/nifi-api/processors/{pid}/run-status",
            headers=hdrs(token), verify=False,
            json={"revision": {"version": ver}, "state": "STOPPED",
                  "disconnectedNodeAcknowledged": False},
        )

    # Delete connections first
    rc = requests.get(
        f"{NIFI_URL}/nifi-api/process-groups/{group_id}/connections",
        headers=hdrs(token), verify=False,
    )
    for c in rc.json().get("connections", []):
        requests.delete(
            f"{NIFI_URL}/nifi-api/connections/{c['id']}?version={c['revision']['version']}",
            headers=hdrs(token), verify=False,
        )

    # Now delete processors
    r2 = requests.get(
        f"{NIFI_URL}/nifi-api/process-groups/{group_id}/processors",
        headers=hdrs(token), verify=False,
    )
    for p in r2.json().get("processors", []):
        requests.delete(
            f"{NIFI_URL}/nifi-api/processors/{p['id']}?version={p['revision']['version']}",
            headers=hdrs(token), verify=False,
        )
    print("  Cleanup done.")


def get_token() -> str:
    """Returns Bearer token, or empty string if NiFi runs without auth (HTTP mode)."""
    try:
        resp = requests.post(
            f"{NIFI_URL}/nifi-api/access/token",
            data={"username": USERNAME, "password": PASSWORD},
            headers={
                "Content-Type":     "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
            },
            verify=False, timeout=15,
        )
        if resp.status_code in (200, 201):
            return resp.text.strip()
        # 400/401 in HTTP mode = no auth required
        print(f"  No auth required (NiFi HTTP mode, status {resp.status_code})")
        return ""
    except Exception as e:
        print(f"  Token skipped: {e}")
        return ""


def hdrs(token: str) -> dict:
    h = {"Content-Type": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def root_group_id(token: str) -> str:
    r = requests.get(f"{NIFI_URL}/nifi-api/process-groups/root", headers=hdrs(token), verify=False)
    r.raise_for_status()
    return r.json()["id"]


def create_processor(token: str, group_id: str, name: str, ptype: str,
                     x: float, y: float, props: dict,
                     auto_term: list = None) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "type": ptype,
            "name": name,
            "position": {"x": x, "y": y},
            "config": {
                "properties": props,
                "autoTerminatedRelationships": auto_term or [],
            },
        },
    }
    r = requests.post(
        f"{NIFI_URL}/nifi-api/process-groups/{group_id}/processors",
        headers=hdrs(token), json=body, verify=False,
    )
    if r.status_code not in (200, 201):
        print(f"  ERROR creating {name}: {r.status_code} {r.text[:200]}")
        sys.exit(1)
    pid = r.json()["id"]
    print(f"  ✅ {name} ({pid[:8]}...)")
    return pid


def create_connection(token: str, group_id: str,
                      src_id: str, src_name: str,
                      dst_id: str, dst_name: str,
                      rels: list) -> str:
    body = {
        "revision": {"version": 0},
        "component": {
            "source": {"id": src_id, "groupId": group_id, "type": "PROCESSOR"},
            "destination": {"id": dst_id, "groupId": group_id, "type": "PROCESSOR"},
            "selectedRelationships": rels,
            "backPressureObjectThreshold": 10000,
            "backPressureDataSizeThreshold": "1 GB",
        },
    }
    r = requests.post(
        f"{NIFI_URL}/nifi-api/process-groups/{group_id}/connections",
        headers=hdrs(token), json=body, verify=False,
    )
    if r.status_code not in (200, 201):
        print(f"  ERROR connecting {src_name}→{dst_name}: {r.status_code} {r.text[:200]}")
        sys.exit(1)
    cid = r.json()["id"]
    print(f"  ✅ {src_name} → {dst_name} [{rels}]")
    return cid


def start_processor(token: str, pid: str, name: str):
    r = requests.put(
        f"{NIFI_URL}/nifi-api/processors/{pid}/run-status",
        headers=hdrs(token),
        json={"revision": {"version": 1}, "state": "RUNNING", "disconnectedNodeAcknowledged": False},
        verify=False,
    )
    if r.status_code in (200, 202):
        print(f"  ▶ {name} started")
    else:
        print(f"  ⚠ Could not start {name}: {r.status_code}")


def main():
    print("=== NiFi Flow Setup ===")
    print(f"Connecting to {NIFI_URL} ...")

    # Wait for NiFi to be ready
    for attempt in range(20):
        try:
            # /nifi-api/access is public — any HTTP response means NiFi is up
            r = requests.get(f"{NIFI_URL}/nifi-api/access", verify=False, timeout=5)
            if r.status_code in (200, 400, 401, 403):
                print(f"  NiFi reachable (status {r.status_code})")
                break
        except Exception:
            pass
        print(f"  Waiting for NiFi... ({attempt+1}/20)")
        time.sleep(10)
    else:
        print("ERROR: NiFi not reachable after 200s")
        sys.exit(1)

    token    = get_token()
    group_id = root_group_id(token)
    print(f"Root group: {group_id}\n")

    print("Cleaning up existing processors:")
    cleanup_processors(token, group_id)

    # ── Create processors ──────────────────────────────────────────────────────
    print("Creating processors:")

    p_getfile = create_processor(token, group_id, "GetFile",
        "org.apache.nifi.processors.standard.GetFile", -600, 0,
        props={
            "Input Directory":        OUTPUT_DIR,
            "File Filter":            "[a-z]+\\.json",
            "Keep Source File":       "true",
            "Minimum File Age":       "5 sec",
            "Polling Interval":       "30 sec",
            "Batch Size":             "10",
            "Recurse Subdirectories": "false",
            "Ignore Hidden Files":    "true",
        },
        auto_term=["failure"],
    )

    p_split = create_processor(token, group_id, "SplitJson",
        "org.apache.nifi.processors.standard.SplitJson", -300, 0,
        props={"JsonPath Expression": "$.*"},
        auto_term=["failure", "original"],
    )

    p_eval = create_processor(token, group_id, "EvaluateJsonPath",
        "org.apache.nifi.processors.standard.EvaluateJsonPath", 0, 0,
        props={
            "Destination":              "flowfile-attribute",
            "Return Type":              "auto-detect",
            "Path Not Found Behavior":  "ignore",
            "Null Value Representation": "empty string",
            "record.name":        "$.name",
            "record.brand":       "$.brand",
            "record.model":       "$.model",
            "record.price":       "$.price",
            "record.old_price":   "$.old_price",
            "record.currency":    "$.currency",
            "record.discount":    "$.discount",
            "record.rating":      "$.rating",
            "record.reviews":     "$.reviews",
            "record.url":         "$.url",
            "record.source_site": "$.source_site",
            "record.scraped_at":  "$.scraped_at",
        },
        auto_term=["failure", "unmatched"],
    )

    p_exec = create_processor(token, group_id, "ExecuteStreamCommand",
        "org.apache.nifi.processors.standard.ExecuteStreamCommand", 600, 0,
        props={
            "Command Path":        "python3",      # NiFi 2.x renamed from "Command"
            "Command Arguments":   WRITER_SCRIPT,
            "Working Directory":   WORK_DIR,
            "Ignore STDIN":        "false",
            "Environment Variables": GCP_ENV,
            "Argument Delimiter":  ";",
        },
        auto_term=["nonzero status", "original"],  # NiFi 2.x has "original" relationship
    )

    p_log_ok = create_processor(token, group_id, "LogAttribute (success)",
        "org.apache.nifi.processors.standard.LogAttribute", 900, 0,
        props={
            "Log Level":         "info",
            "Attributes to Log": "bigtable.row.key,record.name,record.price,record.source_site",
            "Log Payload":       "false",
        },
        auto_term=["success"],
    )

    p_log_fail = create_processor(token, group_id, "LogAttribute (failure)",
        "org.apache.nifi.processors.standard.LogAttribute", 300, 200,
        props={"Log Level": "warn", "Log Payload": "true"},
        auto_term=["success"],
    )

    # ── Create connections ─────────────────────────────────────────────────────
    print("\nCreating connections:")

    create_connection(token, group_id, p_getfile, "GetFile",
                      p_split,   "SplitJson",             ["success"])
    create_connection(token, group_id, p_split,   "SplitJson",
                      p_eval,    "EvaluateJsonPath",       ["split"])
    create_connection(token, group_id, p_eval,    "EvaluateJsonPath",
                      p_exec,    "ExecuteStreamCommand",   ["matched"])
    create_connection(token, group_id, p_exec,    "ExecuteStreamCommand",
                      p_log_ok,  "LogAttribute (success)", ["output stream"])  # NiFi 2.x renamed from "success"
    create_connection(token, group_id, p_eval,    "EvaluateJsonPath",
                      p_log_fail,"LogAttribute (failure)", ["failure", "unmatched"])

    # ── Start all processors ───────────────────────────────────────────────────
    print("\nStarting processors:")
    for pid, name in [
        (p_getfile, "GetFile"),
        (p_split,   "SplitJson"),
        (p_eval,    "EvaluateJsonPath"),
        (p_exec,    "ExecuteStreamCommand"),
        (p_log_ok,  "LogAttribute (success)"),
        (p_log_fail,"LogAttribute (failure)"),
    ]:
        start_processor(token, pid, name)

    print("\n✅ Flow setup complete.")
    print(f"   NiFi UI: {NIFI_URL}/nifi")
    print("   Flow polls output/*.json every 30s → writes to GCP Bigtable")


if __name__ == "__main__":
    main()
