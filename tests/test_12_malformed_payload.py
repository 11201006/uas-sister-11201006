import requests

BASE = "http://localhost:8080"

def test_malformed_payload():
    bad = "{this is not json}"

    res = requests.post(
        f"{BASE}/publish",
        data=bad,
        headers={"Content-Type": "application/json"}
    )
    assert res.status_code == 422
