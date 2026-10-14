"""Turn placement contract shared by both storage black-box suites."""

import urllib.parse

from blackbox_api import event, request, require, require_status, token_usage


def run_turn_contract(base_url: str, token: str, run_id: str) -> None:
    def make(session, name, second, turn_id, *, agent="claude-code"):
        value = event(f"{run_id}-{name}", session, "request", name, token_usage(input_tokens=1), run_id)
        value.pop("attachments", None)
        value["agent"]["name"] = agent
        value["llm"] = {"provider": "Anthropic", "model": "claude-x"}
        value["observed_at"] = f"2026-10-08T00:00:{second:02}Z"
        value["metadata"] = {}
        if turn_id is not None:
            value["turn_id"] = turn_id
        return value

    def ingest(*events):
        result = request(base_url, "POST", "/v1/agent-usage/events", token=token, json_body={"events": events})
        require_status(result, 200, "turn fixture ingest")

    def turns(session):
        query = urllib.parse.urlencode({"session_id": session, "fields": "list", "limit": 100})
        result = request(base_url, "GET", f"/v1/agent-usage/events?{query}", token=token)
        require_status(result, 200, "turn event lookup")
        events = result.json()["events"]
        return {item["event_id"].removeprefix(f"{run_id}-"): item["turn_index"] for item in events}

    # Events naming a turn share it; events naming none join the turn in
    # progress; the first named turn adopts the turn opened without one.
    session = f"{run_id}-turns"
    ingest(
        make(session, "probe", 5, None),
        make(session, "main-1", 10, "p1"),
        make(session, "subagent-1", 12, "p1"),
        make(session, "recap", 20, None),
        make(session, "main-2", 30, "p2"),
    )
    ingest(
        make(session, "late-title", 32, None),
        make(session, "late-subagent", 45, "p1"),
    )
    turn = turns(session)
    for name in ["probe", "main-1", "subagent-1", "recap", "late-subagent"]:
        require(turn[name] == 1, f"{name} belongs to the first turn")
    for name in ["main-2", "late-title"]:
        require(turn[name] == 2, f"{name} belongs to the second turn")

    # Turns are numbered by start time, whatever order they arrive in, and
    # without regard to the Agent.
    late = f"{run_id}-late-turns"
    ingest(make(late, "b", 30, "b", agent="codex"), make(late, "c", 50, "c", agent="codex"))
    ingest(make(late, "a", 10, "a", agent="codex"))
    turn = turns(late)
    require((turn["a"], turn["b"], turn["c"]) == (1, 2, 3), "a late turn takes its place by start time")
    print("turn placement API contract passed")
