"""Turn placement contract shared by both storage black-box suites."""

import urllib.parse

from blackbox_api import event, request, require, require_status, token_usage


def run_turn_contract(base_url: str, token: str, run_id: str) -> None:
    def make(session, name, second, turn_id, *, agent="claude-code", response_id=None, event_type="request"):
        value = event(f"{run_id}-{name}", session, "request", name, token_usage(input_tokens=1), run_id)
        value.pop("attachments", None)
        value.pop("response_id", None)
        value["agent"]["name"] = agent
        value["llm"] = {"provider": "Anthropic", "model": "claude-x"}
        value["observed_at"] = f"2026-10-08T00:00:{second:02}Z"
        value["metadata"] = {}
        if turn_id is not None:
            value["turn_id"] = turn_id
        if response_id is not None:
            value["response_id"] = response_id
        value["event_type"] = event_type
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

    # Missing turn ids fall back to a response, including reversed delivery
    # of request/response pairs and calls that overlap in observed time.
    fallback = f"{run_id}-response-turns"
    ingest(
        make(fallback, "r2-response", 35, None, response_id="r2", event_type="response"),
        make(fallback, "r1-response", 40, None, response_id="r1", event_type="response"),
    )
    ingest(
        make(fallback, "r1-request", 10, None, response_id="r1"),
        make(fallback, "r2-request", 30, None, response_id="r2"),
        make(fallback, "r3-request", 50, None, response_id="r3"),
        make(fallback, "r3-response", 55, None, response_id="r3", event_type="response"),
    )
    turn = turns(fallback)
    require(turn == {
        "r1-request": 1, "r1-response": 1,
        "r2-request": 2, "r2-response": 2,
        "r3-request": 3, "r3-response": 3,
    }, "each response owns one fallback turn, ordered by first observation")

    # Explicit ids win even across multiple model responses. Equal strings
    # in the two id domains do not merge, in either arrival order.
    for order in ("fallback-first", "explicit-first"):
        mixed = f"{run_id}-{order}"
        explicit = make(mixed, f"{order}-explicit", 20, "shared", response_id="a")
        response = make(mixed, f"{order}-fallback", 10, None, response_id="shared")
        ingest(*([response, explicit] if order == "fallback-first" else [explicit, response]))
        ingest(make(mixed, f"{order}-explicit-next", 30, "shared", response_id="b"))
        turn = turns(mixed)
        require(turn[f"{order}-fallback"] == 1, "response id keeps its own turn")
        require(turn[f"{order}-explicit"] == turn[f"{order}-explicit-next"] == 2,
                "explicit turn id takes priority over distinct response ids")

    # Provider ids are scoped to the session.
    separate = f"{run_id}-separate-response-turns"
    ingest(
        make(separate, "separate-r2", 10, None, response_id="r2"),
        make(separate, "separate-r1", 20, None, response_id="r1"),
    )
    require(turns(separate) == {"separate-r2": 1, "separate-r1": 2},
            "response ids cannot reuse turns from another session")
    print("turn placement API contract passed")
