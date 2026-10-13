"""Claude Code turn attachment contract shared by both storage black-box suites."""

import urllib.parse

from blackbox_api import event, request, require, require_status


def run_claude_turn_contract(base_url: str, token: str, run_id: str) -> None:
    session_id = f"{run_id}-claude-thread"

    def make(name, second, metadata, *, agent="claude-code", session=session_id):
        value = event(f"{run_id}-{name}", session, "request", name, {"input_tokens": 1}, run_id)
        value.pop("attachments", None)
        value["agent"]["name"] = agent
        value["llm"] = {"provider": "Anthropic", "model": "claude-x"}
        value["observed_at"] = f"2026-10-08T00:00:{second:02}Z"
        # Current collectors send no turn number; blackbox_api's events still
        # carry one, as older collectors do.
        value.pop("turn_index")
        value["metadata"] = metadata
        return value

    def main_loop(turn, prompt):
        return {
            "claude_turn_id": f"cc_turn:{turn}",
            "claude_code_billing": {"cc_prompt_id": prompt, "cc_turn_index": str(turn)},
        }

    def ingest(*events):
        result = request(base_url, "POST", "/v1/agent-usage/events", token=token, json_body={"events": events})
        require_status(result, 200, "Claude turn fixture ingest")

    def turns(session):
        query = urllib.parse.urlencode({"session_id": session, "fields": "list", "limit": 100})
        result = request(base_url, "GET", f"/v1/agent-usage/events?{query}", token=token)
        require_status(result, 200, "Claude turn event lookup")
        events = result.json()["events"]
        return {item["event_id"].removeprefix(f"{run_id}-"): item["turn_pk"] for item in events}

    ingest(
        make("main-1", 10, main_loop(1, "prompt-1")),
        make("subagent-1", 12, {"claude_code_billing": {"cc_prompt_id": "prompt-1", "cc_is_subagent": "true"}}),
        make("recap", 20, {"message_id": f"msg-recap-{run_id}"}),
        make("main-2", 30, main_loop(2, "prompt-2")),
        make("main-2-replay", 35, main_loop(2, "prompt-2b")),
        make("subagent-2", 40, {"claude_code_billing": {"cc_prompt_id": "prompt-2b"}}),
    )
    ingest(
        make("late-title", 32, {"request_hash": f"title-{run_id}"}),
        make("late-subagent", 45, {"claude_code_billing": {"cc_prompt_id": "prompt-1"}}),
        make("early-probe", 1, {}),
    )
    turn = turns(session_id)
    require(turn["main-1"] != turn["main-2"], "Claude Code main-loop turns stay distinct")
    require(turn["main-2-replay"] == turn["main-2"], "a replay keeps its cc_turn turn")
    for name in ["subagent-1", "recap", "late-subagent", "early-probe"]:
        require(turn[name] == turn["main-1"], f"{name} joins the first turn")
    for name in ["subagent-2", "late-title"]:
        require(turn[name] == turn["main-2"], f"{name} joins the second turn")

    other = f"{run_id}-codex-turns"
    ingest(
        make("codex-1", 10, {"codex_turn_id": f"turn-{run_id}"}, agent="codex", session=other),
        make("codex-2", 20, {"response_id": f"response-b-{run_id}"}, agent="codex", session=other),
        make("codex-2-response", 21, {"turn_id": f"response-b-{run_id}"}, agent="codex", session=other),
        make("unidentified-1", 30, {}, agent="codex", session=other),
        make("unidentified-2", 31, {}, agent="codex", session=other),
    )
    codex = turns(other)
    require(codex["codex-1"] != codex["codex-2"], "other Agents keep provider turn identities")
    require(codex["codex-2-response"] != codex["codex-1"], "a turn_id names its own turn")
    require(
        len({codex["codex-1"], codex["codex-2"], codex["unidentified-1"], codex["unidentified-2"]}) == 4,
        "an event without any turn identity is a turn of its own",
    )
    print("Claude Code turn attachment API contract passed")
