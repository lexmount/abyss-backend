"""Session title adapter contract shared by both storage black-box suites."""

import base64
import json
import urllib.parse
import uuid

from blackbox_api import event, request, require, require_status


NAMING_TURN = "01a0f320-138e-7d21-99ea-03a7ae2a9b50"
TASK_TURN = "01a0f320-1350-71c2-9c93-5fc5fd7a6bfb"
PROMPT = "Draw a pelican riding a bicycle"
CODEX_NAMING = "Generate a concise, single-line task title\nUser prompt:\n" + PROMPT
CLAUDE_NAMING = "You are naming a coding session"


def run_title_contract(base_url: str, token: str, run_id: str) -> None:
    def make(session, kind, text, *, source="user", installation="one", agent="codex"):
        value = event(str(uuid.uuid4()), f"{run_id}-{session}", kind, text, {}, run_id)
        value.pop("attachments", None)
        value["agent"]["name"] = agent
        value["device"]["host_name"] = f"title-contract-{run_id}"
        value["observed_at"] = "2026-09-30T00:00:01Z" if kind == "request" else "2026-09-30T00:00:02Z"
        value["metadata"] = {"codex_turn_id": NAMING_TURN if source == "thread_title" else TASK_TURN}
        if kind == "request":
            payload = {
                "input": [{"role": "user", "content": [{"type": "input_text", "text": text}]}],
                "client_metadata": {"x-codex-turn-metadata": json.dumps({
                    "thread_source": source, "installation_id": installation,
                })},
            }
            value["metadata"]["raw_body"] = {"parts": [{
                "truncated": False,
                "content_base64": base64.b64encode(json.dumps(payload).encode()).decode(),
            }]}
        return value

    def ingest(*events):
        result = request(base_url, "POST", "/v1/agent-usage/events", token=token, json_body={"events": events})
        require_status(result, 200, "title fixture ingest")

    def session(name):
        query = urllib.parse.urlencode({"session_id": f"{run_id}-{name}", "limit": 1})
        result = request(base_url, "GET", f"/v1/agent-usage/events?{query}", token=token)
        require_status(result, 200, "title event lookup")
        pk = result.json()["events"][0]["session_pk"]
        result = request(base_url, "GET", f"/v1/agent-usage/sessions/{pk}/outline", token=token)
        require_status(result, 200, "title outline")
        return result.json()["session"]

    # Response-first and raw-only naming requests exercise both storage paths.
    naming = make("naming", "request", CODEX_NAMING, source="thread_title")
    naming["text"] = None
    ingest(make("naming", "response", '{"title":"Draw a pelican"}', source="thread_title"))
    ingest(naming)
    ingest(make("task", "request", PROMPT))
    task = session("task")
    require(task["title"] == "Draw a pelican", "raw Codex task receives its generated title")
    require(session("naming")["parent_session_pk"] == task["session_pk"], "title source links to the task")

    other_device = make("other-device", "request", PROMPT)
    other_device["device"]["host_name"] += "-other"
    ingest(other_device, make("other-installation", "request", PROMPT, installation="two"),
           make("guardian", "request", PROMPT, source="guardian_review"))
    require(session("naming")["parent_session_pk"] == task["session_pk"], "unrelated requests do not withdraw the link")

    ingest(make("conflict", "request", PROMPT))
    require(session("naming")["parent_session_pk"] is None, "a real second candidate withdraws the link")
    require(session("task")["title"] is None, "ambiguous title is removed from its former task")

    for agent, name, instruction, expected in [
        ("claude-code", "claude", CLAUDE_NAMING, "Build fix"),
        ("cursor", "generic-claude", CLAUDE_NAMING, None),
        ("cursor", "generic-codex", CODEX_NAMING, None),
        ("codex", "quoted-user", CODEX_NAMING, None),
    ]:
        ingest(make(name, "request", instruction, agent=agent),
               make(name, "response", '{"title":"Build fix"}', agent=agent))
        value = session(name)
        require(value["title"] == expected, f"{agent}/{name} follows its adapter policy")
        require(value["purpose"] is None, f"{agent}/{name} is not an auxiliary title thread")
    print("session title adapter API contract passed")
