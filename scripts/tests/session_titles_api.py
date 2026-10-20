"""Session title contract shared by both storage black-box suites."""

import json
import urllib.parse
import uuid

from blackbox_api import event, request, require, require_status, token_usage


PROMPT = "Draw a pelican riding a bicycle"
CODEX_NAMING = "Generate a concise, single-line task title\nUser prompt:\n" + PROMPT


def run_title_contract(base_url: str, token: str, run_id: str) -> None:
    def make(session, kind, text, *, naming_thread=False, agent="codex", metadata=None):
        value = event(str(uuid.uuid4()), f"{run_id}-{session}", kind, text, token_usage(), run_id)
        value.pop("attachments", None)
        value["agent"]["name"] = agent
        value["device"]["host_name"] = f"title-contract-{run_id}"
        value["observed_at"] = "2026-09-30T00:00:01Z" if kind == "request" else "2026-09-30T00:00:02Z"
        value["metadata"] = dict(metadata or {})
        if naming_thread:
            value["naming_thread"] = True
        if kind == "request":
            value["metadata"]["conversation"] = {
                "entries": [{"role": "user", "text": text}], "completeness": "complete",
            }
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

    # Claude Code: the collector reports the naming call and its title, in
    # either arrival order relative to the session's first user input.
    naming = {"purpose": "session_title"}
    for name, title_first in [("claude", False), ("claude-late-input", True)]:
        events = [
            make(name, "request", "fix the build", agent="claude-code"),
            make(name, "request", "<session>fix the build</session>", agent="claude-code", metadata=naming),
            make(name, "response", '{"title":"Build fix"}', agent="claude-code",
                 metadata={**naming, "session_title": "Build fix"}),
        ]
        if title_first:
            events.reverse()
        for value in events:
            ingest(value)
        value = session(name)
        require(value["title"] == "Build fix", f"{name}: the collector title names the session")
        require(value["purpose"] is None, f"{name}: a session naming itself is not a title thread")

    # A thread opened only to name another session is marked; the task keeps
    # its first user input.
    ingest(make("naming", "request", CODEX_NAMING, naming_thread=True),
           make("naming", "response", '{"title":"Draw a pelican"}', naming_thread=True))
    value = session("naming")
    require(value["purpose"] == "title_generation", "naming threads are marked")
    require(value["title"] is None and value["parent_session_pk"] is None, "naming threads are not linked")
    ingest(make("task", "request", PROMPT))
    require(session("task")["title"] == PROMPT, "Codex tasks keep their first user input")

    # Instruction text and title-shaped replies never name a session by themselves.
    for agent, name in [("claude-code", "quoted-claude"), ("cursor", "generic-codex")]:
        ingest(make(name, "request", CODEX_NAMING, agent=agent),
               make(name, "response", '{"title":"Build fix"}', agent=agent))
        value = session(name)
        require(value["title"] == " ".join(CODEX_NAMING.split())[:79] + "…", f"{agent}/{name} keeps its first input")
        require(value["purpose"] is None, f"{agent}/{name} is not a title thread")

    first = make("default", "request", "  修复\n登录问题 " + "好" * 90, agent="cursor")
    ingest(first)
    expected = "修复 登录问题 " + "好" * 71 + "…"
    require(session("default")["title"] == expected, "default title truncates Unicode user input")
    later = make("default", "request", "Later request", agent="cursor")
    later["observed_at"] = "2026-09-30T00:00:03Z"
    ingest(later, make("default", "response", '{"title":"Do not rename"}', agent="cursor"))
    require(session("default")["title"] == expected, "later requests and responses preserve the first input")
    earlier = make("default", "request", "Earlier request", agent="cursor")
    earlier["observed_at"] = "2026-09-30T00:00:00Z"
    ingest(earlier)
    require(session("default")["title"] == "Earlier request", "late delivery restores the earliest input")

    result = request(base_url, "GET", "/v1/agent-usage/sessions?agent_name=cursor&page_size=100", token=token)
    require_status(result, 200, "default title session list")
    pk = session("default")["session_pk"]
    listed = next(item for item in result.json()["items"] if item["session_pk"] == pk)
    require(listed["title"] == "Earlier request", "session list and outline share the default title")
    print("session title API contract passed")
