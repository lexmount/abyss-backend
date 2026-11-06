"""Session title contract shared by both storage black-box suites."""

import json
import urllib.parse
import uuid

from blackbox_api import event, request, require, require_status, token_usage


PROMPT = "Draw a pelican riding a bicycle"
INSTRUCTION = "Generate a concise, single-line task title\nUser prompt:\n" + PROMPT


def run_title_contract(base_url: str, token: str, run_id: str) -> None:
    def make(session, kind, text, *, agent="codex"):
        value = event(str(uuid.uuid4()), f"{run_id}-{session}", kind, text, token_usage(), run_id)
        value.pop("attachments", None)
        value["agent"]["name"] = agent
        value["device"]["host_name"] = f"title-contract-{run_id}"
        value["observed_at"] = "2026-09-30T00:00:01Z" if kind == "request" else "2026-09-30T00:00:02Z"
        value["metadata"] = {}
        if kind == "request":
            value["conversation"] = {
                "entries": [{"role": "user", "text": text}], "completeness": "complete",
            }
        else:
            value.pop("conversation", None)
        return value

    def ingest(*events):
        result = request(base_url, "POST", "/v1/agent-usage/events", token=token, json_body={"events": events})
        require_status(result, 200, "title fixture ingest")

    def session(name):
        query = urllib.parse.urlencode({"session_id": f"{run_id}-{name}", "limit": 1})
        result = request(base_url, "GET", f"/v1/agent-usage/events?{query}", token=token)
        require_status(result, 200, "title event lookup")
        pk = result.json()["events"][0]["session_pk"]
        result = request(base_url, "GET", "/v1/agent-usage/sessions?page_size=100", token=token)
        require_status(result, 200, "title session list")
        value = next(item for item in result.json()["items"] if item["session_pk"] == pk)
        header = request(base_url, "GET", f"/v1/agent-usage/sessions/{pk}/header", token=token)
        require_status(header, 200, "title header")
        require(header.json()["title"] == value["title"], "header and list share one title")
        return value

    ingest(make("task", "request", PROMPT))
    require(session("task")["title"]["text"] == PROMPT, "a session takes its earliest request label")

    # Instruction text and title-shaped replies never name a session by themselves.
    for agent, name in [("claude-code", "quoted-claude"), ("cursor", "generic-codex")]:
        ingest(make(name, "request", INSTRUCTION, agent=agent),
               make(name, "response", '{"title":"Build fix"}', agent=agent))
        value = session(name)
        require(value["title"]["text"] == INSTRUCTION[:50], f"{agent}/{name} keeps its first input")
        require(value["purpose"] is None, f"{agent}/{name} has no purpose")

    first = make("default", "request", "  修复\n登录问题 " + "好" * 90, agent="cursor")
    ingest(first)
    expected = first["conversation"]["entries"][0]["text"][:50]
    require(session("default")["title"]["text"] == expected, "title returns the unformatted fifty-unit request label")
    later = make("default", "request", "Later request", agent="cursor")
    later["observed_at"] = "2026-09-30T00:00:03Z"
    ingest(later, make("default", "response", '{"title":"Do not rename"}', agent="cursor"))
    require(session("default")["title"]["text"] == expected, "later requests and responses preserve the first input")
    earlier = make("default", "request", "Earlier request", agent="cursor")
    earlier["observed_at"] = "2026-09-30T00:00:00Z"
    ingest(earlier)
    require(session("default")["title"]["text"] == "Earlier request", "late delivery restores the earliest input")

    result = request(base_url, "GET", "/v1/agent-usage/sessions?agent_name=cursor&page_size=100", token=token)
    require_status(result, 200, "default title session list")
    pk = session("default")["session_pk"]
    listed = next(item for item in result.json()["items"] if item["session_pk"] == pk)
    require(listed["title"]["text"] == "Earlier request", "session list and header share the default title")
    print("session title API contract passed")
