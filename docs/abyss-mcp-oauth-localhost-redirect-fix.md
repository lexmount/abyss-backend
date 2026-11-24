# Connecting a desktop MCP client

When a desktop agent registers its local callback, the product gateway sends
registration and consent to Identity. The MCP service uses the resulting
scoped grant to read the user's history. Configure the agent with the
product's public `/api/mcp` URL. The [Identity design](https://github.com/lexmount/abyss-services/blob/task/abyss-mcp-oauth-localhost-redirect-fix/docs/abyss-mcp-oauth-localhost-redirect-fix.md)
describes localhost callback matching and rollout in [Identity PR 40](https://github.com/lexmount/abyss-services/pull/40).

The standalone deployment reads history through `/v1/agent-usage` using
its configured deployment bearer. Its HTTP suite checks the destination
boundary on both SQLite and PostgreSQL by sending a desktop registration to
the standalone server and verifying HTTP 404. Existing authenticated history
checks exercise the standalone path. The standalone change can ship
independently of Identity. The [local test report](http://127.0.0.1:32910/review/index.html)
records both storage profiles and the review dashboard.
