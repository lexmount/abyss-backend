# Standalone authentication during WorkBuddy MCP registration

WorkBuddy connects to the product MCP endpoint and discovers its OAuth registration endpoint in Identity. The standalone event backend has a different deployment contract. A dashboard or query client presents the deployment bearer token to the event API. A standalone administrator configuring WorkBuddy should use a product MCP endpoint for that integration.

The companion [Identity design](https://github.com/lexmount/abyss-services/blob/task/fix-workbuddy-mcp/docs/fix-workbuddy-mcp.md) covers the registration error that lets WorkBuddy retry with its loopback callback. No event schema, migration or API change is needed here. The SQLite and PostgreSQL HTTP suites exercise a registration request against standalone alongside the existing checks for bearer authentication and event access. These changes can be integrated independently.

The product fix and its SDK acceptance test are in [abyss-services PR 39](https://github.com/lexmount/abyss-services/pull/39).

The [standalone PR checks](https://github.com/lexmount/abyss-backend/pull/27/checks) run the HTTP contract against PostgreSQL and SQLite.
