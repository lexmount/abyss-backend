# Enterprise AI credential boundary

Enterprise AI accounts belong to the optional product service in abyss-services. Its administrator manages named API keys and accounts for each employee and provider, and maintains a separate harness-to-provider routing table. Employees authenticate with Identity to synchronize their own credentials and the enterprise routing policy.

A standalone deployment maps its backend bearer token to one fixed event owner. It has no enterprise employee directory or administrator authorization for provider keys. Routing a credential through this API would collapse the employee boundary. The standalone dashboard continues to use its existing event ingestion and query APIs.

The shared HTTP black-box suite exercises employee configuration, provider catalog, credential management and harness routing paths with a valid standalone credential, using both the product prefix and the prefix after dashboard proxy rewriting. Both storage profiles must return 404. The product service owns the database migration and deploys independently.

[Product service design](https://github.com/lexmount/abyss-services/blob/task/agent-vpn/docs/agent-vpn.md)

[Product service PR](https://github.com/lexmount/abyss-services/pull/34) · [Standalone validation](https://github.com/lexmount/abyss-backend/pull/24/checks)
