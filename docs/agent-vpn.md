# Enterprise AI credential boundary

Enterprise AI accounts belong to the optional product service in abyss-services. Its administrator configures a provider credential for an employee and harness. Identity authenticates the employee before the service delivers that credential.

A standalone deployment maps its backend bearer token to one fixed event owner. It has no enterprise employee directory or administrator authorization for provider keys. Routing a credential through this API would collapse the employee boundary. The standalone dashboard continues to use its existing event ingestion and query APIs.

The shared HTTP black-box suite exercises employee lookup and administrator assignment paths with a valid standalone credential, using both the product prefix and the prefix after dashboard proxy rewriting. Both storage profiles must return 404. The product service owns the database migration and deploys independently.

[Product service design](https://github.com/lexmount/abyss-services/blob/task/agent-vpn/docs/agent-vpn.md)
