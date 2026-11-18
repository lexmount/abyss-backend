# Service publishing investigation: standalone boundary assessment

Task: `optimize-services-deploy.md`.

Companion PR: [lexmount/abyss-services#33](https://github.com/lexmount/abyss-services/pull/33).

## Problem and scope

The product service images are slow to publish to Tencent Cloud. The
[companion diagnosis and proposal](https://github.com/lexmount/abyss-services/blob/task/optimize-services-deploy/docs/optimize-services-deploy.md)
separates emulated ARM compilation, registry upload time, and job startup delay
using completed GitHub Actions logs. This document records the corresponding
standalone assessment at baseline
`6c516820b50cd774dc556dce8e987567a96d89a2`.

The task delivers the investigation and removes ARM64 from product service and
frontend image publishing after the user confirmed AMD64-only Kubernetes
clusters. This repository keeps its image workflow and runtime unchanged. No
repository changes APIs, stored data, or production deployments in this task.

## Ownership and compatibility

The standalone backend owns generic event storage, queries, and its Docker Hub
and native binary releases. Product Identity, Agent Data, MCP, Update, and Search
Indexer images belong to `abyss-services`; the product does not run on top of
this backend. A service-image distribution improvement therefore has no event
schema, authorization, query behavior, or database migration to mirror here.

The current [standalone CD](../.github/workflows/cd.yml) already publishes one
`linux/amd64` image to Docker Hub and gates publication on its reusable CI
workflow. Version tags also build Linux/macOS SQLite binaries and GitHub release
assets. It does not publish to either Tencent registry and does not request an
emulated ARM64 container build. Copying the product proposal into this workflow
would change unrelated release contracts without addressing the reported path.
The standalone dashboard's backend routing and consumers of the pinned native
backend artifacts retain their existing contracts.

## Decision and validation

Keep standalone publication unchanged. Assess shared build-cache concepts only
in a separate measured task if this pipeline shows a similar bottleneck. A
future product implementation must keep the two release lifecycles separate;
it must not point standalone deployments at product service images or add
product Identity dependencies to the generic backend.

The coordination requirement is satisfied by reviewing the actual workflow and
recording this boundary decision. The companion PRs can merge independently;
there is no artifact version bump, migration, or integration ordering between
these changes. The persistent Chinese HTML report under
`artifacts/optimize-services-deploy/` in the coordinator workspace records actual
validation and any outstanding protocol checks. Historical product publication
logs establish diagnosis only, not a measured speedup for the new AMD64-only workflows.
