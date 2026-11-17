# Reliable CLI installation and upgrade

## Problem and expected behavior

Task: `fix-saas-cli.md`. A Linux user installed endpoint 1.6.0 into `~/.local/bin`, but `abyss version` still returned 1.0.1 while the absolute installed path returned 1.6.0. The invoking shell retained its old executable location. The old CLI rejected the packaged `deepseek-harness` policy before login. Separately, the SaaS CLI profiles omitted `product.dashboard.url`, so the documented dashboard command failed.

The current shell must select the installed CLI before authentication. Packaged CLI configuration must be accepted by the packaged executable, and SaaS dashboard commands must select the appropriate regional console.

## Repository approach

Synchronize standalone operator troubleshooting with the product installer repair. Document that an old CLI selected by the invoking shell can reject the newer runtime policy before contacting this backend; compare executable versions and refresh the shell instead of altering event storage or weakening policy validation. Ingestion and query contracts remain unchanged.

## Compatibility and integration

No event schema, storage migration, authentication protocol, TLS policy or released dependency version changes. Existing configuration is preserved; reinstalling does not overwrite a customized sidecar. Companion repositories are `abyss-services`, `AbyssApp`, `abyss-frontend`, `abyss-backend`, and `abyss-dashboard`, each using branch `task/fix-saas-cli` and its own design. Deploy installer guidance and documentation together; new profile defaults take effect in the next product CLI release. No release is published as part of this task.

## Validation

Run the repository-required unit, black-box and quality checks. Reproduce Bash command hashing and validate the shipped Linux CLI against its actual policy. Render the updated guide, deploy both local stacks, seed identifiable CLI scenarios, and record exact source revisions and outcomes in the persistent HTML report at `task-artifacts/fix-saas-cli/test-report.html` in the coordinator workspace. Final PR and evidence links will be recorded before handoff.
