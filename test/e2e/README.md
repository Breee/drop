# Chainsaw E2E Tests

This directory contains scenario-based E2E tests using [Kyverno Chainsaw](https://kyverno.github.io/chainsaw/).

## Prerequisites

- A running Kind cluster with the operator deployed
- `chainsaw` binary installed (`make chainsaw`)

## Running

```bash
# From repo root
make test-e2e
```

## Test Scenarios

| Directory | Description |
|-----------|-------------|
| `cachedimage-basic/` | Basic CachedImage creation and pod scheduling |
| `cachedimage-failure/` | Failure backoff and Degraded phase behavior |
| `cachedimage-pacing/` | PullPolicy pacing enforcement |
| `cachedimageset/` | CachedImageSet managing child resources |
| `cachedimageset-discovery/` | CachedImageSet backed by a DiscoveryPolicy |
| `discovery/` | DiscoveryPolicy with mock Prometheus |
| `discovery-failure/` | DiscoveryPolicy with unreachable Prometheus endpoint |
| `discovery-loki/` | DiscoveryPolicy with real Alloy-ingested Loki events + eventPullTime signals |
| `discovery-modelexposure/` | DiscoveryPolicy modelExposure ranking with dynamic nodeSelector node count |
| `discovery-registry/` | DiscoveryPolicy listing tags from a mock registry |
| `discovery-registry-gitlab/` | DiscoveryPolicy version-aware ranking of GitLab runner helper style tags against a mock registry |
| `discovery-registry-gitlab-auth/` | DiscoveryPolicy against the real `registry.gitlab.com`, exercising the anonymous OCI bearer-token flow (needs egress) |
