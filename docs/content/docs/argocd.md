---
title: Argo CD
weight: 5
description: Argo CD resource health checks for drop CRDs.
llmsDescription: |
  Argo CD integration for drop. Argo CD has no built-in health assessment for
  custom resources, so drop CRDs report Healthy immediately by default. This page
  provides Lua resource health checks that map drop status (status.phase and
  status.conditions[type=Ready]) to Argo CD health: Healthy / Progressing /
  Degraded. Install them cluster-wide via the argocd-cm ConfigMap
  (resource.customizations.health.<group>_<kind>) so Applications and the UI show
  real drop health, and sync waves can gate on CachedImage/CachedImageSet
  readiness.
---

Argo CD does **not** interpret arbitrary custom-resource status. Unknown CRDs are
reported `Healthy` as soon as they exist, so a `DiscoveryPolicy` that is failing
to reach its registry, or a `CachedImage` still pulling, would show green.

drop already exposes the idiomatic Kubernetes status shape — `status.phase` and
`status.conditions[]` with a `Ready` condition — but Argo CD needs a small **Lua
health check** to translate it. Install the checks below to get real health in the
Argo CD UI, accurate `Application` health, and the ability to gate sync waves on
`CachedImage`/`CachedImageSet` readiness.

## Status drop exposes

| Kind | `status.phase` | `Ready` condition | Notes |
|------|----------------|-------------------|-------|
| `CachedImage` | `Pending` \| `Pulling` \| `Ready` \| `Degraded` | yes (`reason`/`message`) | `status.ready` is a `nodesReady/nodesTargeted` string |
| `CachedImageSet` | `Pending` \| `Ready` \| `Degraded` | yes | aggregates child `CachedImage`s |
| `DiscoveryPolicy` | — | yes (`Synced` / error reason) | no `phase`; health comes from the `Ready` condition |
| `PullPolicy` | — | — | config-only, no status → always `Healthy` |

## Health checks

Add the following to the `argocd-cm` ConfigMap in the Argo CD install namespace.
The keys use the standard `resource.customizations.health.<group>_<kind>` form.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
  namespace: argocd
  labels:
    app.kubernetes.io/name: argocd-cm
    app.kubernetes.io/part-of: argocd
data:
  # CachedImage: gate on phase, fall back to the Ready condition.
  resource.customizations.health.drop.corewire.io_CachedImage: |
    hs = {}
    if obj.status == nil then
      return { status = "Progressing", message = "Waiting for status" }
    end
    -- Wait for the controller to observe the latest spec.
    if obj.metadata.generation ~= nil and obj.status.observedGeneration ~= nil
       and obj.status.observedGeneration < obj.metadata.generation then
      return { status = "Progressing", message = "Reconciling" }
    end
    local msg = obj.status.ready or ""
    if obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Ready" and c.message ~= nil and c.message ~= "" then
          msg = c.message
        end
      end
    end
    if obj.status.phase == "Ready" then
      return { status = "Healthy", message = msg }
    end
    if obj.status.phase == "Degraded" then
      return { status = "Degraded", message = msg }
    end
    -- Pending / Pulling / empty
    return { status = "Progressing", message = msg }

  # CachedImageSet: same shape, phase is Pending | Ready | Degraded.
  resource.customizations.health.drop.corewire.io_CachedImageSet: |
    if obj.status == nil then
      return { status = "Progressing", message = "Waiting for status" }
    end
    if obj.metadata.generation ~= nil and obj.status.observedGeneration ~= nil
       and obj.status.observedGeneration < obj.metadata.generation then
      return { status = "Progressing", message = "Reconciling" }
    end
    local managed = obj.status.imagesManaged or 0
    local ready = obj.status.imagesReady or 0
    local msg = tostring(ready) .. "/" .. tostring(managed) .. " images ready"
    if obj.status.conditions ~= nil then
      for _, c in ipairs(obj.status.conditions) do
        if c.type == "Ready" and c.message ~= nil and c.message ~= "" then
          msg = c.message
        end
      end
    end
    if obj.status.phase == "Ready" then
      return { status = "Healthy", message = msg }
    end
    if obj.status.phase == "Degraded" then
      return { status = "Degraded", message = msg }
    end
    return { status = "Progressing", message = msg }

  # DiscoveryPolicy: no phase — health is the Ready condition.
  resource.customizations.health.drop.corewire.io_DiscoveryPolicy: |
    if obj.status == nil or obj.status.conditions == nil then
      return { status = "Progressing", message = "Waiting for first sync" }
    end
    if obj.metadata.generation ~= nil and obj.status.observedGeneration ~= nil
       and obj.status.observedGeneration < obj.metadata.generation then
      return { status = "Progressing", message = "Reconciling" }
    end
    for _, c in ipairs(obj.status.conditions) do
      if c.type == "Ready" then
        if c.status == "True" then
          return { status = "Healthy", message = c.message }
        end
        return { status = "Degraded", message = c.message }
      end
    end
    return { status = "Progressing", message = "No Ready condition yet" }

  # PullPolicy: configuration-only, no status.
  resource.customizations.health.drop.corewire.io_PullPolicy: |
    return { status = "Healthy", message = "Configuration resource" }
```

If your Argo CD version prefers the list-based form, the same logic can be placed
under the `resourceHealthChecks` key instead — one entry per `group`/`kind`.

## Applying it

- **Argo CD Helm chart / Kustomize install**: set these under
  `configs.cm` (`resource.customizations.health.*`) in your Argo CD values, or
  patch the `argocd-cm` ConfigMap. Health checks are a **cluster-wide** Argo CD
  setting — they are read from `argocd-cm` and apply to every `Application` and
  `AppProject`; Argo CD has no per-`Application` or per-`AppProject` health-check
  override.
- **Reload**: `argocd-cm` is picked up automatically; no restart is required.

## Sync waves

Because the checks report `Progressing` until a resource is actually `Ready`,
you can make a later sync wave wait for caching to finish. For example, keep the
`drop` operator on an earlier wave and place `CachedImageSet` resources on a later
wave; Argo CD will hold the wave until the sets report `Healthy`.
