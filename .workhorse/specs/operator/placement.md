---
id: PLACE
---

# Pod placement defaults

The operator stamps cluster-wide scheduling defaults onto every pod it creates:
restore Jobs, replica Deployments, and the other Jobs that run against a restore.
They are a property of the cluster the operator runs in, so they are configured on
the operator's ConfigMap rather than on any replica.

## Configuration

- [ ] The ConfigMap's `nodeSelector` key holds comma-separated `key=value` node
  selector entries.
- [ ] The ConfigMap's `podAnnotations` key holds comma-separated `key=value` pod
  annotations. A value may itself contain `=`.
- [ ] The ConfigMap's `tolerations` key holds comma-separated tolerations, each
  `key=value:Effect` to tolerate a taint with that exact value, or `key:Effect` to
  tolerate the key with any value. The effect is one of `NoSchedule`,
  `PreferNoSchedule`, or `NoExecute`.
- [ ] A malformed entry in any key is logged and skipped; the remaining entries of
  that key still apply.
- [ ] Changes to the ConfigMap apply to pods created afterwards, without an operator
  restart.

## Application

- [ ] Every pod the operator creates carries the configured node selector entries,
  annotations, and tolerations.
- [ ] A node selector or annotation key the operator already set on a pod for its
  own reasons keeps its value; the default only fills keys that are absent.
- [ ] A configured toleration is added to a pod unless the pod already carries the
  same toleration.
