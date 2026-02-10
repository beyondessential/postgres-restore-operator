# Overlay Database (FDW) Feature Plan

## Problem Statement

Add an optional "overlay database" feature to `PostgresPhysicalReplica`. When configured, the operator creates a persistent CNPG-managed PostgreSQL database that imports schemas from the active restore via Foreign Data Wrappers (FDW). On each restore swap, the old FDW schemas are dropped and re-imported from the new restore, keeping the overlay database persistent with its own local data while providing transparent read access to the restored data.

## Approach

Extend the existing `PostgresPhysicalReplica` CRD with an optional `overlayDatabase` field. When present, the operator manages:
1. A CNPG `Cluster` CR for the persistent overlay database
2. Per-restore Services (for all replicas, not just overlay-enabled ones) for stable FDW endpoints
3. FDW setup in the overlay database (server, user mapping, foreign schema imports)
4. FDW teardown and re-import on each restore switchover

## Workplan

### Phase 0: Replace `nodeSelector` with `affinity` (breaking change)
- [x] In `types/replica.rs`: remove `node_selector: Option<HashMap<String, String>>` from `PostgresPhysicalReplicaSpec`
- [x] In `types/replica.rs`: add `affinity: Option<Affinity>` to `PostgresPhysicalReplicaSpec` (using a custom `Affinity` type mirroring k8s affinity, or re-export `k8s_openapi::api::core::v1::Affinity` with schemars support)
- [x] In `controllers/restore.rs` (`build_deployment`): replace `node_selector` usage with `affinity` in the PodSpec
- [x] Update test helpers (`make_replica`, `make_replica_with_opts`) to use `affinity` instead of `node_selector`
- [x] Verify build and tests pass

### Phase 1: Per-Restore Services (foundation)
- [x] Add per-restore `Service` creation in the restore controller (`reconcile_ready`), creating a `<restore-name>` Service with selector `pgro.bes.au/restore: <restore-name>` — this is the default experience for all restores, not just overlay-enabled ones
- [x] Ensure the per-restore Service is owned by the restore (cascade delete)
- [x] Add RBAC — already covered (Services are in the operator.yaml permissions)

### Phase 2: CRD Types
- [x] Add `OverlayDatabaseConfig` struct to `types/replica.rs`:
  ```rust
  pub struct OverlayDatabaseConfig {
      /// PostgreSQL major version for the CNPG cluster (e.g. "17").
      /// If absent, resolved from the CNPG image catalog (see image_catalog).
      /// Falls back to a hardcoded default ("17") if no catalog is available.
      pub postgres_version: Option<String>,
      /// CNPG image catalog to use for PG version discovery and image resolution.
      /// If absent, defaults to ClusterImageCatalog kind.
      pub image_catalog: Option<ImageCatalogRef>,
      /// Override for the overlay database PVC size.
      /// If absent, auto-sized: 5Gi + ceil(snapshot_size / 10) rounded up to whole Gi.
      /// Auto-sizing only ever increases (ratchets up), never shrinks.
      pub storage_size_override: Option<String>,
      /// Storage class (optional)
      pub storage_class: Option<String>,
      /// Resource requirements for the overlay database pods
      pub resources: Option<ResourceRequirements>,
      /// Pod affinity/anti-affinity rules
      pub affinity: Option<Affinity>,
      /// Tolerations
      pub tolerations: Vec<Toleration>,
      /// Schema mapping: if provided, only these schemas are imported.
      /// Key = remote schema name, Value = local schema name in overlay DB.
      /// If absent, all user schemas are imported at their original names.
      pub schema_mapping: Option<HashMap<String, String>>,
  }

  pub struct ImageCatalogRef {
      /// Name of the image catalog resource
      pub name: String,
      /// Kind: "ClusterImageCatalog" (default) or "ImageCatalog"
      pub kind: Option<String>,
  }
  ```
- [x] Add `overlay_database: Option<OverlayDatabaseConfig>` field to `PostgresPhysicalReplicaSpec`
- [x] Add overlay-related status fields to `PostgresPhysicalReplicaStatus`:
  - `overlay_cluster_name: Option<String>` — name of the CNPG Cluster CR
  - `overlay_fdw_restore: Option<String>` — name of the restore whose schemas are currently imported
  - `overlay_storage_size: Option<String>` — current (possibly ratcheted) storage size of the overlay PVC
  - `overlay_postgres_version: Option<String>` — resolved PG major version used for the overlay cluster

### Phase 3: CNPG Cluster Management
- [x] Add CNPG CRD types (minimal structs via dynamic API or typed structs):
  - `Cluster` — enough to create and read status
  - `ClusterImageCatalog` / `ImageCatalog` — enough to list and read `.spec.images[].major` versions
- [x] Implement PG version resolution logic:
  1. If `postgres_version` is explicitly set in `OverlayDatabaseConfig`, use it directly
  2. Otherwise, look up the CNPG image catalog:
     - Use the catalog specified in `image_catalog` (name + kind)
     - Default kind: `ClusterImageCatalog` (cluster-scoped)
     - List images in the catalog, parse major versions, pick the highest
  3. If no catalog is found or readable, fall back to hardcoded default `"17"`
  4. The resolved version is stored in `status.overlayPostgresVersion` so it's visible and stable
  5. Reference the catalog in the CNPG `Cluster` CR's `imageCatalogRef` field so CNPG resolves the actual image
- [x] In the replica controller, if `overlay_database` is configured:
  - [x] Resolve PG version (above logic)
  - [x] Create a CNPG `Cluster` CR named `<replica-name>-overlay` with:
    - Single instance
    - Storage size: use `storage_size_override` if set, otherwise compute `5Gi + ceil(snapshot_size / 10)` rounded up to whole Gi
    - **Ratchet logic:** compare computed size against `status.overlayStorageSize`; only update the CNPG Cluster if the new size is larger (PVCs can grow but not shrink). Persist the high-water mark in `status.overlayStorageSize`.
    - PG version from resolution step; `imageCatalogRef` pointing at the user's catalog
    - Resources, node selector, tolerations from config
    - `postgres_fdw` in `shared_preload_libraries` (or enable via `CREATE EXTENSION`)
  - [x] Create a dedicated FDW credentials Secret (`<replica-name>-overlay-fdw-creds`) with a generated read-only username/password
  - [x] Wait for the CNPG Cluster to be ready before proceeding with FDW setup
- [x] Add RBAC for CNPG CRDs in `operator.yaml` (Cluster, ClusterImageCatalog, ImageCatalog)
- [x] Update `operator.yaml` RBAC to include CNPG resources:
  ```yaml
  - apiGroups: ["postgresql.cnpg.io"]
    resources: ["clusters"]
    verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
  - apiGroups: ["postgresql.cnpg.io"]
    resources: ["clusterimagecatalogs", "imagecatalogs"]
    verbs: ["get", "list"]
  ```

### Phase 4: FDW User in Restore Database
- [x] Modify the restore deployment init container (`build_deployment` in `restore.rs`) to create a read-only FDW user in the restore database using credentials from the overlay FDW secret. This user only needs `SELECT` / `pg_read_all_data` access.
- [x] The FDW user creation should only happen when the parent replica has `overlay_database` configured

### Phase 5: FDW Setup on Switchover
- [x] In the replica controller's switchover logic (step 7, when `switching_restore` is found):
  - [x] If `overlay_database` is configured and CNPG cluster is ready:
    1. Connect to the overlay database (via CNPG service)
    2. In a single transaction:
       a. Drop existing foreign schemas (from `overlay_fdw_restore` if set)
       b. Drop the old FDW server if it exists
       c. `CREATE EXTENSION IF NOT EXISTS postgres_fdw`
       d. `CREATE SERVER` pointing at the new restore's per-restore Service
       e. `CREATE USER MAPPING` for the overlay DB superuser → FDW read-only user
       f. Discover schemas (query `information_schema.schemata` on restore DB, excluding `pg_*`, `information_schema`)
       g. For each schema (or mapped schemas): `IMPORT FOREIGN SCHEMA <schema> FROM SERVER ... INTO <local_schema>`
    3. Update `overlay_fdw_restore` in replica status
- [x] Implement the FDW setup as a Kubernetes Job (since the operator itself shouldn't hold long DB connections) that runs SQL against both the overlay and restore databases

### Phase 6: FDW Teardown on Restore Cleanup
- [x] When cleaning up an old restore (step 8 in replica controller), if that restore's schemas are currently imported in the overlay DB (`overlay_fdw_restore == old_restore_name`), the FDW schemas should already have been swapped out by the switchover step — add a safety check

### Phase 7: Testing & Validation
- [x] Add unit tests for new type structures
- [x] Add unit tests for overlay storage size computation:
  - `compute_overlay_storage_size(snapshot_bytes)` → `"5Gi"` baseline + `ceil(snapshot_bytes / 10)` rounded up to Gi
  - e.g. 100Gi snapshot → `5Gi + 10Gi = 15Gi`
  - e.g. 1Gi snapshot → `5Gi + 1Gi = 6Gi`
  - e.g. 500Mi snapshot → `5Gi + 1Gi = 6Gi` (rounds up to whole Gi)
- [x] Add unit tests for ratchet logic (new size only applied if > current)
- [x] Update `make_replica` test helpers with the new `overlay_database` field
- [x] Ensure existing tests pass with `overlay_database: None`
- [x] Verify the build compiles cleanly (`cargo build`)
- [ ] Add integration test

## Notes

- The overlay database is fully optional — when `overlayDatabase` is `None`, behavior is identical to today
- Per-restore Services benefit all users (not just overlay), enabling direct pod-stable access to individual restores
- The CNPG Cluster manages its own PVC, HA, upgrades, etc. — our operator only creates the CR and manages FDW lifecycle
- FDW connections are read-only by design (dedicated user with minimal privileges)
- The swap procedure is transactional where possible: old schemas dropped and new ones imported within a single DB transaction
- System schemas (`pg_catalog`, `pg_toast`, `information_schema`, etc.) are never imported via FDW
- Schema mapping allows users to control exactly which schemas are imported and under what names

### Overlay Storage Sizing

Default formula: **`5Gi + ceil(snapshot_size_bytes / 10)`**, rounded up to whole Gi.

Later we might consider making the computation configurable.

The computed size is a **ratchet** — it only ever increases. This prevents PVC shrink errors (Kubernetes PVCs can only be expanded, not shrunk). The high-water mark is stored in `status.overlayStorageSize`. If the user sets `storageSizeOverride`, it takes precedence over the auto-computed value (but is still subject to the ratchet).
