# Replica redaction via dbt-masking manifests

## Context

In `~/code/work/bestool`, the `tamanu psql` command loads a per-version "redaction method" that flags which columns in a Tamanu database hold sensitive data. Today it is **display-only**: bestool fetches a dbt manifest from `https://docs.data.bes.au/tamanu/v{version}/manifest.json`, parses out `source.<id>.columns.<col>.config.meta.masking`, and renders matching cells as `"***"` in psql output. The database itself is never modified.

We want PGRO to be able to produce a **redacted replica** — i.e. a restore whose underlying data has actually been anonymised, so any consumer (analytics tools, sandboxes, dev environments) connecting to that replica's Service sees masked data regardless of how they query it.

The Tamanu-on-bes.au setup is one consumer, but the operator should be **generic over the source of the manifest**: any user who publishes a dbt manifest with the same `meta.masking` annotation schema can point a replica at it. PGRO knows about *the manifest schema*, not about Tamanu, not about bes.au, not about `local_system_facts`. The Tamanu deployment just becomes a particular configuration of the generic feature.

Approach (per user decisions in planning):

- Use the **`postgresql_anonymizer`** Postgres extension to do the masking. The manifest provides the *list* of columns to mask, not *how* to mask them; `anon` provides the masking functions (`anon.fake_email()`, `anon.partial(...)`, `anon.random_*`, etc.).
- Drive the redaction step **from inside the operator**, not via a Job. We have `tokio-postgres` and `reqwest` already; the work is fetch-manifest + run-some-SQL, and AGENTS.md prefers operator-driven over scripted-in-Job.
- After redaction completes, **re-enable read-only** so the analytics user can't write to a redacted replica.

The redaction step plugs into the existing replica lifecycle alongside `persistent_schemas`: restore reaches Ready → redact → schema-migrate → switchover.

## Manifest schema (the contract)

PGRO consumes a dbt-shaped JSON document at an HTTP URL. The minimum shape it relies on:

```json
{
  "sources": {
    "<any-id>": {
      "schema": "<schema_name>",
      "name":   "<table_name>",
      "columns": {
        "<col_name>": {
          "config": { "meta": { "masking": <mask_instruction> } }
        }
      }
    }
  }
}
```

Each source must carry explicit `schema` and `name` (the Tamanu dbt manifest always emits them — 163/163 sources in the v2.54.3 manifest). Sources missing either field are skipped with a warning. Source keys are otherwise opaque to PGRO.

### `meta.masking` — canonical contract

The Tamanu masking spec is documented at https://github.com/beyondessential/tamanu/tree/main/database#masking. It is deliberately implementation-agnostic — descriptions are vague about exact behaviour so implementations (bestool's display-only one, this DB-side one, future others) can vary. The bits PGRO has to honour:

- **Short and extended form are equivalent.** `masking: name` ≡ `masking: { kind: name }`. The extended form *must* carry `kind`; it may carry additional parameters (currently only `range`).
- **Nulls are preserved.** When a column value is `NULL` it must stay `NULL` after masking.
- **Two locations.** Column-level masks live under `sources.<id>.columns.<col>.config.meta.masking`. Table-level masks (currently only `truncate`) live under `sources.<id>.config.meta.masking` (and `sources.<id>.meta.masking` mirrors it). PGRO reads both paths.

#### Canonical kinds (full list per the docs)

| kind | scope | behaviour | proposed implementation |
|---|---|---|---|
| `truncate` | **table** | Empty the entire table. | `TRUNCATE TABLE schema.table` as superuser, before the column-mask pass. Not a `SECURITY LABEL`. |
| `date` | column | Anonymise across different dates. Works on `date`/`timestamp(tz)` and on text representations like `character(10)`. | `MASKED WITH FUNCTION anon.random_date()` (or `random_date_between` if we want bounded). Wrap in `CASE WHEN <col> IS NULL THEN NULL ELSE … END` to preserve nulls. |
| `datetime` | column | Anonymise the time-of-day while preserving the date component. Works on `timestamp(tz)` and on text representations like `character(19)`. | Compose: keep `date_trunc('day', <col>)` and add a random interval of seconds. Specifically `date_trunc('day', <col>) + (floor(random() * 86400) || ' seconds')::interval` (cast as needed for text columns). Null-preserved via CASE. |
| `text` | column | Random words/sentences, approximately the same length as the original. | `anon.lorem_ipsum(characters := length(<col>))` (or `words` derived from `length(<col>)/6`). |
| `string` | column | Random printable ASCII, no spaces, approximately the same length as the original. | `anon.random_string(length(<col>))` if the function accepts a dynamic length, else fall back to a fixed length and accept the deviation. |
| `email`, `name`, `phone`, `place`, `url` | column | Fake data of the indicated shape. | `anon.fake_email()` / `anon.fake_first_name()` / a `partial(<col>, 2, '****', 2)`-style call / `anon.fake_city()` / a constructed URL respectively. For `name`, the docs ask us to inspect whether the original contains a space and use full vs single name — implement with `CASE WHEN <col> LIKE '% %' THEN anon.fake_name() ELSE anon.fake_first_name() END`. |
| `zero` | column | Keep the data length identical but replace with zeroes. Primary use: `bytea`. | Type-dispatched (see "Type-aware planning" below). For `bytea`: `repeat(E'\\x00'::bytea, length(<col>))`. For text types: `repeat('0', length(<col>))`. For numeric types: `0`. |
| `empty` | column | Delete the value without nulling: `0` for numbers, `''` for strings, `{}` for json(b), `[]` for arrays, etc. | Type-dispatched. The redaction module looks up each masked column's `data_type` in `information_schema.columns` and emits the appropriate `MASKED WITH VALUE …`. |
| `nil` | column | Null the field. The docs note it only applies to nullable columns. | `MASKED WITH VALUE NULL`. Operator skips columns where `is_nullable = 'NO'` and records the skip as a tolerated error. |
| `default` | column | Set the column to its `DEFAULT` value. The docs note it only applies to columns that have a default. | At planning time, look up `pg_get_expr(adbin, adrelid)` from `pg_attrdef` for the column. If present, emit `MASKED WITH VALUE <expr>`. If absent, tolerated error. |
| `integer` | column | Random integer. Optional `range: "L-H"` constrains the output. | `floor(random() * (H - L + 1) + L)::int` (or `anon.random_int_between(L, H)` if available). Default range `int4` if unspecified. |
| `float` | column | Random float. Optional `range: "L-H"` constrains. | `(random() * (H - L) + L)::numeric` (or `anon.random_in_numrange('[L,H]'::numrange)`). Default unbounded if unspecified. |
| `money` | column | Like `float`/`integer`, but the value is generated as a float with two decimals for `numeric` columns. | Same as `float`, then `round(<expr>, 2)`. |

#### Range parameter parsing

`range: "L-H"` is two numbers joined by a hyphen. Parse by splitting on the **last** `-` so floats like `1.001-1.03` decompose correctly. Both halves must parse as `f64`; parse failures are tolerated errors → fall back to the unbounded variant. Per the docs example (`kind: integer, range: 0-10.5`), the operator accepts a decimal range for `kind: integer` and rounds.

#### Type-aware planning

For `zero`, `empty`, and `default`, the operator can't decide the right `MASKED WITH VALUE` (or function) without knowing the column type / default. So before issuing any `SECURITY LABEL`, the redaction module runs a single batch query against `information_schema.columns` (and `pg_attrdef` for `default`) to resolve `data_type` and `column_default` for every (schema, table, column) tuple it's about to mask. Columns not present in the DB are dropped (tolerated error). The mapping decisions for those three kinds use this metadata.

#### Unknown kinds

If a future manifest version introduces a kind PGRO doesn't recognise (the spec is open-ended), the affected columns are dropped with a tolerated error and the run is reported as `partial`. Adding a new kind is a code change.

## High-level design

A new optional `redaction` field on `PostgresPhysicalReplicaSpec`. When set, after a new restore reaches the `Ready` phase the operator runs the redaction step before the restore is eligible for switchover. The step is tracked in status as `redactionPhase` (`pending` → `active` → `complete` / `partial` / `failed`), mirroring how `schemaMigrationPhase` works today.

Order during a switchover cycle (new restore N replacing active A):
1. N restored from snapshot → `Ready`.
2. **Redaction** runs against N. While running, `default_transaction_read_only` is off on N (we need writes). On success the operator sets it back on via `ALTER DATABASE ... SET default_transaction_read_only = on`.
3. `persistent_schemas` migration A→N (existing behaviour). The schema migration job already runs against N as superuser, so read-only at the DB-default level doesn't block it (superuser is exempt by SET ROLE; `default_transaction_read_only` is a session default, not a hard lock).
4. Switchover Service → N, grace period on A, sweep.

Redaction runs *before* schema migration so that dbt-style views in persistent schemas can be regenerated against already-redacted source tables on the next dbt run.

## CRD changes

`src/types/replica.rs` — add to `PostgresPhysicalReplicaSpec`:

```rust
/// If set, apply a redaction manifest to the restored data before the
/// replica becomes eligible for switchover. Requires Postgres 18+ and
/// the postgresql_anonymizer extension (loaded via image-volume mount,
/// see plan section "Postgres version gate and extension loading").
#[serde(default, skip_serializing_if = "Option::is_none")]
pub redaction: Option<RedactionSpec>,
```

```rust
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RedactionSpec {
    /// HTTP(S) URL of the dbt-style masking manifest. May contain a
    /// `{version}` placeholder, in which case `version` or
    /// `versionQuery` must be set.
    pub manifest_url: String,

    /// Pinned version to substitute into `{version}`. Mutually exclusive
    /// with `versionQuery`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

    /// SQL query that returns a single text column with the version
    /// string. Run against the restore's main database as the operator's
    /// superuser. Mutually exclusive with `version`.
    ///
    /// Example (Tamanu): `SELECT value FROM local_system_facts WHERE key = 'currentVersion'`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_query: Option<String>,

    /// If the manifest URL with the discovered/pinned version 404s,
    /// retry with the major.minor.0 base version. Useful when manifests
    /// are only published for minor releases. Defaults to false.
    #[serde(default)]
    pub version_fallback_to_base: bool,

    /// Override the OCI image used as the source of the
    /// postgresql_anonymizer extension files (mounted as an image
    /// volume on the restore Pod). Defaults to
    /// `registry.gitlab.com/dalibo/postgresql_anon:latest`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension_image: Option<String>,
}
```

Validation: if `manifestUrl` contains `{version}`, exactly one of `version` or `versionQuery` must be set; otherwise both must be unset. The operator rejects malformed spec at reconcile time (no admission webhook today).

Status additions to `PostgresPhysicalReplicaStatus`:

```rust
/// Phase of redaction: pending, active, complete, partial, failed.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub redaction_phase: Option<String>,

/// Resolved manifest version used in the last redaction run.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub redaction_version: Option<String>,

/// Number of columns redacted in the last run.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub redaction_columns_applied: Option<u32>,
```

Update the readme CRD tables (AGENTS.md exception explicitly allows this).

### Example (Tamanu)

```yaml
spec:
  redaction:
    manifestUrl: "https://docs.data.bes.au/tamanu/v{version}/manifest.json"
    versionQuery: "SELECT value FROM local_system_facts WHERE key = 'currentVersion'"
    versionFallbackToBase: true
```

### Example (pinned)

```yaml
spec:
  redaction:
    manifestUrl: "https://example.com/redactions/manifest.json"
```

## New module: `src/controllers/replica/redaction.rs`

Single module exposing:

```rust
pub async fn reconcile_redaction(
    ctx: &Context,
    replica: &PostgresPhysicalReplica,
    restore_name: &str,
) -> Result<RedactionOutcome>;
```

Internals:

1. **Resolve version**.
   - If `spec.redaction.version` is set, use it.
   - Else if `spec.redaction.versionQuery` is set, connect to the restore as superuser and run the query. Expect a single row with a single text column; error clearly on shape mismatch.
   - Else if `manifestUrl` contains `{version}`, fail at validation (covered in CRD validation above).
   - Else (no `{version}` in URL), no version is needed.

2. **Fetch the manifest** via `ctx.http_client` (existing `reqwest::Client`). No caching: redaction runs at most once per restore (i.e. once per scheduled cycle, on the order of hours), so the bandwidth/time saved by caching is negligible against the risk of invalidation bugs. If `versionFallbackToBase` is true and the first fetch is a 404, retry with `{major}.{minor}.0` derived from a `MAJOR.MINOR.PATCH`-shaped version (mirroring bestool's `get_base_version`); silently no-op the fallback for versions that don't match that shape.

3. **Parse the manifest** with `serde_json::Value`. Read each source's `schema` and `name`. Then collect masks:
   - **Table-level**: if `sources.<id>.config.meta.masking` (or `meta.masking` as fallback) is `truncate`, add a `TableMask::Truncate` for that source.
   - **Column-level**: iterate `sources.<id>.columns.*` and collect those with a non-null `config.meta.masking`. Normalise short-form (`"name"`) to extended (`{ "kind": "name" }`). Return:

   ```rust
   struct ColumnMask {
       schema: String,
       table: String,
       column: String,
       kind: String,                        // "email", "date", "integer", ...
       range: Option<(f64, f64)>,           // parsed from "L-H", last-dash split
   }

   enum TableMask { Truncate { schema: String, table: String } }
   ```

   Unknown kinds and malformed ranges are kept (carrying their kind string) and rejected later, not at parse time, so the operator can count them as tolerated errors with useful context.

4. **Type-aware planning**. Open the superuser connection (used in step 5). For every `ColumnMask`, look up `data_type`, `is_nullable`, and `column_default` via a single batch query against `information_schema.columns` LEFT JOIN `pg_attrdef`. Drop entries where the column doesn't exist (tolerated error). The kinds `zero`, `empty`, and `default` use this metadata to choose their `MASKED WITH VALUE …` / function expression; `nil` uses `is_nullable` to skip non-nullable columns; everything else ignores the type.

5. **Resolve each `ColumnMask` → SECURITY LABEL fragment** per the canonical table. The function returns either:
   - `MASKED WITH VALUE <expr>` for kinds that resolve to a constant or pg-default expression (`nil`, `empty`, `default`, `zero`-on-numeric), or
   - `MASKED WITH FUNCTION <expr>` for kinds that need a function call (most fakes, `text`, `string`, `datetime`-arithmetic, `integer`/`float`/`money`).
   For column kinds where "nulls preserved" matters (most of them; not `nil`/`empty`/`default` which intentionally overwrite nulls per the docs), wrap the expression in `CASE WHEN <col> IS NULL THEN NULL ELSE <expr> END`.

6. **Apply masking via the extension**:
   - Open a tokio-postgres connection to the restore as superuser, against the main user database (use `postgres::discover_restore_database`).
   - `CREATE EXTENSION IF NOT EXISTS anon CASCADE;` (pulls in pgcrypto).
   - `SELECT anon.init();` (loads the fake-data tables; idempotent).
   - For each `TableMask::Truncate`: `TRUNCATE TABLE {quote_ident(schema)}.{quote_ident(table)};` Tolerated errors counted.
   - For each `ColumnMask` (after type-aware planning): emit the `SECURITY LABEL FOR anon ON COLUMN …` statement. Tolerated errors counted.
   - `SELECT anon.anonymize_database();` — destructive in-place rewrite of all labelled columns.
   - Leave the extension installed (don't `DROP EXTENSION`). The SECURITY LABELs and the fake-data tables stay around (~7 MB) so analytics consumers can call anon functions themselves if useful. Dynamic masking (`MASKED ROLE`) is **not** enabled — that would need `shared_preload_libraries = 'anon'` and a role-grant step, and is out of scope.

7. **Re-enable read-only**:
   - `ALTER DATABASE {quote_ident(dbname)} SET default_transaction_read_only = on;` — applies to all new sessions on that DB.
   - If the wider `spec.read_only` is true and `persistent_schemas` is also set, the existing override (line 686 of `restore/builders.rs`) forces postgresql.conf to `off` cluster-wide; the ALTER DATABASE-level setting still takes effect for non-superuser sessions because it's applied last in the GUC resolution order. The schema-migration Job runs as superuser, so it isn't blocked. Verify this assumption during implementation; if it doesn't hold, fall back to issuing `SELECT pg_reload_conf();` after rewriting postgresql.conf inline.

Returns `RedactionOutcome { version: Option<String>, columns_attempted, columns_failed }`.

Errors propagate; the caller writes the status patch.

## Reconciler wiring

`src/controllers/replica.rs`:

- After a new restore reaches `Ready`, before the switchover branch:
  - Check the restore's PG version (already populated in `status.postgresVersion`). If `spec.redaction.is_some()` and the version is < 18, set phase `"failed: redaction requires PostgreSQL 18+"` and skip switchover.
  - If `spec.redaction.is_some()` and `status.redaction_phase != Some("complete")` and `!= Some("partial")`:
    - Set phase `"active"` in status.
    - Call `redaction::reconcile_redaction(ctx, replica, &new_restore_name)`.
    - On Ok: set phase `"complete"` or `"partial"` (depending on per-statement error count) and store `redaction_version` + `redaction_columns_applied`.
    - On Err: set phase `"failed: {msg}"` and return early so the reconciler retries.
  - The new restore is not eligible to become the switchover target until phase is `complete` or `partial`.
- Schema migration (`reconcile_schema_migration`) gates on redaction completing first — extend its early-return check so it doesn't kick off until `redaction_phase` is settled (when redaction is configured).
- On every new restore created by the schedule, `redaction_phase` resets to `None` (so the next restore re-runs redaction).

## Postgres version gate and extension loading

Redaction is **PG 18+ only**. PG 18 introduces `extension_control_path` and `dynamic_library_path` as runtime-settable GUCs, which lets us mount the extension files via a Kubernetes image volume instead of having to ship a custom Postgres image with anon pre-baked.

Two enforcement points:

1. **CRD-level rejection at reconcile time**: when `spec.redaction` is set and the restore's `status.postgresVersion` resolves to anything < 18 (or the cluster's discovered PG version from the snapshot is < 18), set `status.redactionPhase = "failed: redaction requires PostgreSQL 18+"` and refuse the switchover. Don't try to silently bump versions or fall back.

2. **Extension availability**: when redaction is configured, the operator mounts the postgresql_anonymizer extension files into the restore Pod via a Kubernetes [image volume](https://kubernetes.io/docs/concepts/storage/volumes/#image) (Kubernetes 1.34 is in use, so the feature is GA-available). The restore Pod builder:

   - Adds a volume `image: <spec.redaction.extensionImage or default>` mounted at `/extensions/anon`. Default image: `registry.gitlab.com/dalibo/postgresql_anon:latest`.
   - Appends to the generated postgresql.conf:
     ```
     extension_control_path = '$system:/extensions/anon/share'
     dynamic_library_path   = '$libdir:/extensions/anon/lib'
     ```
   These GUCs were introduced in PG 18, which is why redaction is gated to PG 18+.

The redaction reconciler then runs `CREATE EXTENSION anon CASCADE` once Postgres is up; the extension files are already on disk thanks to the volume mount.

## Tests

- Unit tests in `redaction.rs`:
  - `parse_manifest` round-trip: short-form (`"name"`) and extended-form (`{"kind":"integer","range":"20-50"}`) both normalised to the same `ColumnMask`; table-level `truncate` recognised; missing-schema/missing-name source skipped; unknown kind preserved verbatim.
  - `base_version` fallback derivation (mirror bestool's `get_base_version` cases).
  - `range` parsing: last-dash split handles `1.001-1.03`, integer rounding for `0-10.5`, parse failures fall back to unbounded.
  - Fragment building for each canonical kind, including the type-dispatched ones (`zero` / `empty` / `default` against fixture `data_type` and `column_default` lookups), the `name` space-detection CASE, and the null-preserving CASE wrappers.
- Integration test under `tests/` — **deferred to a follow-up**. The existing kopia-repo fixture snapshots PG 16; redaction requires PG 18+. Landing the test means adding a `setup-kopia-repo-pg18.yaml` fixture, a sample-manifest HTTP server fixture, pre-pulling `postgres:18` and the anon extension image onto the kind node, and a new matrix entry in `.github/workflows/integration.yml`. Each step is straightforward but the combined surface is large enough to be its own change.

## Files to touch

| File | Change |
|---|---|
| `src/types/replica.rs` | Add `redaction` to spec (`RedactionSpec` struct); `redaction_phase`/`redaction_version`/`columns_applied` to status. |
| `src/controllers/replica/redaction.rs` (new) | Whole module: manifest fetch/parse, mask-instruction parsing + registry, SQL application, PG-18 version check. |
| `src/controllers/replica.rs` | Wire `reconcile_redaction` into the post-Ready, pre-switchover branch; gate switchover on redaction phase; reset phase on new restore. PG-version gate. |
| `src/controllers/replica/schema_migration.rs` | Make `reconcile_schema_migration` wait for redaction-complete when redaction is configured. |
| `src/controllers/restore/builders.rs` | When `spec.redaction.is_some()`, inject the postgresql_anonymizer image volume into the restore Pod and append `extension_control_path` / `dynamic_library_path` to postgresql.conf. Refuse to build the restore Pod with PG < 18 when redaction is set. |
| `src/controllers/postgres.rs` | No change — `connect_to_restore`, `discover_restore_database`, `quote_ident` already cover what we need. |
| `src/context.rs` | No change. |
| `Cargo.toml` | No new deps (uses existing `tokio-postgres`, `reqwest`, `serde_json`). |
| `README.md` | Update CRD tables only (AGENTS.md explicit exception). |
| `.github/workflows/integration.yml` | Add matrix entry for the new integration test file. |

## Verification

- `cargo clippy` and `cargo fmt` clean per AGENTS.md.
- Unit tests pass.
- End-to-end against a real cluster: create a `PostgresPhysicalReplica` with the Tamanu `redaction:` example above, observe the new restore reaches `Ready`, then `redactionPhase` transitions `active` → `complete`, then schema migration runs, then switchover. Connect as the analytics user and confirm:
  - The flagged columns return masked values.
  - `SELECT pg_settings WHERE name = 'default_transaction_read_only'` is `on` for a fresh analytics session.
  - An attempted `INSERT` as analytics user is rejected.

## Open items / follow-ups

- **Default extension image tag** — the plan defaults `extensionImage` to `registry.gitlab.com/dalibo/postgresql_anon:latest`. `:latest` is brittle; users who want stability should pin a tag in their spec. Worth revisiting the default to a pinned digest once we know which dalibo build works against the Tamanu manifest in practice.
- **New canonical kinds** — the Tamanu masking spec is intentionally open-ended. If a future manifest introduces a new kind, redaction will report it as a tolerated error and complete as `partial`; adding support is a code change.
- **anon function names** — the SQL fragments above use plausible-but-not-verified names from postgresql_anonymizer. During implementation, validate each against the dalibo docs (or `\df anon.*` in an installed instance) and adjust. Particular ones to double-check: `random_in_int4range` vs `random_int_between`, whether `random_string` accepts a dynamic length, whether `lorem_ipsum` has a `characters :=` parameter, whether `random_date_between` exists.
