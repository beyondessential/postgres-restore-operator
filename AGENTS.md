- When adding or changing features, or when fixing bugs, add tests whenever possible.
- When adding integration tests, they don't run locally, so ensure the user knows about them so they can be tested in CI. Also add a matrix entry for the new test file in `.github/workflows/integration.yml`.
- Never write documentation files or readmes. Exception: when adding or changing items in the CRD specs, also change the tables in the readme.
- Always run `cargo clippy` and `cargo fmt` before committing changes.
- Use conventional commit messages.
- Never write useless comments that only repeat the code.
- Never print summaries or unnecessary information.
- Don't use emojis unless absolutely necessary.
- When removing code that has already been committed, delete it unless explicitly requested that it be commented out.
- Prefer using small dependencies instead of reimplementing the wheel. Ask the user to pick a dependency if there is no obvious choice.
- Use `--no-pager` with `git log`, `git diff`, etc commands. The option goes before the subcommand, e.g. `git --no-pager log`. NEVER use any interactive commands, including commands that require an editor. You can't use those and they'll just block you.
- Imports: merge them and group them by std, then third-party/workspace, then local (crate, super, self).
- `use` statements always go before `mod` statements.
- Ask the user instead of making an assumption if there's a major detail missing from instructions that could affect code quality or implementation design.
- When writing parsers, unless very trivial, implement them using winnow.
- Use the newer `foo.rs` / `foo/sub.rs` style of modules.
- ALWAYS use the edit tool to edit or write file, NEVER use "cat >> EOF". YOU WILL LOSE DATA.
- Never write long summaries at the end of responses. Maximum 50 words if absolutely necessary.
- To silence a warning, use `#[expect(..., reason = "...")]` instead of `#[allow(...)]`.
- Don't use double spaces after punctuation.
- Always work from a branch. If you're on `main`, create a new branch.
- If you can at all do something from the operator, instead of in a script that runs in a job, do so. For example, if you need to query something in a database, you should be able to do so from the operator, instead of writing SQL code in bash logic.
- You are FORBIDDEN from suggesting to change the build behaviour of bestool-canopy wrt CANOPY_OPENAPI_OFFLINE or any other way. The way it works is as designed. DO NOT EVER re-litigate it.

## Lessons learned

- Detect dead Postgres connections with **TCP keepalives** (`socket2::TcpKeepalive` on the raw `TcpStream` before handing it to `tokio_postgres::Config::connect_raw`), not with a wall-clock `tokio::time::timeout` around the query. Keepalives fire on the kernel's own timer regardless of in-flight queries, so a legitimately long-running statement (e.g. `DROP SCHEMA … CASCADE` on a populated dbt schema, which can run for tens of minutes) keeps working, while a NAT-evicted / silently-dead socket still errors within ~90s and the reconcile retries.
- Wall-clock timeouts on a long-tail-distributed operation make a retry loop **never converge**: every attempt hits the cap, error policy retries, every retry hits the cap again. Don't add a timeout unless you have a real p99 number for the operation it wraps.
- The operator's own libpq sockets are separate from the migration Job's libpq URI. Keepalive fixes in the Job (`keepalives=1&keepalives_idle=...`) do **not** cover the operator — set both.
- The kube port-forward path is not a raw TCP socket; socket-level keepalives don't apply. That path keeps its own liveness via the API server WebSocket — only `connect_via_tcp` needs the socket2 setup.
