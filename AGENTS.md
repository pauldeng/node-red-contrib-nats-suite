# Agent instructions

node-red-contrib-nats-suite is a Node-RED 5 package of 12 nodes wrapping `@nats-io/*@3.4.0` (Core, JetStream, KV, Object Store, Services).

Floors: Node.js `>=22.9.0`, Node-RED `>=5.0.0`, nats-server `>=2.14`. No new runtime dependencies. No backward-compatible fallbacks for missing flow properties — current editor defaults win.

## Commands

```bash
docker compose up -d nats-server nodered
npm test                          # real stack; node:test --test-concurrency=1
node --test --test-concurrency=1 test/<file>.test.js   # focused; npm test -- file still runs the glob
npm run lint
npm pack --dry-run
```

After `nodes/*.js` edits: `docker compose restart nodered` before any container test — Node-RED `require()`-caches the module at process start; flow redeploy does not reload it.

After `nodes/*.html` edits: restart `nodered` and hard-reload the editor (`Ctrl+Shift+R`). A plain refresh serves the cached `/nodes` bundle.

## Releasing

The npm name is `@pauldeng/node-red-contrib-nats-suite` (scoped, like `@pauldeng/node-red-contrib-redis`). The unscoped `node-red-contrib-nats-suite` is a different package owned by someone else.

npm cannot attach a trusted publisher until the package exists, so the **first** version is a one-time local publish from a logged-in session (browser 2FA). Do not put an `NPM_TOKEN` in GitHub. After that, every later version publishes only from GitHub Actions over OIDC.

```bash
npm login
npm publish --access public
```

Then on npmjs.com (package → Settings → Trusted publishers → GitHub Actions):

| Field             | Value                         |
| ----------------- | ----------------------------- |
| Organization/user | `pauldeng`                    |
| Repository        | `node-red-contrib-nats-suite` |
| Workflow filename | `release.yml`                 |
| Environment       | leave empty                   |
| Allowed actions   | `npm publish`                 |

Or from a logged-in CLI: `npm trust github @pauldeng/node-red-contrib-nats-suite --file release.yml --repo pauldeng/node-red-contrib-nats-suite --allow-publish`. Renaming `release.yml` invalidates that row — update npmjs.com in the same change.

Later releases:

1. Bump `version` in `package.json` and add a `CHANGELOG.md` entry.
2. Merge to `main`.
3. Create a GitHub Release tagged `v<version>` (must match `package.json`). `workflow_dispatch` is recovery only.

Confirm with `npm view @pauldeng/node-red-contrib-nats-suite@<version> dist` — a CI publish has an `attestations` field.

After the first OIDC publish works: npmjs.com → package Settings → Publishing access → require 2FA and disallow tokens. Then add the node on flows.nodered.org (catalogue does not auto-index).

## Read when the task touches that surface

- [Runtime](docs/agents/runtime.md) — `done()`, Catch, close, connect timeout vs events
- [Testing](docs/agents/testing.md) — real Node-RED + real NATS, what not to mock
- [Editor](docs/agents/editor.md) — defaults, theme variables, help
- [NATS client and layout](docs/agents/nats-and-layout.md) — API source of truth, `lib/` vs `nodes/`
