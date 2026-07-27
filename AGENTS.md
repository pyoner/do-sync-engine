<!--VITE PLUS START-->

# Using Vite+, the Unified Toolchain for the Web

This project is using Vite+, a unified toolchain built on top of Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task. Vite+ wraps runtime management, package management, and frontend tooling in a single global CLI called `vp`. Vite+ is distinct from Vite, and it invokes Vite through `vp dev` and `vp build`. Run `vp help` to print a list of commands and `vp <command> --help` for information about a specific command.

Docs are local at `node_modules/vite-plus/docs` or online at https://viteplus.dev/guide/.

## Review Checklist

- [ ] Run `vp install` after pulling remote changes and before getting started.
- [ ] Run `vp check`, `vp test`, and `vp run -r test` to format, lint, type check, and test changes.
- [ ] Check if there are `vite.config.ts` tasks or `package.json` scripts necessary for validation, run via `vp run <script>`.
- [ ] If setup, runtime, or package-manager behavior looks wrong, run `vp env doctor` and include its output when asking for help.

## Workspace Tests

- Use `vp run -r test` as the canonical full-workspace test command. It runs every workspace package's `test` script, including the SQL adapter's Node and Cloudflare projects and the todo demo's Workers tests.
- Use `vp run -r --no-cache -v test` when verifying changed test configuration; it disables cached results and prints every selected workspace task.
- Root `vp test` intentionally excludes Workers-only suites because the root configuration uses the Node pool. Those suites are not skipped by `vp run -r test`; their package scripts run them with `cloudflareTest()`.
- Keep `--no-file-parallelism` in the SQL adapter and todo demo test scripts. It bounds `workerd` resource use without excluding test files.

<!--VITE PLUS END-->

## Default Agent Skills

- Use the Caveman skill by default for terse, filler-free communication. Default mode: full.
- Use the Ponytail skill by default for minimal, YAGNI implementation. Default mode: full.
