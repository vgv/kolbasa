---
name: changelog
description: Write the CHANGELOG.md entry for one release tag, from the diff between it and the tag before it.
argument-hint: <tag>
disable-model-invocation: true
allowed-tools: Bash(git tag:*), Bash(git log:*), Bash(git show:*), Bash(git diff:*), Bash(git rev-parse:*)
---

# Changelog entry for a release tag

Write the `CHANGELOG.md` entry for the tag in `$ARGUMENTS`. Every push to `main` is a release with its own tag, so the
entry describes exactly one merged PR.

## Tag

- One tag. The `v` is optional: `v0.239.0` and `0.239.0` are the same.
- No tag, more than one, or a tag that doesn't exist: say so and stop.
- The previous tag is the one before it in version order (`git tag -l 'v*' --sort=v:refname`).

## What to read

For the tag `T` and the previous tag `P`:

- `git log -1 --format='%cs%n%B' T` — the release date and the merge commit message, which holds the PR number and
  title.
- `git log --format='%h %s' P..T` and `git diff --stat P..T` — what changed, and where.
- Then the diffs themselves, file by file. Read the code at the tag (`git show T:path`) when the diff alone doesn't
  say what a change does. Check every claim against the code. Never guess from a commit message or a name.

## What goes in

Only what a user of kolbasa notices:

- the public API of the three published modules: `kolbasa`, `kolbasa-opentelemetry`, `kolbasa-spring-boot-4-starter`
  (`*/src/main`, declarations that aren't `internal` or `private`);
- behavior: defaults, what is thrown and when, what is logged, what is retried;
- the queue schema (`kolbasa/src/main/kotlin/kolbasa/schema`): new or dropped columns and indexes, and what happens to
  existing tables on the next `createOrUpdateQueues`;
- dependencies a user gets: runtime dependencies of the published modules, and minimum supported versions (JVM,
  PostgreSQL, Micrometer, OpenTelemetry, Spring Boot);
- `butcher` (`butcher/src/main`): commands, options, output. Start such an item with `butcher:`;
- a new page in `docs/`.

Leave out tests, `test-support`, `performance`, `examples`, build logic, CI, the Gradle wrapper, KDoc-only changes and
internal refactorings.

## Entry format

```
## [0.239.0] - 2026-11-22

One sentence on what the release is about ([#177](https://github.com/vgv/kolbasa/pull/177)).

### Breaking changes

- ...
```

Groups, in this order, only the non-empty ones: `Breaking changes`, `Schema`, `Added`, `Changed`, `Deprecated`,
`Fixed`, `Security`.

- `Breaking changes`: anything that makes existing code fail to compile, or makes it behave differently in a way the
  user must react to. Removed and renamed API goes here, so there is no `Removed` group.
- `Schema`: every change to queue tables. If it needs anything from the user, add a line to `Breaking changes` too.
- A release with nothing a user notices gets the heading and the sentence, nothing else.

Writing:

- English. Short, plain sentences.
- One item, one change. Name the API exactly, in backticks.
- A breaking change says what to write instead. Add a short Kotlin snippet when the new code isn't obvious. Add a
  Java snippet only where Java is written differently (`INSTANCE`, a builder), with a line before it.
- A fix says what the user saw go wrong, then what happens now. Not what changed in the code.
- No PR links inside items, no commit hashes.

Each entry has a link line at the bottom of the file, newest first:

```
[0.239.0]: https://github.com/vgv/kolbasa/compare/v0.238.0...v0.239.0
```

## Writing the file

1. Show the entry in chat first. Write nothing until the user agrees.
2. If `CHANGELOG.md` doesn't exist, create it with this header:
   ```
   # Changelog

   What changed in each release of kolbasa (`io.github.vgv:kolbasa`, `kolbasa-opentelemetry`,
   `kolbasa-spring-boot-4-starter`) and `butcher`, newest first. The format follows
   [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). History starts at 0.222.0.
   ```
3. Put the entry in its place by version, newest first, and its link line in its place at the bottom. If the tag
   already has an entry, show the old one next to the new one, then replace it.
4. Don't commit.
