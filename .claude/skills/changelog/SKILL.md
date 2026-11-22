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
- the Spring Boot starter: name it "the Spring Boot starter" in the item, not just "the starter";
- a new page in `docs/`.

Leave out tests, `test-support`, `performance`, `examples`, build logic, CI, the Gradle wrapper, KDoc-only changes and
internal refactorings.

## Entry format

```
## [0.239.0](https://github.com/vgv/kolbasa/compare/v0.238.0...v0.239.0) - 2026-11-21

The most important change, in a few words ([#177](https://github.com/vgv/kolbasa/pull/177)).

### Breaking changes

- ...
```

The line under the heading is the summary: the most important change of the release, in a few words, with the PR
link. If the release has two or more important changes, list them instead:

```
Highlights ([#146](https://github.com/vgv/kolbasa/pull/146)):

- Queue pipes
- `QueueProcessor` moved to `kolbasa.worker.processor`
```

Important means a breaking change, a schema change the user must act on, or a big new feature, not every item.
Write each one in a few words, not a sentence.

The version in the heading links to the diff from the previous tag to this one.

Groups, in this order, only the non-empty ones: `Breaking changes`, `Schema`, `Added`, `Changed`, `Deprecated`,
`Fixed`, `Security`.

- `Breaking changes`: anything that makes existing code fail to compile, or makes it behave differently in a way the
  user must react to. Removed and renamed API goes here, so there is no `Removed` group.
- `Schema`: every change to queue tables. If it needs anything from the user, add a line to `Breaking changes` too.
- A release with nothing a user notices gets the heading and the summary, nothing else.

Writing:

- English. Short, plain sentences.
- One item, one change. Name the API exactly, in backticks.
- Say each thing once. When new API is what fixes a bug, the `Fixed` item tells what went wrong and what happens
  now, and the `Added` item only names the API and what it is for.
- Keep one feature in one item, even if it touches several groups: the same option for two modes is one item.
- `Added`: one item per feature, naming its main classes and what it is for. Don't list options and defaults: KDoc
  has them.
- A breaking change says what to write instead. Add a short Kotlin snippet when the new code isn't obvious. Add a
  Java snippet only where Java is written differently (`INSTANCE`, a builder), with a line before it.
- A fix says what the user saw go wrong, then what happens now. Not what changed in the code.
- No PR links inside items, no commit hashes.

## Writing the file

1. Show the entry in chat first. Write nothing until the user agrees.
2. If `CHANGELOG.md` doesn't exist, create it with the line `# Changelog`.
3. Put the entry in its place by version, newest first. If the tag already has an entry, show the old one next to
   the new one, then replace it.
4. Don't commit.
