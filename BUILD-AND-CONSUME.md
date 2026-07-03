# Building this fork and using it in another project

This fork of `pekko-persistence-r2dbc` adds the opt-in `StreamBatchedWriteJournal` (see
[`docs/src/main/paradox/journal.md`](docs/src/main/paradox/journal.md) -> "Batched writes"). This guide explains how to
build the artifact and depend on it from another project, either locally or via GitHub Packages.

## Which build do you need? (your Pekko line matters)

`pekko-persistence-r2dbc` is pinned to a specific Pekko core version, so you must use the build that matches **your
app's Pekko line** or you will hit binary-incompatible eviction errors. Two builds are published from this fork:

| Your app's Pekko line | Use this version | Built against | Branch |
|---|---|---|---|
| official `pekko-persistence-r2dbc 1.1.x` (Pekko **1.2.0**) | **`1.1.1-batched-journal-SNAPSHOT`** | Pekko 1.2.0, Scala 2.13.16 / 3.3.6 | `batched-journal-1.1.x` |
| Pekko **2.0.0-M3** (next-major milestone) | `1.1.0-batched-journal-SNAPSHOT` | Pekko 2.0.0-M3, Scala 2.13.18 / 3.3.8 | `stream-batched-write-journal` |

Most apps on a released Pekko are on the **1.1.x** line, so use **`1.1.1-batched-journal-SNAPSHOT`**.

### The adaptive `UNNEST` build (this branch)

The `unnest-stream-batched-write-journal` branch adds, on top of the batched journal, an experimental **`UNNEST`-based
multi-row insert** with an **adaptive** switch (see
[`docs/src/main/paradox/journal.md`](docs/src/main/paradox/journal.md) -> `batch.use-unnest`). It is published locally
for both Pekko lines:

| Your app's Pekko line | Use this version | Scala | Built from |
|---|---|---|---|
| Pekko **2.0.0-M3** (milestone) | **`1.1.0-batched-unnest-SNAPSHOT`** | 2.13 / 3.3 | `unnest-stream-batched-write-journal` |
| official `1.1.x` (Pekko **1.2.0**) | **`1.1.1-batched-unnest-SNAPSHOT`** | 2.12 / 2.13 / 3.3 | `unnest-1.1.x` (port onto `batched-journal-1.1.x`) |

Publish whichever matches your app (Ivy + Maven local, all Scala versions):

```shell
# Pekko 2.0.0-M3 line (from the unnest-stream-batched-write-journal branch)
sbt -java-home "$JAVA_HOME" 'set ThisBuild / version := "1.1.0-batched-unnest-SNAPSHOT"' +core/publishLocal +core/publishM2

# stable 1.1.x / Pekko 1.2.0 line (from the unnest-1.1.x branch)
sbt -java-home "$JAVA_HOME" 'set ThisBuild / version := "1.1.1-batched-unnest-SNAPSHOT"' +core/publishLocal +core/publishM2
```

Enable it with `batch.use-unnest = on` (see "Using the journal" below). It is **off by default**, only engages for the
postgres/yugabyte dialects with `bytea` payloads, and automatically falls back to the `add()` batch for events above
`batch.unnest-max-payload-size` (default `8 KiB`).

> The `1.1.1-...` version deliberately outranks the official `1.1.0` release so your app resolves the fork; still add a
> `dependencyOverrides` (sbt) / `resolutionStrategy.force` (Gradle) / `<dependencyManagement>` pin (Maven) to be certain.



### Make your build pick it (fixes "Plugin class name must be defined" / eviction)

Because the fork shares `org.apache.pekko:pekko-persistence-r2dbc` with the official artifact, your app can resolve the
official one instead - and a *release* (`1.1.0`) outranks a same-numbered `-SNAPSHOT`, which is why `1.1.0-...-SNAPSHOT`
loses. `1.1.1-...` already outranks the official `1.1.0`, but to be certain, **force** it:

sbt:
```scala
libraryDependencies   += "org.apache.pekko" %% "pekko-persistence-r2dbc" % "1.1.1-batched-journal-SNAPSHOT"
dependencyOverrides   += "org.apache.pekko" %% "pekko-persistence-r2dbc" % "1.1.1-batched-journal-SNAPSHOT"
```

Maven: pin it in `<dependencyManagement>`. Gradle:
`resolutionStrategy.force("org.apache.pekko:pekko-persistence-r2dbc_3:1.1.1-batched-journal-SNAPSHOT")`.

Verify with `sbt evicted` / `sbt dependencyTree` (resolved version) and that the jar on the classpath actually has the
feature: `unzip -p <jar> reference.conf | grep batched-journal`.

## Coordinates

| | |
|---|---|
| group id | `org.apache.pekko` |
| artifact | `pekko-persistence-r2dbc_2.13` (Scala 2.13) or `pekko-persistence-r2dbc_3` (Scala 3) |
| version | depends on your Pekko line - see "Which build do you need?" above |
| built against | Pekko 1.2.0 (1.1.x build) or Pekko 2.0.0-M3 (main build) |

> **Compatibility:** your consuming project must use the **matching Pekko version** (1.2.0 for the `1.1.x` build,
> 2.0.0-M3 for the `main` build) and the matching Scala binary version (`_2.13` or `_3`). Mixing Pekko versions causes
> eviction / `NoSuchMethodError`.

## Prerequisites

- **JDK 17** (the build targets 17). Any JDK 17 works; with coursier:

  ```shell
  export JAVA_HOME=$(cs java-home --jvm temurin:1.17.0.0)
  export PATH="$JAVA_HOME/bin:$PATH"
  ```

  If your `sbt` launcher picks a different JDK, pass it explicitly: `sbt -java-home "$JAVA_HOME" <commands>`.
- **sbt** (the repo pins the sbt version).

### Pick a version

The build derives its version from git tags via dynver. On an untagged branch that yields an unstable, timestamped
value containing `+` characters (e.g. `0.0.0+614-e1feafa8+20260625-2056-SNAPSHOT`), which changes every build and is
rejected by Maven/GitHub Packages. **Always set an explicit version** when building this fork, for example:

```shell
sbt 'set ThisBuild / version := "1.1.0-batched-journal-SNAPSHOT"' <publish task>
```

Keep it distinct from official releases. A `-SNAPSHOT` suffix is republishable; a release version is immutable on
GitHub Packages. The examples below use `1.1.0-batched-journal-SNAPSHOT`.

---

## Option A - Publish to your machine (simplest)

Best when the consuming project is built on the same machine.

### A1. sbt consumer (Ivy local, `~/.ivy2/local`)

Build and publish (`+` builds both Scala 2.13 and 3.3; drop it to build only 2.13):

```shell
sbt -java-home "$JAVA_HOME" 'set ThisBuild / version := "1.1.0-batched-journal-SNAPSHOT"' +core/publishLocal
```

In the **other** project's `build.sbt`:

```scala
val PekkoVersion = "2.0.0-M3"

libraryDependencies ++= Seq(
  "org.apache.pekko" %% "pekko-persistence-r2dbc" % "1.1.0-batched-journal-SNAPSHOT",
  // align core Pekko to the same milestone to avoid evictions
  "org.apache.pekko" %% "pekko-persistence-typed" % PekkoVersion,
  "org.apache.pekko" %% "pekko-stream"            % PekkoVersion)
```

sbt reads `~/.ivy2/local` automatically - no resolver needed.

### A2. Maven / Gradle consumer (Maven local, `~/.m2/repository`)

```shell
sbt -java-home "$JAVA_HOME" 'set ThisBuild / version := "1.1.0-batched-journal-SNAPSHOT"' +core/publishM2
```

Maven `pom.xml` (note the Scala suffix is part of the artifactId):

```xml
<dependency>
  <groupId>org.apache.pekko</groupId>
  <artifactId>pekko-persistence-r2dbc_2.13</artifactId>
  <version>1.1.0-batched-journal-SNAPSHOT</version>
</dependency>
```

Maven uses `~/.m2` automatically. Gradle:

```kotlin
repositories { mavenLocal(); mavenCentral() }
dependencies {
  implementation("org.apache.pekko:pekko-persistence-r2dbc_2.13:1.1.0-batched-journal-SNAPSHOT")
}
```

---

## Option B - Publish to GitHub Packages (share across machines / CI)

GitHub Packages hosts Maven artifacts scoped to a repository. sbt can both publish and consume them.

### B1. Publish (one-time: create a token)

Create a GitHub **Personal Access Token** (classic) with the `write:packages` scope. Then publish with the helper
script [`publish-github.sh`](publish-github.sh), which overrides `publishTo`/credentials only for that invocation (it
does not modify the build or affect CI):

```shell
export GITHUB_REPOSITORY="your-user/pekko-persistence-r2dbc"   # owner/repo that will host the package
export GITHUB_ACTOR="your-user"
export GITHUB_TOKEN="ghp_xxx"                                  # PAT with write:packages
VERSION="1.1.0-batched-journal-SNAPSHOT" ./publish-github.sh
```

If your sbt launcher is not already on JDK 17, also `export SBT_JAVA_HOME=$(cs java-home --jvm temurin:1.17.0.0)`.

Equivalent without the script (override `publishTo` at publish time with `set` - this is reliable; a committed build
file would be shadowed by the Apache Sonatype plugin):

```shell
sbt 'set ThisBuild / version := "1.1.0-batched-journal-SNAPSHOT"' \
    'set LocalProject("core") / publishTo := Some("GitHub Packages" at "https://maven.pkg.github.com/your-user/pekko-persistence-r2dbc")' \
    'set LocalProject("core") / credentials += Credentials("GitHub Package Registry", "maven.pkg.github.com", sys.env("GITHUB_ACTOR"), sys.env("GITHUB_TOKEN"))' \
    +core/publish
```

The package then appears under your repo's "Packages". `-SNAPSHOT` versions are republishable; release versions are
immutable on GitHub Packages.

### B2. Consume from GitHub Packages

The consumer needs a token with the `read:packages` scope.

**sbt** (consuming project's `build.sbt`):

```scala
resolvers += "GitHub Packages" at "https://maven.pkg.github.com/your-user/pekko-persistence-r2dbc"
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env("GITHUB_ACTOR"),
  sys.env("GITHUB_TOKEN")) // PAT with read:packages

libraryDependencies += "org.apache.pekko" %% "pekko-persistence-r2dbc" % "1.1.0-batched-journal-SNAPSHOT"
```

**Maven** - in `~/.m2/settings.xml`:

```xml
<servers>
  <server>
    <id>github-pekko-r2dbc</id>
    <username>your-user</username>
    <password>ghp_read_packages_token</password>
  </server>
</servers>
```

and in `pom.xml`:

```xml
<repositories>
  <repository>
    <id>github-pekko-r2dbc</id>
    <url>https://maven.pkg.github.com/your-user/pekko-persistence-r2dbc</url>
  </repository>
</repositories>
```

**Gradle** (`build.gradle.kts`):

```kotlin
repositories {
  mavenCentral()
  maven {
    url = uri("https://maven.pkg.github.com/your-user/pekko-persistence-r2dbc")
    credentials {
      username = System.getenv("GITHUB_ACTOR")
      password = System.getenv("GITHUB_TOKEN") // read:packages
    }
  }
}
```

### B3. Publish from GitHub Actions (optional)

```yaml
name: Publish to GitHub Packages
on:
  push:
    tags: ["v*"]
jobs:
  publish:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: "17" }
      - name: Publish
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          # GITHUB_REPOSITORY and GITHUB_ACTOR are provided automatically by Actions
        run: VERSION="1.1.0-batched-journal-SNAPSHOT" ./publish-github.sh
```

---

## Using the journal in your project (runtime)

1. Select the journal plugin in your `application.conf`:

   ```hocon
   # default journal:
   pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.journal"
   # or the batched journal:
   pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.batched-journal"
   ```

2. Configure the database connection-factory (host, database, user, password) - see this repo's
   `core/src/main/resources/reference.conf` and [`docs/src/main/paradox/config.md`](docs/src/main/paradox/config.md).

3. Create the schema in your database:

   ```shell
   psql -U postgres < ddl-scripts/create_tables_postgres.sql
   ```

4. The batched journal additionally requires `use-app-timestamp = on` and `db-timestamp-monotonic-increasing = on`
   (its config block sets these) and supports only the `postgres` and `yugabyte` dialects. Tuning lives under
   `pekko.persistence.r2dbc.batched-journal.batch.*`.

5. To try the adaptive `UNNEST` insert (the `1.1.0-batched-unnest-SNAPSHOT` build), turn it on:

   ```hocon
   pekko.persistence.r2dbc.batched-journal.batch {
     use-unnest = on                    # off by default
     # unnest-max-payload-size = 8 KiB  # events above this fall back to the add() insert (memory-safe)
   }
   ```

   Requires `bytea` payloads (the default `payload-column-type`); with `json`/`jsonb` payloads it is ignored and the
   `add()` batch is used.

---

## Troubleshooting

- **Eviction / `NoSuchMethodError` at runtime** - a Pekko version mismatch. Pin every `pekko-*` dependency to
  `2.0.0-M3` (run `sbt evicted` / `./gradlew dependencies` to check).
- **Wrong Scala suffix** - use `_2.13` or `_3` to match your project's Scala binary version (with sbt's `%%` this is
  automatic).
- **Version changes every build / Maven rejects the version** - you did not set an explicit version; see "Pick a
  version".
- **GitHub Packages 401/403** - the token lacks `write:packages` (publish) or `read:packages` (consume), or the
  `GITHUB_REPOSITORY` owner/repo is wrong.
- **Want a clearly separate coordinate** - change `organization` in `build.sbt` (e.g. to your own group id) before
  publishing so it never collides with the official `org.apache.pekko` artifacts.
