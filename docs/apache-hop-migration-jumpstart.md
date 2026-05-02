# Apache Hop Support in PETL — Jump-Start Plan

*Status: paused, to be picked up later. Last updated 2026-05-02.*

This document is the launching point when we resume the work to add Apache Hop
support to PETL. Read `apache-hop-overview.md` first for background and the
case for doing this at all.

## TL;DR

Add an Apache Hop job runner to PETL alongside the existing Pentaho Kettle
runner. Keep both runners working in parallel during the transition so deployed
sites can migrate `.ktr`/`.kjb` files to `.hpl`/`.hwf` on their own schedule.
Eventually retire the Pentaho path once all sites have moved.

## Why we're doing this (one-paragraph recap)

Pentaho Kettle 9.1.0.0-324 artifacts are no longer obtainable from any
anonymous public Maven repository. The "public" Hitachi JFrog repo at
`hitachiedge1.jfrog.io/artifactory/pntpub-maven/` returns HTTP 401 without
a Hitachi customer login, and Maven Central never carried these artifacts.
The current workaround in `pom.xml` is a local `file://` repository at
`/tmp/petl-local-maven-repo/.m2/repository`, which is fragile and not portable
across build environments. Apache Hop is on Maven Central under Apache 2.0,
ASF-governed, and the actively maintained successor to the Kettle codebase.

## Current Pentaho usage in PETL (the surface area to migrate)

- **Maven dependencies** (in `pom.xml`):
  - `pentaho-kettle:kettle-core:9.1.0.0-324`
  - `pentaho-kettle:kettle-engine:9.1.0.0-324`
  - `org.pentaho.di.plugins:pdi-core-plugins-impl:9.1.0.0-324`
  - `rhino:js:1.7R3` (used by JavaScript steps inside Kettle)
  - `javax.mail:mail:1.4.7`
  - Custom `<repository>` pointing at `file:///tmp/petl-local-maven-repo/.m2/repository`

- **Java code**:
  - `src/main/java/org/pih/petl/job/PentahoJob.java` — the only file that
    imports `org.pentaho.di.*`. Touchpoints:
    - `KettleEnvironment.init()` / `KettleEnvironment.reset()`
    - `TransMeta` + `Trans` (for `.ktr`)
    - `JobMeta` + `Job` (for `.kjb`)
    - `Result`, `LogLevel`, `KettleException`, `NamedParams`, `Repository`
    - System properties: `KETTLE_HOME`, `KETTLE_PLUGIN_CLASSES`
    - Files written: `kettle.properties`, `pih-kettle.properties`
    - Plugin pre-registered: `org.pentaho.di.trans.steps.append.AppendMeta`
  - `src/test/java/org/pih/petl/job/PentahoJobTest.java` — covers the runner.

- **Pentaho job/transform fixtures in this repo** (test only):
  - `src/test/resources/configuration/jobs/pentaho/job.kjb`
  - `src/test/resources/configuration/jobs/pentaho/failing-job.kjb`
  - `src/test/resources/configuration/jobs/pentaho/transform.ktr` + `transform.yml`

- **Job-config wiring**:
  - YAML configs declare `type: "pentaho-job"` (see `transform.yml` etc.).
    Hop equivalent would introduce a sibling `type: "hop-pipeline"` or
    `type: "hop-workflow"` (or a unified `type: "hop-job"`).

- **Production `.ktr` / `.kjb` files** live **outside this repo** in
  per-deployment site config. Migration of those is each site's responsibility,
  not part of the code change here. PETL just needs to keep executing them.

## Apache Hop equivalent API map

| Kettle (current) | Apache Hop (target) |
|---|---|
| `org.pentaho.di.*` packages | `org.apache.hop.*` |
| `KettleEnvironment.init()` | `HopEnvironment.init()` |
| `KettleEnvironment.reset()` | (not directly equivalent; engine cleanup is per-instance) |
| `TransMeta` + `Trans` | `PipelineMeta` + `IPipelineEngine` (typically `LocalPipelineEngine`) |
| `JobMeta` + `Job` | `WorkflowMeta` + `IWorkflowEngine` (typically `LocalWorkflowEngine`) |
| `NamedParams` | `INamedParameters` |
| `Result`, `LogLevel` | `Result`, `LogLevel` (under `org.apache.hop.core.*`) |
| `KettleException` | `HopException` |
| `KETTLE_HOME` + `kettle.properties` + `pih-kettle.properties` | `hop-config.json` + project + environment + variables file |
| `KETTLE_PLUGIN_CLASSES` env var | Plugin discovery via classpath + `@Transform` / `@Action` annotations |
| `.ktr`, `.kjb` | `.hpl`, `.hwf` |

API names should be verified against the actual Hop release in use when work
resumes (2.17 line as of this writing); some interface names have been refined
over the 2.x series.

## Suggested Maven dependencies for Hop

Pin to the latest stable Hop release at the time we start. Starting set:

```xml
<dependency>
  <groupId>org.apache.hop</groupId>
  <artifactId>hop-engine</artifactId>
  <version>${hop.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.hop</groupId>
  <artifactId>hop-core</artifactId>
  <version>${hop.version}</version>
</dependency>
```

Per-transform / per-action plugins are separate artifacts under
`org.apache.hop:hop-transform-*` and `org.apache.hop:hop-action-*`. We will need
to pull in the specific ones used by the deployed pipelines (database lookup,
table input/output, etc.). Maven Central has 332 Hop artifacts — survey before
locking the list.

Hop requires Java 11+ as of the 2.x line. PETL is currently on Java 8
(`java.version = 1.8` in `pom.xml`). Resolving this is part of the work — see
Open Questions.

## Migration approach

**Recommended: parallel runners.** Add a `HopJob` component alongside the
existing `PentahoJob`, both implementing `PetlJob`. Add a `type: "hop-job"`
(name TBD) job-config option. Keep `type: "pentaho-job"` working unchanged.

This avoids a flag-day cutover for deployed sites. Each site converts its
`.ktr`/`.kjb` files when ready and flips its YAML config. The Pentaho path
gets removed after the last site has migrated.

Alternative — full replacement — is simpler in the codebase but forces a
coordinated migration across all deployments, which is operationally riskier.

## Step-by-step plan

1. **Decide Java version policy.** Hop 2.x requires Java 11. PETL is on Java 8.
   Either bump PETL to Java 11+ (probably needed anyway) or pin Hop to the
   newest 2.x release that still supports Java 8 (verify availability — this
   may not exist, in which case the bump is forced).

2. **Add Hop dependencies** to `pom.xml`. Survey deployed `.ktr` files to
   identify which `hop-transform-*` / `hop-action-*` artifacts are needed.

3. **Run `hop-import` against the test fixtures** in
   `src/test/resources/configuration/jobs/pentaho/` to generate `.hpl` / `.hwf`
   equivalents. Inspect output for issues. Place results under a parallel
   `src/test/resources/configuration/jobs/hop/` tree.

4. **Implement `HopJob`** in `src/main/java/org/pih/petl/job/HopJob.java`,
   modelled on `PentahoJob` but using the Hop API. Key responsibilities:
   - Bootstrap `HopEnvironment.init()` once per process (cache state).
   - Resolve project / environment from job config rather than writing
     `kettle.properties` files.
   - Load `.hpl` via `PipelineMeta` and execute via `LocalPipelineEngine`
     (or the configured engine).
   - Load `.hwf` via `WorkflowMeta` and execute via `LocalWorkflowEngine`.
   - Translate parameters, log level, error handling to match `PentahoJob`'s
     contract.

5. **Register `HopJob`** as a Spring `@Component` keyed on a new job type
   string in YAML configs. Decide naming: `hop-job` (mirrors `pentaho-job`),
   or split into `hop-pipeline` / `hop-workflow`. Recommend `hop-job` for
   symmetry.

6. **Write `HopJobTest`** that mirrors `PentahoJobTest`, exercising the
   converted fixtures.

7. **Update documentation**: `README.md` to describe both runners,
   `docs/examples/` to include a Hop example, deprecation notice for
   `pentaho-job`.

8. **Verify on a real deployment** with one site's converted pipelines
   before announcing general availability.

9. **Plan retirement of `PentahoJob`** once adoption is complete. The
   `file://` Maven repository hack and local-jar workaround go away with it.

## Open questions to settle before coding

- **Java version**: bump to 11 (or 17) as part of this work, or stay on 8?
  Hop 2.x will likely force the answer.
- **Engine selection**: local-only initially, or expose engine choice
  (Beam/Spark/Flink) in YAML config from the start?
- **Configuration model**: use Hop's project + environment files, or
  synthesise them from PETL's existing per-job YAML at runtime? The second
  is less invasive for users; the first is more idiomatic in Hop.
- **Plugin discovery**: which `hop-transform-*` artifacts must be in the
  uber-jar by default? Survey deployed `.ktr` files at sites to answer.
- **Fixture migration**: convert `transform.ktr` / `job.kjb` /
  `failing-job.kjb` once and commit, or convert on the fly in tests? Commit.
- **Deprecation timeline**: how long do we run both runners before removing
  `PentahoJob`? Tied to deployment migration pace.

## Risks and gotchas

- **API drift**: Hop 2.x interface names have evolved. Verify class names
  against the chosen release rather than this document.
- **JavaScript steps**: imported pipelines that use the JavaScript transform
  may need manual fixes; the Rhino dependency that Kettle pulls in changes
  in Hop.
- **Custom Pentaho plugins**: anything beyond stock steps will need a Hop
  equivalent or a port.
- **Metastore objects** are not imported by `hop-import` — DB connections,
  named clusters, etc. defined in a Pentaho metastore must be recreated as
  Hop project metadata.
- **Spring Boot 2.2.5 + Java 11**: should work, but the Spring Boot version
  is old (2020-era). A version bump may be sensible to do alongside.
- **Logging**: PETL relies on `LogLevel.MINIMAL` semantics from Kettle.
  Verify Hop's `LogLevel` enum maps cleanly.

## Useful commands

Convert Kettle files to Hop with the CLI tool:

```bash
hop-import.sh \
  --type=kettle \
  --input=/path/to/kettle/project \
  --output=/path/to/hop/project \
  --kettle-shared-xml=/path/to/shared.xml \
  --kettle-kettle-properties=/path/to/kettle.properties
```

Probe Maven Central for the current Hop release set:

```bash
curl -s 'https://search.maven.org/solrsearch/select?q=g:org.apache.hop&rows=100&wt=json' \
  | jq '.response.docs[] | "\(.g):\(.a):\(.latestVersion)"'
```

## References

- Companion document: [apache-hop-overview.md](apache-hop-overview.md)
- Hop user manual: https://hop.apache.org/manual/latest/
- Hop developer manual: https://hop.apache.org/dev-manual/latest/
- `hop-import` docs: https://hop.apache.org/tech-manual/latest/hop-vs-kettle/hop-import.html
- Hop on Maven Central: https://search.maven.org/search?q=g:org.apache.hop
