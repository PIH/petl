# Apache Hop and Why PETL Should Support It

*Informational. Last updated 2026-05-02.*

## Background: Kettle, PDI, and Hop

The Pentaho Data Integration ("PDI" / "Kettle") engine was created by Matt Casters
in the early 2000s, open-sourced in 2005, and acquired by Pentaho in 2006. Pentaho
itself was acquired by Hitachi in 2015 and folded into Hitachi Vantara's
"Pentaho Plus Platform" alongside their commercial offerings.

**Apache Hop** ("Hop Orchestration Platform") began in late 2019 as a community
fork of the Kettle codebase, again led by Matt Casters and a group of long-time
Kettle contributors. Hop entered the Apache Incubator in 2021 and graduated to
a top-level Apache Software Foundation project. As of 2026-05-02 the project is
on its **2.x line** (latest 2.17.0-rc1, released 2026-02-02), with releases on
roughly a two-month cadence.

The two projects now have separate roadmaps and are no longer source- or
binary-compatible. Hop's documentation states explicitly that
*"Compatibility with Kettle/PDI was never a goal for Apache Hop."* A one-shot
import tool (`hop-import`) is provided to help projects migrate `.ktr`/`.kjb`
files into Hop's `.hpl`/`.hwf` format, but the Java embedding API, configuration
model, and plugin system have all been redesigned.

## Why this matters for PETL

PETL currently embeds Pentaho Kettle 9.1.0.0-324 to execute `.ktr` and `.kjb`
files supplied by deployed sites. This binds PETL to artifacts that are
increasingly hard to obtain.

### 1. The Pentaho public Maven repository is effectively closed

As of 2026, none of the Kettle 9.1.0.0-324 artifacts PETL needs
(`pentaho-kettle:kettle-core`, `pentaho-kettle:kettle-engine`,
`org.pentaho.di.plugins:pdi-core-plugins-impl`) are available on Maven Central.
Hitachi has consolidated the old Pentaho Nexus into a JFrog Artifactory
instance at `hitachiedge1.jfrog.io`. The repository named "pntpub-maven" still
exists on that instance, but anonymous requests now return HTTP 401, and the
older `nexus.pentaho.org` and `public.nexus.pentaho.org` URLs all redirect to
a generic landing page rather than serving artifacts. The current PETL workaround
is a `file://` repository pointing at a local extracted copy of the JARs — see
the `<repository>` block in `pom.xml`.

### 2. Pentaho Data Integration Community Edition is no longer maintained

Hitachi has wound down the open-source PDI CE releases. The community successor
is Apache Hop. Continuing to depend on Kettle 9.1 means depending on a frozen
artifact set with no security updates and no public distribution channel.

### 3. Apache Hop solves the supply problem

- All `org.apache.hop:*` artifacts are published to **Maven Central** under
  Apache License 2.0, with **no auth required**. PETL builds would no longer
  need a custom repository at all.
- Governance is the **Apache Software Foundation**, not a single vendor.
  No future "we're moving this behind a paywall" surprise.
- The project is **active**: 1,370+ stars, ~450 forks, regular releases,
  recent commits as of late April 2026.

### 4. Modern features that align with PETL's direction

Hop introduces several capabilities PETL would benefit from down the line:

- **Pluggable runtime engines** via Apache Beam, including local execution,
  Spark, Flink, and Google Cloud DataFlow. PETL today uses only the local
  engine, but distributed execution becomes an option without rewriting jobs.
- **Project + environment model**: pipelines, workflows, variables, and
  database connections are organised as projects with environment overlays,
  rather than a flat `${KETTLE_HOME}/.kettle/*.properties` directory. This
  maps more naturally onto PETL's per-deployment configuration.
- **Single config file**: `hop-config.json` replaces the spread of
  `kettle.properties`, `repositories.xml`, `shared.xml`, etc.
- **Annotation-driven plugins** rather than XUL XML, so adding a custom
  transform is a normal Java module rather than a packaging exercise.
- **Built-in unit testing** for pipelines.
- **Cleaner license posture**: the Apache 2.0 license applies cleanly to all
  components, including pieces of the old `pentaho-metastore` that had APL
  compliance gaps in Kettle.

## Compatibility summary

| Aspect | Status |
|---|---|
| Java API (`org.pentaho.di.*` → `org.apache.hop.*`) | Not compatible. Bulk rename plus engine-instantiation differences. |
| `.ktr` / `.kjb` files | Not natively executable by Hop. One-shot import to `.hpl` / `.hwf` via `hop-import`. |
| Database connection definitions | Imported from `shared.xml`, `jdbc.properties`, embedded refs. Same-named conflicts deduplicate. |
| Variables / `kettle.properties` | Imported. |
| JavaScript steps, custom plugins, edge-case step config | May need manual touch-up after import. |
| Metastore | Not imported; rebuild from project setup. |
| Runtime model | Different. Hop uses a pluggable `IPipelineEngine` / `IWorkflowEngine` factory rather than `new Trans(meta).execute(...)`. |

## What this means for PETL

Apache Hop is a credible long-term replacement for the embedded Kettle engine
in PETL. Adopting it solves the immediate supply-chain pain (no more
private-repo dance), aligns PETL with an actively maintained open-source
project, and unlocks future capabilities like distributed execution.

The migration is real work, not a rename — see
`apache-hop-migration-jumpstart.md` for the implementation plan.

## References

- Apache Hop project: https://hop.apache.org/
- Apache Hop on GitHub: https://github.com/apache/hop
- Hop vs Kettle (official): https://hop.apache.org/tech-manual/latest/hop-vs-kettle/index.html
- If You Know Kettle: https://hop.apache.org/tech-manual/latest/hop-vs-kettle/if-you-know-kettle.html
- Importing Kettle Projects: https://hop.apache.org/tech-manual/latest/hop-vs-kettle/import-kettle-projects.html
- `hop-import` CLI: https://hop.apache.org/tech-manual/latest/hop-vs-kettle/hop-import.html
- Apache Hop reaches open-source milestone (TechTarget, 2021): https://www.techtarget.com/searchdatamanagement/news/252512339/Apache-Hop-data-orchestration-hits-open-source-milestone
