# Publishing guide

[中文](RELEASING.md) | [English](RELEASING.en.md)

This guide defines public publishing conditions and artifact contracts. Published versions are immutable; each upload must correspond to an approved commit, version, and complete archive.

## Prerequisites

- The publishing account's Central Portal namespace covering `io.github.nasa-runtime` is `Verified`. Check the actual Portal account; repository documentation does not establish authorization.
- A Central Portal User Token is configured.
- A local GPG signing key exists and its public key is available on a Central-supported public key server.
- Scope includes only the target component and directly associated content. The POM, README dependency example, and final POM agree on version.
- Commit and push the target branches, obtain successful remote CI, and verify the remote SHA matches the candidate artifact source.
- Obtain explicit maintainer approval for version, branch, and public upload before uploading.
- Local quality code is for local use only and does not enter the public repository or product archives. Product code must not depend on its entry points.

Keep Central credentials in user-level `~/.m2/settings.xml`, never the repository:

```xml
<settings>
    <servers>
        <server>
            <id>central</id>
            <username>${env.CENTRAL_USERNAME}</username>
            <password>${env.CENTRAL_PASSWORD}</password>
        </server>
    </servers>
</settings>
```

Let `gpg-agent` handle the passphrase. Do not put it in `-Dgpg.passphrase=`, shell history, or process arguments. On macOS, an installed `pinentry-mac` can be configured in `~/.gnupg/gpg-agent.conf`:

```text
pinentry-program /opt/homebrew/bin/pinentry-mac
default-cache-ttl 10800
max-cache-ttl 10800
```

Keep `GNUPGHOME` paths short enough for the gpg-agent Unix socket limit, approximately 104 characters on macOS. Excessive depth can produce `can't connect to the gpg-agent: File name too long`.

## Documents and local artifacts

```bash
mvn -B -ntp clean verify
```

The README introduction and independent architecture section explain multiple sources and lane isolation, shared Java/Rust contracts, confirmation points, transactional scope, finite-task recovery, capacity, and observability. POM description, Javadoc, examples, and configuration must agree. A successful build does not replace document review. Chinese and English documents must describe the same defaults, entry points, and failure boundaries.

Expected primary artifacts are:

- `nasa-kafka-1.0.0.jar`
- `nasa-kafka-1.0.0-sources.jar`
- `nasa-kafka-1.0.0-javadoc.jar`

Inspect all three archives directly: file lists, version, licenses, source contents, and rendered type/API documentation. The main and sources JARs include current bilingual README, compatibility, contribution, security, and publishing documents under `META-INF/`; the Javadoc JAR includes them under `doc-files/`. Compare every document and license byte-for-byte with the candidate. Check POM coordinates, dependencies, and description. Exclude local quality content, logs, secrets, and machine metadata. Bytecode targets Java 21.

Verify functional packages and working Javadoc type links. Boot metadata points to `io.github.nasaruntime.kafka.spring.NasaKafkaConfig`; configuration conversion requires `io/github/nasaruntime/kafka/config/flat-source-defaults.json`. Source archives contain actual source and declared public resources without obsolete flat-package classes.

Read both languages' README, compatibility, security, contribution, publishing, type/method Javadoc, POM, examples, and references from related components. Check paths, permissions, confirmation/shutdown semantics, and local document links. Describe current behavior without issue-resolution labels, internal stage numbers, dated batches, assistant attribution, or machine execution records; preserve real API/protocol names and dependency versions.

If an existing public archive omits required content, a local edit does not change it. A separate version and its publication authorization are required.

## Upload and readback

```bash
export CENTRAL_USERNAME='<token username>'
export CENTRAL_PASSWORD='<token password>'
mvn -B -ntp -Pcentral-release clean deploy
```

The `central-release` profile signs the POM, main, sources, and Javadoc artifacts. Central Publishing Maven Plugin generates checksums and uploads a deployment. `autoPublish=false` keeps publication manual: after validation, review coordinates and files in Central Portal before selecting Publish.

After publication, read back registry metadata and downloadable POM/JARs and compare version, README, and licenses with the local candidate. Upload success alone does not prove artifact contents.

## GitHub Release

After Central publication succeeds, use `git tag -s` for the approved version and push the tag with `git push origin`. The tag must point to the verified commit. Base public capabilities and compatibility notes on the current README and [integration contract](CHANGELOG.en.md). Never attach signing keys, Central tokens, or user-level Maven configuration.
