// Copyright 2026 Maravilla Labs
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Maven registry integration tests (API-level + CLI-level).

mod common;

use axum::body::Body;
use common::{TestRegistry, TestServer, has_command};
use tempfile::TempDir;

// Helpers for maven-specific checksums
fn sha1_hex(data: &[u8]) -> String {
    use sha1::Digest;
    hex::encode(sha1::Sha1::digest(data))
}

fn md5_hex(data: &[u8]) -> String {
    use md5::Digest;
    hex::encode(md5::Md5::digest(data))
}

// ---------------------------------------------------------------------------
// Maven API-level tests (via tower oneshot)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_maven_deploy_download() {
    let reg = TestRegistry::new().await;

    let jar_data = b"fake-jar-content-for-testing-maven-deploy";
    let pom_data = br#"<project><modelVersion>4.0.0</modelVersion></project>"#;

    // --- PUT JAR artifact ---------------------------------------------------
    let req = reg
        .request("PUT", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::from(jar_data.to_vec()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        201,
        "PUT JAR should return 201: {}",
        String::from_utf8_lossy(&body)
    );

    // --- PUT POM artifact ---------------------------------------------------
    let req = reg
        .request("PUT", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.pom")
        .body(Body::from(pom_data.to_vec()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        201,
        "PUT POM should return 201: {}",
        String::from_utf8_lossy(&body)
    );

    // --- GET JAR back -------------------------------------------------------
    let req = reg
        .request("GET", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = reg.send(req).await;
    assert_eq!(status, 200, "GET JAR should return 200");
    assert_eq!(body, jar_data, "JAR bytes must match");

    // Verify response headers
    assert_eq!(
        headers.get("content-type").unwrap().to_str().unwrap(),
        "application/java-archive"
    );
    assert_eq!(
        headers.get("x-checksum-sha1").unwrap().to_str().unwrap(),
        sha1_hex(jar_data)
    );
    assert_eq!(
        headers.get("x-checksum-md5").unwrap().to_str().unwrap(),
        md5_hex(jar_data)
    );
    assert!(headers.get("etag").is_some(), "ETag should be present");
    assert_eq!(
        headers
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        jar_data.len()
    );

    // --- GET auto-generated checksum sidecar --------------------------------
    let req = reg
        .request(
            "GET",
            "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar.sha1",
        )
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "GET .sha1 should return 200");
    let checksum_str = String::from_utf8(body).unwrap();
    assert_eq!(checksum_str.trim(), sha1_hex(jar_data));

    // --- HEAD JAR -----------------------------------------------------------
    let req = reg
        .request("HEAD", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = reg.send(req).await;
    assert_eq!(status, 200, "HEAD JAR should return 200");
    assert!(body.is_empty(), "HEAD should have no body");
    assert_eq!(
        headers
            .get("content-length")
            .unwrap()
            .to_str()
            .unwrap()
            .parse::<usize>()
            .unwrap(),
        jar_data.len()
    );

    // --- GET artifact-level metadata ----------------------------------------
    let req = reg
        .request("GET", "/-/maven/com/example/mylib/maven-metadata.xml")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200, "GET metadata should return 200");
    let xml = String::from_utf8(body).unwrap();
    assert!(
        xml.contains("<versions>"),
        "metadata should contain <versions>"
    );
    assert!(xml.contains("1.0.0"), "metadata should list version 1.0.0");
    assert!(
        xml.contains("<release>"),
        "metadata should contain <release>"
    );

    // --- GET non-existent artifact ------------------------------------------
    let req = reg
        .request(
            "GET",
            "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0-sources.jar",
        )
        .body(Body::empty())
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 404, "non-existent artifact should return 404");
}

#[tokio::test]
async fn test_maven_release_immutability() {
    let reg = TestRegistry::new().await;

    let jar_data = b"original-jar-content";

    // --- First PUT ---
    let req = reg
        .request("PUT", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::from(jar_data.to_vec()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 201, "first PUT should return 201");

    // --- Second PUT (same path) → 409 ---
    let req = reg
        .request("PUT", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::from(b"different-content".to_vec()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        409,
        "re-deploy of release artifact should return 409: {}",
        String::from_utf8_lossy(&body)
    );

    // --- Verify original content untouched ---
    let req = reg
        .request("GET", "/-/maven/com/example/mylib/1.0.0/mylib-1.0.0.jar")
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    assert_eq!(body, jar_data, "original content should be untouched");
}

#[tokio::test]
async fn test_maven_snapshot_redeploy() {
    let reg = TestRegistry::new().await;

    let snap1 = b"snapshot-build-1-content";
    let snap2 = b"snapshot-build-2-content";

    // --- PUT first snapshot build ---
    let req = reg
        .request(
            "PUT",
            "/-/maven/com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-20240101.120000-1.jar",
        )
        .body(Body::from(snap1.to_vec()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        201,
        "first snapshot PUT should return 201: {}",
        String::from_utf8_lossy(&body)
    );

    // --- PUT second snapshot build ---
    let req = reg
        .request(
            "PUT",
            "/-/maven/com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-20240201.120000-2.jar",
        )
        .body(Body::from(snap2.to_vec()))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        201,
        "second snapshot PUT should return 201: {}",
        String::from_utf8_lossy(&body)
    );

    // --- GET both back → distinct content ---
    let req = reg
        .request(
            "GET",
            "/-/maven/com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-20240101.120000-1.jar",
        )
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    assert_eq!(body, snap1);

    let req = reg
        .request(
            "GET",
            "/-/maven/com/example/mylib/1.0.0-SNAPSHOT/mylib-1.0.0-20240201.120000-2.jar",
        )
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    assert_eq!(body, snap2);
}

#[tokio::test]
async fn test_maven_checksum_mismatch() {
    let reg = TestRegistry::new().await;

    let jar_data = b"jar-for-checksum-test";

    // --- PUT JAR ---
    let req = reg
        .request("PUT", "/-/maven/com/example/mylib/2.0.0/mylib-2.0.0.jar")
        .body(Body::from(jar_data.to_vec()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 201);

    // --- PUT .sha1 with WRONG hash → 409 (checksum mismatch) ---
    let req = reg
        .request(
            "PUT",
            "/-/maven/com/example/mylib/2.0.0/mylib-2.0.0.jar.sha1",
        )
        .body(Body::from("0000000000000000000000000000000000000000"))
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(
        status,
        409,
        "wrong checksum should return 409: {}",
        String::from_utf8_lossy(&body)
    );

    // --- PUT .sha1 with CORRECT hash → 409 (release sidecar already auto-generated) ---
    // The server auto-generates .sha1 sidecars on artifact upload, so re-uploading
    // a checksum for a release artifact is blocked by immutability.
    let correct_sha1 = sha1_hex(jar_data);
    let req = reg
        .request(
            "PUT",
            "/-/maven/com/example/mylib/2.0.0/mylib-2.0.0.jar.sha1",
        )
        .body(Body::from(correct_sha1))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(
        status, 409,
        "re-uploading release checksum should return 409 (immutability)"
    );

    // --- Verify auto-generated sidecar has the correct hash ---
    let req = reg
        .request(
            "GET",
            "/-/maven/com/example/mylib/2.0.0/mylib-2.0.0.jar.sha1",
        )
        .body(Body::empty())
        .unwrap();
    let (status, _, body) = reg.send(req).await;
    assert_eq!(status, 200);
    let sidecar = String::from_utf8(body).unwrap();
    assert_eq!(sidecar.trim(), sha1_hex(jar_data));
}

#[tokio::test]
async fn test_maven_path_traversal() {
    let reg = TestRegistry::new().await;

    // --- PUT with .. in path ---
    let req = reg
        .request("PUT", "/-/maven/com/../etc/passwd")
        .body(Body::from("evil"))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 400, "path traversal with .. should return 400");

    // --- PUT with double slash ---
    let req = reg
        .request("PUT", "/-/maven/com/foo//bar/1.0/x.jar")
        .body(Body::from("evil"))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 400, "path with double slash should return 400");

    // --- GET with percent-encoded traversal ---
    let req = reg
        .request("GET", "/-/maven/com/%2e%2e/etc/passwd")
        .body(Body::empty())
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 400, "percent-encoded traversal should return 400");
}

#[tokio::test]
async fn test_maven_metadata_fallback() {
    let reg = TestRegistry::new().await;

    let jar_data = b"jar-for-metadata-fallback-test";

    // --- PUT a JAR (which auto-generates artifact-level metadata) ---
    let req = reg
        .request(
            "PUT",
            "/-/maven/com/example/fallback/1.0.0/fallback-1.0.0.jar",
        )
        .body(Body::from(jar_data.to_vec()))
        .unwrap();
    let (status, _, _) = reg.send(req).await;
    assert_eq!(status, 201);

    // --- GET artifact-level metadata → should be valid XML ---
    let req = reg
        .request("GET", "/-/maven/com/example/fallback/maven-metadata.xml")
        .body(Body::empty())
        .unwrap();
    let (status, headers, body) = reg.send(req).await;
    assert_eq!(status, 200, "metadata fallback should return 200");
    assert_eq!(
        headers.get("content-type").unwrap().to_str().unwrap(),
        "application/xml"
    );
    let xml = String::from_utf8(body).unwrap();
    assert!(xml.contains("1.0.0"), "metadata should list version 1.0.0");
    assert!(
        xml.contains("<groupId>com.example</groupId>") || xml.contains("com.example"),
        "metadata should reference groupId"
    );
    assert!(
        xml.contains("<artifactId>fallback</artifactId>") || xml.contains("fallback"),
        "metadata should reference artifactId"
    );
}

// ---------------------------------------------------------------------------
// Maven CLI integration test
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_maven_cli_deploy_resolve() {
    if !has_command("mvn") {
        eprintln!("SKIP: mvn not found");
        return;
    }
    if !has_command("java") {
        eprintln!("SKIP: java not found");
        return;
    }

    let server = TestServer::start().await;
    let base_url = server.base_url();
    let port = server.addr.port();

    // --- Create publisher project ---
    let pub_dir = TempDir::new().expect("pub_dir");

    // pom.xml for the library to deploy
    std::fs::write(
        pub_dir.path().join("pom.xml"),
        r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example.test</groupId>
    <artifactId>maven-cli-test-lib</artifactId>
    <version>1.0.0</version>
    <packaging>jar</packaging>
    <name>Maven CLI Test Library</name>
    <description>Integration test artifact</description>
    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>
</project>
"#,
    )
    .unwrap();

    // Minimal Java source
    let src_dir = pub_dir.path().join("src/main/java/com/example/test");
    std::fs::create_dir_all(&src_dir).unwrap();
    std::fs::write(
        src_dir.join("App.java"),
        r#"package com.example.test;
public class App {
    public static String greet() { return "hello from maven-cli-test-lib"; }
}
"#,
    )
    .unwrap();

    // settings.xml with server credentials (HTTP Basic auth)
    let settings_xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<settings>
    <servers>
        <server>
            <id>muli</id>
            <username>token</username>
            <password>{token}</password>
        </server>
    </servers>
</settings>
"#,
        token = server.plaintext_token,
    );
    std::fs::write(pub_dir.path().join("settings.xml"), &settings_xml).unwrap();

    // --- Deploy ---
    let dir = pub_dir.path().to_path_buf();
    let settings = pub_dir.path().join("settings.xml");
    let deploy_repo = format!("muli::default::http://127.0.0.1:{port}/-/maven/");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("mvn")
            .args([
                "deploy",
                "-B",
                "-s",
                settings.to_str().unwrap(),
                &format!("-DaltDeploymentRepository={deploy_repo}"),
                "-DskipTests",
            ])
            .current_dir(&dir)
            .output()
            .expect("mvn deploy")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "mvn deploy failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Verify artifacts exist via HTTP GET ---
    let client = reqwest::Client::new();

    // Check JAR
    let jar_url = format!(
        "{base_url}/-/maven/com/example/test/maven-cli-test-lib/1.0.0/maven-cli-test-lib-1.0.0.jar"
    );
    let resp = client
        .get(&jar_url)
        .basic_auth("token", Some(&server.plaintext_token))
        .send()
        .await
        .expect("GET JAR");
    assert_eq!(resp.status(), 200, "deployed JAR should exist");

    // Check POM
    let pom_url = format!(
        "{base_url}/-/maven/com/example/test/maven-cli-test-lib/1.0.0/maven-cli-test-lib-1.0.0.pom"
    );
    let resp = client
        .get(&pom_url)
        .basic_auth("token", Some(&server.plaintext_token))
        .send()
        .await
        .expect("GET POM");
    assert_eq!(resp.status(), 200, "deployed POM should exist");

    // Check metadata
    let meta_url =
        format!("{base_url}/-/maven/com/example/test/maven-cli-test-lib/maven-metadata.xml");
    let resp = client
        .get(&meta_url)
        .basic_auth("token", Some(&server.plaintext_token))
        .send()
        .await
        .expect("GET metadata");
    assert_eq!(resp.status(), 200, "metadata should exist");
    let meta_body = resp.text().await.unwrap();
    assert!(meta_body.contains("1.0.0"), "metadata should list 1.0.0");

    // --- Consumer project: resolve dependency ---
    let consumer_dir = TempDir::new().expect("consumer_dir");

    std::fs::write(
        consumer_dir.path().join("pom.xml"),
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.example.test</groupId>
    <artifactId>maven-cli-test-consumer</artifactId>
    <version>1.0.0</version>
    <dependencies>
        <dependency>
            <groupId>com.example.test</groupId>
            <artifactId>maven-cli-test-lib</artifactId>
            <version>1.0.0</version>
        </dependency>
    </dependencies>
    <repositories>
        <repository>
            <id>muli</id>
            <url>http://127.0.0.1:{port}/-/maven/</url>
        </repository>
    </repositories>
    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>
</project>
"#
        ),
    )
    .unwrap();

    std::fs::write(consumer_dir.path().join("settings.xml"), &settings_xml).unwrap();

    // Add Java source that actually imports and uses the deployed library.
    // This ensures `mvn compile` downloads the JAR, verifies it, and compiles against it.
    let consumer_src = consumer_dir.path().join("src/main/java/com/example/test");
    std::fs::create_dir_all(&consumer_src).unwrap();
    std::fs::write(
        consumer_src.join("Consumer.java"),
        r#"package com.example.test;
// Import the class from the deployed library
import com.example.test.App;
public class Consumer {
    public static void main(String[] args) {
        // Actually call the library method — proves the JAR is valid
        String greeting = App.greet();
        System.out.println(greeting);
    }
}
"#,
    )
    .unwrap();

    let dir = consumer_dir.path().to_path_buf();
    let settings = consumer_dir.path().join("settings.xml");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("mvn")
            .args(["compile", "-B", "-s", settings.to_str().unwrap()])
            .current_dir(&dir)
            .output()
            .expect("mvn compile")
    })
    .await
    .unwrap();
    assert!(
        output.status.success(),
        "mvn compile (consumer with dependency) failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    // --- Re-deploy same release version → should fail (immutability) ---
    let dir = pub_dir.path().to_path_buf();
    let settings = pub_dir.path().join("settings.xml");
    let deploy_repo = format!("muli::default::http://127.0.0.1:{port}/-/maven/");
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("mvn")
            .args([
                "deploy",
                "-B",
                "-s",
                settings.to_str().unwrap(),
                &format!("-DaltDeploymentRepository={deploy_repo}"),
                "-DskipTests",
            ])
            .current_dir(&dir)
            .output()
            .expect("mvn deploy (re-deploy)")
    })
    .await
    .unwrap();
    assert!(
        !output.status.success(),
        "re-deploy of same release version should fail (immutability)"
    );
}
