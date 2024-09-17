use assert_cmd::prelude::*;
use predicates::str::{contains, is_empty};
use rand::Rng;
use std::fs::{self, File};
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// `kvs-client` with no args should exit with a non-zero code.
#[test]
fn client_cli_no_args() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs-client").unwrap();
    cmd.current_dir(&temp_dir).assert().failure();
}

#[test]
fn client_cli_invalid_get() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get", "key", "--addr", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get", "key", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_set() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["set"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["set", "missing_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["set", "key", "value", "extra_field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["set", "key", "value", "--addr", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["get", "key", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_rm() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["rm"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["rm", "extra", "field"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["rm", "key", "--addr", "invalid-addr"])
        .current_dir(&temp_dir)
        .assert()
        .failure();

    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["rm", "key", "--unknown-flag"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

#[test]
fn client_cli_invalid_subcommand() {
    let temp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs-client")
        .unwrap()
        .args(&["unknown"])
        .current_dir(&temp_dir)
        .assert()
        .failure();
}

// `kvs-client -V` should print the version
#[test]
fn client_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs-client").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

// `kvs-server -V` should print the version
#[test]
fn server_cli_version() {
    let temp_dir = TempDir::new().unwrap();
    let mut cmd = Command::cargo_bin("kvs-server").unwrap();
    cmd.args(&["-V"])
        .current_dir(&temp_dir)
        .assert()
        .stdout(contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn cli_log_configuration() {
    let temp_dir = TempDir::new().unwrap();
    let stderr_path = temp_dir.path().join("stderr");
    let mut cmd = Command::cargo_bin("kvs-server").unwrap();
    let mut child = cmd
        .args(&["--engine", "kvs", "--addr", "127.0.0.1:4001"])
        .current_dir(&temp_dir)
        .stderr(File::create(&stderr_path).unwrap())
        .spawn()
        .unwrap();
    thread::sleep(Duration::from_secs(1));
    child.kill().expect("server exited before killed");

    let content = fs::read_to_string(&stderr_path).expect("unable to read from stderr file");
    assert!(content.contains(env!("CARGO_PKG_VERSION")));
    assert!(content.contains("kvs"));
    assert!(content.contains("127.0.0.1:4001"));
}

#[test]
fn cli_wrong_engine() {
    // sled first, kvs second
    {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = Command::cargo_bin("kvs-server").unwrap();
        let mut child = cmd
            .args(&["--engine", "sled", "--addr", "127.0.0.1:4002"])
            .current_dir(&temp_dir)
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        child.kill().expect("server exited before killed");

        let mut cmd = Command::cargo_bin("kvs-server").unwrap();
        cmd.args(&["--engine", "kvs", "--addr", "127.0.0.1:4003"])
            .current_dir(&temp_dir)
            .assert()
            .failure();
    }

    // kvs first, sled second
    {
        let temp_dir = TempDir::new().unwrap();
        let mut cmd = Command::cargo_bin("kvs-server").unwrap();
        let mut child = cmd
            .args(&["--engine", "kvs", "--addr", "127.0.0.1:4002"])
            .current_dir(&temp_dir)
            .spawn()
            .unwrap();
        thread::sleep(Duration::from_secs(1));
        child.kill().expect("server exited before killed");

        let mut cmd = Command::cargo_bin("kvs-server").unwrap();
        cmd.args(&["--engine", "sled", "--addr", "127.0.0.1:4003"])
            .current_dir(&temp_dir)
            .assert()
            .failure();
    }
}

fn cli_access_server(engine: &str, addr: &str) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("kvs-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv();
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    let max_retries = 5;
    let initial_delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(5);

    // Helper function to run a command and return its output
    let run_command = |args: &[&str]| -> Result<(bool, String, String), String> {
        Command::cargo_bin("kvs-client")
            .unwrap()
            .args(args)
            .current_dir(&temp_dir)
            .output()
            .map(|output| {
                (
                    output.status.success(),
                    String::from_utf8_lossy(&output.stdout).to_string(),
                    String::from_utf8_lossy(&output.stderr).to_string(),
                )
            })
            .map_err(|e| format!("Failed to execute command: {}", e))
    };

    // Set key1 to value1
    retry_with_backoff(
        || {
            let (success, stdout, stderr) =
                run_command(&["set", "key1", "value1", "--addr", addr])?;
            if success && stdout.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "Failed to set key1. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to set key1 after multiple retries");

    // Get key1
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["get", "key1", "--addr", addr])?;
            if success && stdout.trim() == "value1" {
                Ok(())
            } else {
                Err(format!(
                    "Failed to get key1. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get key1 after multiple retries");

    // Set key1 to value2
    retry_with_backoff(
        || {
            let (success, stdout, stderr) =
                run_command(&["set", "key1", "value2", "--addr", addr])?;
            if success && stdout.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "Failed to set key1 to value2. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to set key1 to value2 after multiple retries");

    // Get key1 again
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["get", "key1", "--addr", addr])?;
            if success && stdout.trim() == "value2" {
                Ok(())
            } else {
                Err(format!(
                    "Failed to get updated key1. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get updated key1 after multiple retries");

    // Get a non-existent key
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["get", "key2", "--addr", addr])?;
            if success && stdout.contains("Key not found") {
                Ok(())
            } else {
                Err(format!(
                    "Unexpected result for non-existent key. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get expected result for non-existent key after multiple retries");

    // Remove non-existent key
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["rm", "key2", "--addr", addr])?;
            if !success && stderr.contains("Key not found") {
                Ok(())
            } else {
                Err(format!("Unexpected result when removing non-existent key. Success: {}, Stdout: {}, Stderr: {}", success, stdout, stderr))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get expected result when removing non-existent key after multiple retries");

    // Set key2 to value3
    retry_with_backoff(
        || {
            let (success, stdout, stderr) =
                run_command(&["set", "key2", "value3", "--addr", addr])?;
            if success && stdout.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "Failed to set key2. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to set key2 after multiple retries");

    // Remove key1
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["rm", "key1", "--addr", addr])?;
            if success && stdout.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "Failed to remove key1. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to remove key1 after multiple retries");

    // Shutdown the server
    sender.send(()).unwrap();
    handle.join().unwrap();

    // Restart the server
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("kvs-server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv();
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    // Get key2 again
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["get", "key2", "--addr", addr])?;
            if success && stdout.trim() == "value3" {
                Ok(())
            } else {
                Err(format!(
                    "Failed to get key2 after restart. Stdout: {}, Stderr: {}",
                    stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get key2 after restart and multiple retries");

    // Get removed key1
    retry_with_backoff(
        || {
            let (success, stdout, stderr) = run_command(&["get", "key1", "--addr", addr])?;
            if success && stdout.trim().contains("Key not found") {
                Ok(())
            } else {
                Err(format!(
                    "Unexpected result for removed key1. Success: {}, Stdout: {}, Stderr: {}",
                    success, stdout, stderr
                ))
            }
        },
        max_retries,
        initial_delay,
        max_delay,
    )
    .expect("Failed to get expected result for removed key1 after multiple retries");

    // Shutdown the server
    sender.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn cli_access_server_kvs_engine() {
    cli_access_server("kvs", "127.0.0.1:4234");
}

#[test]
fn cli_access_server_sled_engine() {
    cli_access_server("sled", "127.0.0.1:4005");
}

fn retry_with_backoff<F, R, E>(
    mut f: F,
    max_retries: u32,
    initial_delay: Duration,
    max_delay: Duration,
) -> Result<R, E>
where
    F: FnMut() -> Result<R, E>,
    E: std::fmt::Debug,
{
    let mut rng = rand::thread_rng();
    let mut delay = initial_delay;
    let mut attempts = 0;

    loop {
        match f() {
            Ok(result) => return Ok(result),
            Err(e) => {
                attempts += 1;
                if attempts >= max_retries {
                    return Err(e);
                }

                // Calculate sleep duration with jitter
                let jitter = rng.gen_range(0, 100) as f64 / 100.0;
                let sleep_duration = (delay.as_millis() as f64 * (1.0 + jitter)) as u64;
                thread::sleep(Duration::from_millis(sleep_duration));

                delay = std::cmp::min(delay * 2, max_delay);
            }
        }
    }
}
