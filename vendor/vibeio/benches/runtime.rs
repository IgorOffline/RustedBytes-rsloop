use std::time::{Duration, Instant};

fn iterations() -> usize {
    std::env::var("VIBEIO_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100_000)
}

fn report(name: &str, operations: usize, elapsed: Duration) {
    let nanoseconds = elapsed.as_secs_f64() * 1_000_000_000.0;
    println!(
        "{{\"benchmark\":\"{name}\",\"operations\":{operations},\"seconds\":{},\"ns_per_op\":{}}}",
        elapsed.as_secs_f64(),
        nanoseconds / operations as f64,
    );
}

fn main() -> std::io::Result<()> {
    let operations = iterations();
    let runtime = vibeio::RuntimeBuilder::new()
        .rsloop_profile()
        .enable_timer(true)
        .build()?;

    let started = Instant::now();
    runtime.block_on(async move {
        let mut handles = Vec::with_capacity(operations);
        for _ in 0..operations {
            handles.push(vibeio::spawn(async {}));
        }
        for handle in handles {
            handle.await;
        }
    });
    report("spawn_and_join", operations, started.elapsed());

    let timer_operations = operations.min(10_000);
    let started = Instant::now();
    runtime.block_on(async move {
        let deadline = Instant::now() + Duration::from_millis(1);
        let mut handles = Vec::with_capacity(timer_operations);
        for _ in 0..timer_operations {
            handles.push(vibeio::spawn(vibeio::time::Sleep::sleep_until(deadline)));
        }
        for handle in handles {
            handle.await;
        }
    });
    report("timer_fanout", timer_operations, started.elapsed());
    Ok(())
}
