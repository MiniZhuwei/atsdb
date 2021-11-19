fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .compile(
            &["src/server/idl/opentelemetry-proto/opentelemetry/proto/collector/metrics/v1/metrics_service.proto"],
                 &["src/server/idl/opentelemetry-proto"]
        )?;
    Ok(())
}
