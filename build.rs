fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .out_dir("src/server/proto")
        .compile(
            &["src/server/idl/atsdb/proto/service.proto"],
            &["src/server/idl/atsdb"],
        )?;
    Ok(())
}
