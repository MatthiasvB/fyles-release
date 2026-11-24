fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=FYLES_WIRE_DIR");
    let wire = std::env::var("FYLES_WIRE_DIR").unwrap_or_else(|_| "../../wire".to_string());
    println!("cargo:rerun-if-changed={wire}/license.proto");
    tonic_prost_build::compile_protos(format!("{wire}/license.proto"))?;
    Ok(())
}
