fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=FYLES_WIRE_DIR");
    let wire = std::env::var("FYLES_WIRE_DIR").unwrap_or_else(|_| "../../wire".to_string());
    println!("cargo:rerun-if-changed={wire}/filerequest_compat.proto");
    for proto in ["filerequest.proto", "nodestatus.proto"] {
        println!("cargo:rerun-if-changed={wire}/{proto}");
        tonic_prost_build::compile_protos(format!("{wire}/{proto}"))?;
    }
    Ok(())
}
