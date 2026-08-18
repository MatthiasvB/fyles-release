fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("../../wire/filerequest.proto")?;
    tonic_prost_build::compile_protos("../../wire/nodestatus.proto")?;
    Ok(())
}
