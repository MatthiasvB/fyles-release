fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../wire/filerequest.proto")?;
    tonic_build::compile_protos("../../wire/nodestatus.proto")?;
    Ok(())
}
