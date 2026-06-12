use base64::Engine;
use subtle::ConstantTimeEq;
use tonic::{Request, Status};

use crate::library::util::util::generate_random_bytes;

pub(super) fn bearer_auth_interceptor() -> (
    impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone,
    String,
) {
    // Generate a random 32-byte token
    let token = generate_random_bytes(32);
    let token_string = base64::engine::general_purpose::STANDARD.encode(&token);

    let interceptor = move |req: Request<()>| {
        let hdr = req
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| Status::unauthenticated("missing authorization"))?;

        let b64 = hdr
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("bad authorization scheme"))?;
        let presented = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|_| Status::unauthenticated("bad token encoding"))?;

        if presented.len() != token.len() {
            return Err(Status::unauthenticated("invalid token"));
        }
        if presented.ct_eq(token.as_slice()).unwrap_u8() != 1 {
            return Err(Status::unauthenticated("invalid token"));
        }

        Ok(req)
    };

    (interceptor, token_string)
}
