//! TLS configuration for lapin from a profile.

use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use rabbitmq_core::error::{CoreError, Result};
use rabbitmq_core::profile::Profile;

fn read(path: &str, what: &str) -> Result<Vec<u8>> {
    std::fs::read(path).map_err(|e| CoreError::invalid(format!("cannot read {what} {path}: {e}")))
}

pub fn lapin_tls(p: &Profile) -> Result<OwnedTLSConfig> {
    if !p.tls.verify {
        // lapin's rustls backend has no switch to disable verification; we do
        // not add one. Use a CA file for self-signed brokers instead.
        return Err(CoreError::unsupported(
            "tls.verify=false is not supported for AMQP connections; set tls.ca_file to the broker's CA instead",
        ));
    }
    let cert_chain = match &p.tls.ca_file {
        Some(f) => Some(
            String::from_utf8(read(f, "CA file")?)
                .map_err(|_| CoreError::invalid("CA file must be PEM text"))?,
        ),
        None => None,
    };
    let identity = if let Some(p12) = &p.tls.pkcs12_file {
        Some(OwnedIdentity::PKCS12 {
            der: read(p12, "PKCS#12 bundle")?,
            password: p.tls.pkcs12_password.as_ref().map(|s| s.expose().to_string()).unwrap_or_default(),
        })
    } else if let (Some(cert), Some(key)) = (&p.tls.client_cert_file, &p.tls.client_key_file) {
        Some(OwnedIdentity::PKCS8 { pem: read(cert, "client certificate")?, key: read(key, "client key")? })
    } else {
        None
    };
    Ok(OwnedTLSConfig { identity, cert_chain })
}
