use sha2::{Digest, Sha256};

#[must_use]
pub(crate) fn sha256_hex(content: &[u8]) -> String {
    let digest = Sha256::digest(content);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

#[must_use]
pub(crate) fn is_lower_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[cfg(test)]
mod tests {
    use super::is_lower_sha256_hex;

    const VALID_SHA256: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn lower_sha256_validation_accepts_a_valid_digest() {
        assert!(is_lower_sha256_hex(VALID_SHA256));
    }

    #[test]
    fn lower_sha256_validation_rejects_the_empty_value() {
        assert!(!is_lower_sha256_hex(""));
    }

    #[test]
    fn lower_sha256_validation_rejects_the_wrong_length() {
        assert!(!is_lower_sha256_hex(&VALID_SHA256[..63]));
    }

    #[test]
    fn lower_sha256_validation_rejects_uppercase_hex() {
        assert!(!is_lower_sha256_hex(
            "A123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }

    #[test]
    fn lower_sha256_validation_rejects_non_hexadecimal_input() {
        assert!(!is_lower_sha256_hex(
            "g123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        ));
    }
}
