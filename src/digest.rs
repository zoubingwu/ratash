use std::io::{self, Read, Write};

use sha2::{Digest, Sha256};

const STREAM_BUFFER_BYTES: usize = 8 * 1_024;

#[must_use]
pub(crate) fn sha256_hex(content: &[u8]) -> String {
    encode_sha256(&Sha256::digest(content))
}

pub(crate) fn sha256_reader_hex_bounded(reader: impl Read, max_bytes: usize) -> io::Result<String> {
    copy_and_sha256_hex_bounded(reader, io::sink(), max_bytes)
}

pub(crate) fn copy_and_sha256_hex_bounded(
    reader: impl Read,
    mut writer: impl Write,
    max_bytes: usize,
) -> io::Result<String> {
    let mut reader = reader.take((max_bytes as u64).saturating_add(1));
    let mut hasher = Sha256::new();
    let mut buffer = [0; STREAM_BUFFER_BYTES];
    let mut copied = 0;
    loop {
        let length = match reader.read(&mut buffer) {
            Ok(length) => length,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if length == 0 {
            break;
        }
        if length > max_bytes.saturating_sub(copied) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stream exceeds its size limit",
            ));
        }
        writer.write_all(&buffer[..length])?;
        hasher.update(&buffer[..length]);
        copied += length;
    }
    Ok(encode_sha256(&hasher.finalize()))
}

fn encode_sha256(digest: &[u8]) -> String {
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
    use std::io::{self, Read};

    use super::{
        STREAM_BUFFER_BYTES, copy_and_sha256_hex_bounded, is_lower_sha256_hex, sha256_hex,
        sha256_reader_hex_bounded,
    };

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

    #[test]
    fn reader_digest_uses_a_bounded_streaming_buffer() {
        let length = STREAM_BUFFER_BYTES * 4 + 1;
        let mut reader = RepeatedByteReader::new(b'a', length);

        let digest =
            sha256_reader_hex_bounded(&mut reader, length).expect("the bounded stream should hash");

        assert_eq!(digest, sha256_hex(&vec![b'a'; length]));
        assert!(reader.largest_request <= STREAM_BUFFER_BYTES);
    }

    #[test]
    fn reader_digest_propagates_read_errors() {
        let error = sha256_reader_hex_bounded(FailingReader, STREAM_BUFFER_BYTES)
            .expect_err("the read should fail");

        assert_eq!(error.kind(), io::ErrorKind::Other);
    }

    #[test]
    fn reader_digest_rejects_content_above_the_byte_limit() {
        let mut reader = RepeatedByteReader::new(b'a', STREAM_BUFFER_BYTES + 1);
        let mut copied = Vec::new();

        let error = copy_and_sha256_hex_bounded(&mut reader, &mut copied, STREAM_BUFFER_BYTES)
            .expect_err("the stream should exceed its limit");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(copied.len(), STREAM_BUFFER_BYTES);
    }

    #[test]
    fn reader_digest_retries_an_interrupted_read() {
        let mut reader = InterruptedOnceReader::new(RepeatedByteReader::new(b'a', 3));

        let digest = sha256_reader_hex_bounded(&mut reader, 3)
            .expect("the interrupted stream should resume");

        assert_eq!(digest, sha256_hex(b"aaa"));
    }

    struct RepeatedByteReader {
        byte: u8,
        remaining: usize,
        largest_request: usize,
    }

    impl RepeatedByteReader {
        fn new(byte: u8, remaining: usize) -> Self {
            Self {
                byte,
                remaining,
                largest_request: 0,
            }
        }
    }

    impl Read for RepeatedByteReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.largest_request = self.largest_request.max(buffer.len());
            let length = self.remaining.min(buffer.len());
            buffer[..length].fill(self.byte);
            self.remaining -= length;
            Ok(length)
        }
    }

    struct FailingReader;

    impl Read for FailingReader {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("fixture read failed"))
        }
    }

    struct InterruptedOnceReader<R> {
        inner: R,
        interrupted: bool,
    }

    impl<R> InterruptedOnceReader<R> {
        fn new(inner: R) -> Self {
            Self {
                inner,
                interrupted: false,
            }
        }
    }

    impl<R: Read> Read for InterruptedOnceReader<R> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if !self.interrupted {
                self.interrupted = true;
                return Err(io::Error::from(io::ErrorKind::Interrupted));
            }
            self.inner.read(buffer)
        }
    }
}
