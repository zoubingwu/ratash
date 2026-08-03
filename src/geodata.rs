use std::collections::BTreeSet;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::config::BUNDLED_CORE_VERSION;
use crate::digest::is_lower_sha256_hex;

const BUNDLED_MANIFEST: &str = include_str!("../fixtures/mihomo/v1.19.28/geodata-manifest.json");
const MANIFEST_SCHEMA_VERSION: u16 = 1;
const REPOSITORY: &str = "https://github.com/MetaCubeX/meta-rules-dat";
const REPOSITORY_LICENSE: &str = "GPL-3.0-only";
const MAX_ASSET_BYTES: u64 = 32 * 1_024 * 1_024;
const EXPECTED_ASSETS: [(&str, &str); 4] = [
    ("ASN.mmdb", "GeoLite2-ASN.mmdb"),
    ("Country.mmdb", "country.mmdb"),
    ("GeoIP.dat", "geoip.dat"),
    ("GeoSite.dat", "geosite.dat"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeoDataAsset {
    file_name: String,
    size: u64,
    sha256: String,
}

impl GeoDataAsset {
    #[must_use]
    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    #[must_use]
    pub const fn size(&self) -> u64 {
        self.size
    }

    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GeoDataCatalog {
    assets: Vec<GeoDataAsset>,
}

impl GeoDataCatalog {
    pub fn bundled() -> Result<Self, GeoDataError> {
        Self::from_manifest(BUNDLED_MANIFEST)
    }

    pub fn from_manifest(manifest: &str) -> Result<Self, GeoDataError> {
        let manifest: Manifest =
            serde_json::from_str(manifest).map_err(|_| GeoDataError::InvalidManifest)?;
        validate_manifest(manifest)
    }

    #[must_use]
    pub fn assets(&self) -> &[GeoDataAsset] {
        &self.assets
    }

    pub(crate) fn stage_from(
        &self,
        source_root: &Path,
        destination_root: &Path,
    ) -> Result<(), GeoDataError> {
        verify_directory(source_root)?;
        verify_directory(destination_root)?;

        for asset in &self.assets {
            verify_asset(source_root, asset)?;
            let source = source_root.join(asset.file_name());
            cleanup_pending_link(&pending_link_path(destination_root, asset), &source)?;
            if installed_link_is_current(destination_root, asset, &source) {
                continue;
            }
            install_asset_link(destination_root, asset, &source)?;
        }
        sync_directory(destination_root).map_err(|_| GeoDataError::Io)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GeoDataError {
    InvalidManifest,
    InvalidRoot,
    InvalidAsset,
    Io,
}

impl fmt::Display for GeoDataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidManifest => "the bundled Geo-data manifest is invalid",
            Self::InvalidRoot => "the Geo-data root is invalid",
            Self::InvalidAsset => "a bundled Geo-data asset is invalid",
            Self::Io => "Geo-data staging failed",
        })
    }
}

impl std::error::Error for GeoDataError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u16,
    core_version: String,
    repository: String,
    asset_commit: String,
    source_commit: String,
    repository_license: String,
    assets: Vec<ManifestAsset>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestAsset {
    file_name: String,
    source_name: String,
    url: String,
    size: u64,
    sha256: String,
}

fn validate_manifest(manifest: Manifest) -> Result<GeoDataCatalog, GeoDataError> {
    if manifest.schema_version != MANIFEST_SCHEMA_VERSION
        || manifest.core_version != BUNDLED_CORE_VERSION
        || manifest.repository != REPOSITORY
        || manifest.repository_license != REPOSITORY_LICENSE
        || !is_lower_commit(&manifest.asset_commit)
        || !is_lower_commit(&manifest.source_commit)
        || manifest.assets.len() != EXPECTED_ASSETS.len()
    {
        return Err(GeoDataError::InvalidManifest);
    }

    let expected = EXPECTED_ASSETS.into_iter().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    let mut assets = Vec::with_capacity(EXPECTED_ASSETS.len());
    for asset in manifest.assets {
        if !expected.contains(&(asset.file_name.as_str(), asset.source_name.as_str()))
            || !observed.insert((asset.file_name.clone(), asset.source_name.clone()))
            || asset.size == 0
            || asset.size > MAX_ASSET_BYTES
            || !is_lower_sha256_hex(&asset.sha256)
            || asset.url
                != format!(
                    "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/{}/{}",
                    manifest.asset_commit, asset.source_name
                )
        {
            return Err(GeoDataError::InvalidManifest);
        }
        assets.push(GeoDataAsset {
            file_name: asset.file_name,
            size: asset.size,
            sha256: asset.sha256,
        });
    }
    if observed.len() != expected.len() {
        return Err(GeoDataError::InvalidManifest);
    }
    assets.sort_by(|left, right| left.file_name.cmp(&right.file_name));
    Ok(GeoDataCatalog { assets })
}

fn is_lower_commit(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn verify_directory(path: &Path) -> Result<(), GeoDataError> {
    if !path.is_absolute() {
        return Err(GeoDataError::InvalidRoot);
    }
    let metadata = fs::symlink_metadata(path).map_err(|_| GeoDataError::InvalidRoot)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(GeoDataError::InvalidRoot);
    }
    Ok(())
}

fn verify_asset(root: &Path, asset: &GeoDataAsset) -> Result<(), GeoDataError> {
    let path = root.join(asset.file_name());
    let metadata = fs::symlink_metadata(&path).map_err(|_| GeoDataError::InvalidAsset)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() != asset.size() {
        return Err(GeoDataError::InvalidAsset);
    }
    let mut file = File::open(path).map_err(|_| GeoDataError::InvalidAsset)?;
    let mut hasher = Sha256::new();
    let mut length = 0_u64;
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| GeoDataError::InvalidAsset)?;
        if read == 0 {
            break;
        }
        length = length
            .checked_add(read as u64)
            .ok_or(GeoDataError::InvalidAsset)?;
        if length > asset.size() {
            return Err(GeoDataError::InvalidAsset);
        }
        hasher.update(&buffer[..read]);
    }
    if length != asset.size() || encode_digest(hasher.finalize().as_ref()) != asset.sha256() {
        return Err(GeoDataError::InvalidAsset);
    }
    Ok(())
}

fn installed_link_is_current(root: &Path, asset: &GeoDataAsset, source: &Path) -> bool {
    let destination = root.join(asset.file_name());
    fs::symlink_metadata(&destination).is_ok_and(|metadata| metadata.file_type().is_symlink())
        && fs::read_link(destination).is_ok_and(|target| target == source)
}

fn install_asset_link(
    root: &Path,
    asset: &GeoDataAsset,
    source: &Path,
) -> Result<(), GeoDataError> {
    let pending = pending_link_path(root, asset);
    let destination = root.join(asset.file_name());
    let result = symlink(source, &pending)
        .and_then(|()| fs::rename(&pending, &destination))
        .map_err(|_| GeoDataError::Io);
    if result.is_err() {
        let _ = fs::remove_file(&pending);
    }
    result
}

fn pending_link_path(root: &Path, asset: &GeoDataAsset) -> PathBuf {
    root.join(format!(".ratash-geodata-{}.pending", asset.file_name()))
}

fn cleanup_pending_link(path: &Path, source: &Path) -> Result<(), GeoDataError> {
    match fs::symlink_metadata(path) {
        Ok(metadata)
            if metadata.file_type().is_symlink()
                && fs::read_link(path).is_ok_and(|target| target == source) =>
        {
            fs::remove_file(path).map_err(|_| GeoDataError::Io)
        }
        Ok(_) => Err(GeoDataError::InvalidAsset),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(GeoDataError::Io),
    }
}

fn encode_digest(digest: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bundled_catalog_is_pinned_to_the_core_and_four_canonical_assets() {
        let catalog = GeoDataCatalog::bundled().expect("the bundled manifest should be valid");

        assert_eq!(
            catalog
                .assets()
                .iter()
                .map(GeoDataAsset::file_name)
                .collect::<Vec<_>>(),
            ["ASN.mmdb", "Country.mmdb", "GeoIP.dat", "GeoSite.dat"]
        );
    }

    #[test]
    fn catalog_rejects_mutable_asset_urls() {
        let manifest = BUNDLED_MANIFEST.replace(
            "https://raw.githubusercontent.com/MetaCubeX/meta-rules-dat/",
            "https://github.com/MetaCubeX/meta-rules-dat/releases/download/latest/",
        );

        assert_eq!(
            GeoDataCatalog::from_manifest(&manifest),
            Err(GeoDataError::InvalidManifest)
        );
    }
}
