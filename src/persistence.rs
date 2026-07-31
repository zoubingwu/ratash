use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};

use crate::domain::{LocalRuleSetRevision, ProfileId, RuntimeGeneration};
use crate::profile::ProfileRevision;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ObjectId(String);

impl ObjectId {
    pub fn parse(value: &str) -> io::Result<Self> {
        if value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            Ok(Self(value.to_owned()))
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "object ID must be a lowercase SHA-256 digest",
            ))
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn for_content(content: &[u8]) -> Self {
        Self(crate::digest::sha256_hex(content))
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TransactionId(String);

impl TransactionId {
    pub fn parse(value: &str) -> io::Result<Self> {
        validate_digest(value)?;
        Ok(Self(value.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn for_content(content: &[u8]) -> Self {
        Self(crate::digest::sha256_hex(content))
    }
}

impl<'de> Deserialize<'de> for TransactionId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBundle {
    pub supervisor_state: ObjectId,
    pub profile_snapshot: ObjectId,
    pub local_rule_set: ObjectId,
    pub effective_configuration: ObjectId,
    pub profile_revision: ProfileRevision,
    pub local_rule_set_revision: LocalRuleSetRevision,
    pub active_profile_id: ProfileId,
    pub runtime_generation: RuntimeGeneration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TransactionBundleDto {
    schema_version: u16,
    supervisor_state: ObjectId,
    profile_snapshot: ObjectId,
    local_rule_set: ObjectId,
    effective_configuration: ObjectId,
    profile_revision: u64,
    local_rule_set_revision: u64,
    active_profile_id: String,
    runtime_generation: u64,
}

impl From<&TransactionBundle> for TransactionBundleDto {
    fn from(bundle: &TransactionBundle) -> Self {
        Self {
            schema_version: 1,
            supervisor_state: bundle.supervisor_state.clone(),
            profile_snapshot: bundle.profile_snapshot.clone(),
            local_rule_set: bundle.local_rule_set.clone(),
            effective_configuration: bundle.effective_configuration.clone(),
            profile_revision: bundle.profile_revision.0,
            local_rule_set_revision: bundle.local_rule_set_revision.0,
            active_profile_id: bundle.active_profile_id.to_string(),
            runtime_generation: bundle.runtime_generation.0,
        }
    }
}

impl TryFrom<TransactionBundleDto> for TransactionBundle {
    type Error = io::Error;

    fn try_from(dto: TransactionBundleDto) -> Result<Self, Self::Error> {
        if dto.schema_version != 1 {
            return Err(invalid_data(
                "unsupported transaction bundle schema version",
            ));
        }
        let active_profile_id = ProfileId::parse(&dto.active_profile_id)
            .map_err(|_| invalid_data("transaction bundle contains an invalid Profile ID"))?;
        Ok(Self {
            supervisor_state: dto.supervisor_state,
            profile_snapshot: dto.profile_snapshot,
            local_rule_set: dto.local_rule_set,
            effective_configuration: dto.effective_configuration,
            profile_revision: ProfileRevision(dto.profile_revision),
            local_rule_set_revision: LocalRuleSetRevision(dto.local_rule_set_revision),
            active_profile_id,
            runtime_generation: RuntimeGeneration(dto.runtime_generation),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PreparedTransaction {
    pub candidate: TransactionId,
    pub previous: Option<TransactionId>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommittedManifest {
    pub current: TransactionId,
    pub previous: Option<TransactionId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryState {
    pub committed: Option<CommittedManifest>,
    pub prepared: Option<PreparedTransaction>,
}

#[derive(Debug)]
pub struct PersistenceStore {
    root: PathBuf,
}

impl PersistenceStore {
    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        create_private_directory(root)?;
        create_private_directory(&root.join("objects"))?;
        create_private_directory(&root.join("transactions"))?;
        Ok(Self {
            root: root.to_owned(),
        })
    }

    pub fn put_object(&self, content: &[u8]) -> io::Result<ObjectId> {
        let id = ObjectId::for_content(content);
        self.put_immutable(&self.object_path(&id), content)?;
        Ok(id)
    }

    pub fn read_object(&self, id: &ObjectId) -> io::Result<Vec<u8>> {
        let mut content = Vec::new();
        File::open(self.object_path(id))?.read_to_end(&mut content)?;
        if ObjectId::for_content(&content) != *id {
            return Err(invalid_data("stored object does not match its content ID"));
        }
        Ok(content)
    }

    pub fn prepare(&self, bundle: &TransactionBundle) -> io::Result<PreparedTransaction> {
        if read_optional_json::<PreparedTransaction>(&self.root.join("prepared.json"))?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "a prepared transaction requires recovery",
            ));
        }
        self.verify_bundle_objects(bundle)?;
        let serialized = serialize(&TransactionBundleDto::from(bundle))?;
        let candidate = TransactionId::for_content(&serialized);
        self.put_immutable(&self.transaction_path(&candidate), &serialized)?;
        let prepared = PreparedTransaction {
            candidate,
            previous: self.read_manifest()?.map(|manifest| manifest.current),
        };
        replace_private_file(&self.root.join("prepared.json"), &serialize(&prepared)?)?;
        Ok(prepared)
    }

    pub fn recover(&self) -> io::Result<RecoveryState> {
        Ok(RecoveryState {
            committed: self.read_manifest()?,
            prepared: read_optional_json(&self.root.join("prepared.json"))?,
        })
    }

    pub fn commit_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        let journal: Option<PreparedTransaction> =
            read_optional_json(&self.root.join("prepared.json"))?;
        if journal.as_ref() != Some(prepared) {
            return Err(invalid_data(
                "prepared transaction does not match the persisted journal",
            ));
        }
        let current = self.read_manifest()?.map(|manifest| manifest.current);
        if current != prepared.previous {
            return Err(invalid_data(
                "prepared transaction is stale relative to the committed manifest",
            ));
        }
        let bundle = self.load_transaction(&prepared.candidate)?;
        self.verify_bundle_objects(&bundle)?;
        let manifest = CommittedManifest {
            current: prepared.candidate.clone(),
            previous: prepared.previous.clone(),
        };
        replace_private_file(&self.root.join("manifest.json"), &serialize(&manifest)?)
    }

    pub fn clear_prepared(&self, prepared: &PreparedTransaction) -> io::Result<()> {
        let path = self.root.join("prepared.json");
        let journal: Option<PreparedTransaction> = read_optional_json(&path)?;
        if journal.as_ref() != Some(prepared) {
            return Err(invalid_data(
                "prepared transaction does not match the persisted journal",
            ));
        }
        fs::remove_file(path)?;
        sync_directory(&self.root)
    }

    pub fn load_transaction(&self, id: &TransactionId) -> io::Result<TransactionBundle> {
        let content = read_hashed_file(&self.transaction_path(id), id.as_str())?;
        let dto: TransactionBundleDto = deserialize(&content)?;
        dto.try_into()
    }

    fn object_path(&self, id: &ObjectId) -> PathBuf {
        self.root.join("objects").join(id.as_str())
    }

    fn transaction_path(&self, id: &TransactionId) -> PathBuf {
        self.root.join("transactions").join(id.as_str())
    }

    fn put_immutable(&self, path: &Path, content: &[u8]) -> io::Result<()> {
        match fs::read(path) {
            Ok(stored) if stored == content => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        let parent = path.parent().expect("immutable path has a parent");
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| invalid_data("immutable file must have a UTF-8 name"))?;
        let temporary = parent.join(format!(".{file_name}.{}.tmp", uuid::Uuid::new_v4()));
        let mut file = private_new_file(&temporary)?;
        if let Err(error) = write_and_sync(&mut file, content) {
            drop(file);
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        drop(file);
        if let Err(error) = fs::rename(&temporary, path) {
            let _ = fs::remove_file(&temporary);
            return Err(error);
        }
        sync_directory(parent)
    }

    fn verify_bundle_objects(&self, bundle: &TransactionBundle) -> io::Result<()> {
        self.read_object(&bundle.supervisor_state)?;
        self.read_object(&bundle.profile_snapshot)?;
        self.read_object(&bundle.local_rule_set)?;
        self.read_object(&bundle.effective_configuration)?;
        Ok(())
    }

    fn read_manifest(&self) -> io::Result<Option<CommittedManifest>> {
        read_optional_json(&self.root.join("manifest.json"))
    }
}

fn create_private_directory(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn private_new_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    options.open(path)
}

fn write_and_sync(file: &mut File, content: &[u8]) -> io::Result<()> {
    file.write_all(content)?;
    file.sync_all()
}

fn replace_private_file(path: &Path, content: &[u8]) -> io::Result<()> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_data("state file must have a UTF-8 name"))?;
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", std::process::id()));
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(&temporary)?;
    #[cfg(unix)]
    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
    if let Err(error) = write_and_sync(&mut file, content) {
        drop(file);
        let _ = fs::remove_file(&temporary);
        return Err(error);
    }
    drop(file);
    fs::rename(&temporary, path)?;
    sync_directory(path.parent().expect("state path has a parent"))
}

fn read_optional_json<T>(path: &Path) -> io::Result<Option<T>>
where
    T: for<'de> Deserialize<'de>,
{
    match fs::read(path) {
        Ok(content) => deserialize(&content).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_hashed_file(path: &Path, expected: &str) -> io::Result<Vec<u8>> {
    let content = fs::read(path)?;
    if crate::digest::sha256_hex(&content) != expected {
        return Err(invalid_data("stored content does not match its ID"));
    }
    Ok(content)
}

fn serialize<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn deserialize<T>(content: &[u8]) -> io::Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_slice(content)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn validate_digest(value: &str) -> io::Result<()> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "ID must be a lowercase SHA-256 digest",
        ))
    }
}

fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
