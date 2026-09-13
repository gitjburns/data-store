//! Immutable settings travel with explicit storage handles, never a process-global registry.

use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    assembly::model::AssemblyPolicy, config::ServiceConfig, error::ApiError, limits::RuntimeLimits,
    query::profile::RetrievalProfile,
};

/// Effective limits and their audit documents are constructed together once at startup.
#[derive(Debug)]
pub(crate) struct RuntimeSettings {
    pub(crate) limits: RuntimeLimits,
    pub(crate) retrieval_profile: RetrievalProfile,
    pub(crate) assembly_policy: AssemblyPolicy,
}

impl RuntimeSettings {
    /// Seal profiles from the same validated values every runtime consumer receives.
    pub(crate) fn from_config(config: &ServiceConfig) -> Result<Arc<Self>, ApiError> {
        let limits = config.runtime_limits();
        limits
            .validate()
            .map_err(|message| ApiError::InvalidConfig { message })?;
        Ok(Arc::new(Self {
            retrieval_profile: crate::query::profile::from_limits(&limits.retrieval)?,
            assembly_policy: crate::assembly::policy::from_limits(&limits.retrieval)?,
            limits,
        }))
    }
}

/// A storage root carries the settings needed to open bounded database/artifact readers.
/// Clones share immutable settings; changing a pathname cannot silently change policy.
#[derive(Debug, Clone)]
pub(crate) struct StorageContext {
    index_root: PathBuf,
    settings: Arc<RuntimeSettings>,
    // Every clone belongs to this storage owner's monitoring run. Observations
    // are transient and do not enter immutable settings or artifact identities.
    monitoring: Arc<crate::monitoring::Monitoring>,
}

impl StorageContext {
    /// Attach one validated runtime to its configured storage location.
    pub(crate) fn new(index_root: PathBuf, settings: Arc<RuntimeSettings>) -> Self {
        Self {
            index_root,
            settings,
            monitoring: Arc::new(crate::monitoring::Monitoring::new()),
        }
    }

    /// Share observations with admitted workers without a global registry or SQL reads.
    pub(crate) fn monitoring(&self) -> &Arc<crate::monitoring::Monitoring> {
        &self.monitoring
    }

    /// Borrow operational limits without re-reading TOML during an operation.
    pub(crate) fn limits(&self) -> &RuntimeLimits {
        &self.settings.limits
    }

    /// Profiles and resource admission must describe the same startup configuration.
    pub(crate) fn settings(&self) -> &RuntimeSettings {
        &self.settings
    }

    /// Long-lived readers share ownership so their settings survive a caller's scope.
    pub(crate) fn shared_settings(&self) -> Arc<RuntimeSettings> {
        Arc::clone(&self.settings)
    }
}

impl Deref for StorageContext {
    type Target = Path;
    /// Filesystem-only helpers may borrow the path; SQL openers require the full context.
    fn deref(&self) -> &Path {
        &self.index_root
    }
}

impl AsRef<Path> for StorageContext {
    /// Support ordinary path APIs without discarding the context at database boundaries.
    fn as_ref(&self) -> &Path {
        &self.index_root
    }
}
