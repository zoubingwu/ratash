use std::process::Command;

const MIHOMO_OVERRIDE_ENVIRONMENT: [&str; 15] = [
    "CLASH_AGE_SECRET_KEY",
    "CLASH_CONFIG_FILE",
    "CLASH_CONFIG_STRING",
    "CLASH_HOME_DIR",
    "CLASH_OVERRIDE_EXTERNAL_CONTROLLER",
    "CLASH_OVERRIDE_EXTERNAL_CONTROLLER_PIPE",
    "CLASH_OVERRIDE_EXTERNAL_CONTROLLER_ROUTING_MARK",
    "CLASH_OVERRIDE_EXTERNAL_CONTROLLER_TLS",
    "CLASH_OVERRIDE_EXTERNAL_CONTROLLER_UNIX",
    "CLASH_OVERRIDE_EXTERNAL_UI_DIR",
    "CLASH_OVERRIDE_SECRET",
    "CLASH_POST_DOWN",
    "CLASH_POST_UP",
    "SAFE_PATHS",
    "SKIP_SAFE_PATH_CHECK",
];

pub(crate) fn enforce_managed_runtime(command: &mut Command) {
    for variable in MIHOMO_OVERRIDE_ENVIRONMENT {
        command.env_remove(variable);
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;

    use super::*;

    #[test]
    fn managed_runtime_removes_configuration_and_lifecycle_overrides() {
        let mut command = Command::new("mihomo");
        for variable in MIHOMO_OVERRIDE_ENVIRONMENT {
            command.env(variable, "untrusted");
        }

        enforce_managed_runtime(&mut command);

        for variable in MIHOMO_OVERRIDE_ENVIRONMENT {
            assert_eq!(
                command
                    .get_envs()
                    .find(|(name, _)| *name == OsStr::new(variable)),
                Some((OsStr::new(variable), None))
            );
        }
    }
}
