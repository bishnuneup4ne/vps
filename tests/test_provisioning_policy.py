from provisioning_policy import build_container_runtime_config, normalize_vps_credentials, validate_vps_request


def test_runtime_config_avoids_privileged_access():
    config = build_container_runtime_config(
        memory_bytes=2 * 1024 * 1024 * 1024,
        cpu_cores=2,
        hostname="hostforge-test",
        network_name="bridge",
        ports={"22/tcp": "22022"},
        volumes={"hostforge-test": {"bind": "/data", "mode": "rw"}},
    )

    assert config["privileged"] is False
    assert config["cap_drop"] == ["ALL"]
    assert "cap_add" not in config
    assert config["security_opt"] == ["no-new-privileges:true"]


def test_validate_vps_request_rejects_invalid_specs():
    ok, message = validate_vps_request(memory=0, cpu=0, disk=0)
    assert ok is False
    assert "Memory" in message


def test_normalize_vps_credentials_uses_provided_values():
    username, password = normalize_vps_credentials("custom-user", "custom-pass")
    assert username == "custom-user"
    assert password == "custom-pass"
