def build_container_runtime_config(memory_bytes, cpu_cores, hostname, network_name, ports, volumes):
    """Return a safer runtime config for non-privileged sandboxing."""
    return {
        "detach": True,
        "privileged": False,
        "cap_drop": ["ALL"],
        "security_opt": ["no-new-privileges:true"],
        "hostname": hostname,
        "mem_limit": memory_bytes,
        "cpu_period": 100000,
        "cpu_quota": int(cpu_cores * 100000),
        "network": network_name,
        "ports": ports,
        "volumes": volumes,
        "restart_policy": {"Name": "always"},
    }


def normalize_vps_credentials(username=None, password=None):
    """Normalize VPS login details and fall back to safe defaults."""
    normalized_username = (username or "root").strip() or "root"
    normalized_password = (password or "root").strip() or "root"
    return normalized_username, normalized_password


def validate_vps_request(memory, cpu, disk):
    """Validate basic VPS request parameters before provisioning."""
    if memory < 1 or memory > 512:
        return False, "Memory must be between 1GB and 512GB"
    if cpu < 1 or cpu > 32:
        return False, "CPU cores must be between 1 and 32"
    if disk < 10 or disk > 1000:
        return False, "Disk space must be between 10GB and 1000GB"
    return True, "ok"
