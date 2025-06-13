import json


def load_config(config_path: str) -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        print(f"[INFO] Loaded config from {config_path}")
        return config
    except FileNotFoundError:
        print(f"[ERROR] Config file not found: {config_path}")
        raise
    except json.JSONDecodeError as e:
        print(f"[ERROR] Invalid JSON in config file: {e}")
        raise
