import os
import shutil
import subprocess
import maturin

def _ensure_cargo():
    cargo_bin = os.path.expanduser("~/.cargo/bin")
    if os.path.isdir(cargo_bin) and cargo_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"

    if not shutil.which("cargo"):
        if os.name != "nt":
            print("[rust_clob] Installing Rust toolchain (rustup) automatically for pip build...")
            subprocess.run(
                "curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal",
                shell=True,
                check=True,
            )
            os.environ["PATH"] = f"{cargo_bin}{os.pathsep}{os.environ.get('PATH', '')}"

def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    _ensure_cargo()
    return maturin.build_wheel(wheel_directory, config_settings, metadata_directory)

def build_sdist(sdist_directory, config_settings=None):
    _ensure_cargo()
    return maturin.build_sdist(sdist_directory, config_settings)

def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    _ensure_cargo()
    return maturin.build_editable(wheel_directory, config_settings, metadata_directory)

def get_requires_for_build_wheel(config_settings=None):
    return maturin.get_requires_for_build_wheel(config_settings)

def get_requires_for_build_sdist(config_settings=None):
    return maturin.get_requires_for_build_sdist(config_settings)

def get_requires_for_build_editable(config_settings=None):
    return maturin.get_requires_for_build_editable(config_settings)

def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):
    _ensure_cargo()
    return maturin.prepare_metadata_for_build_wheel(metadata_directory, config_settings)

def prepare_metadata_for_build_editable(metadata_directory, config_settings=None):
    _ensure_cargo()
    return maturin.prepare_metadata_for_build_editable(metadata_directory, config_settings)
