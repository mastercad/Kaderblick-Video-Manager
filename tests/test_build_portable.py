from pathlib import Path

from scripts import build_portable


def test_split_file_creates_numbered_parts(tmp_path):
    source = tmp_path / "artifact.tar.xz"
    source.write_bytes(b"a" * 10 + b"b" * 10 + b"c" * 5)

    parts = build_portable._split_file(source, chunk_size=10)

    assert [part.name for part in parts] == [
        "artifact.tar.xz.part-00",
        "artifact.tar.xz.part-01",
        "artifact.tar.xz.part-02",
    ]
    assert parts[0].read_bytes() == b"a" * 10
    assert parts[1].read_bytes() == b"b" * 10
    assert parts[2].read_bytes() == b"c" * 5


def test_finalize_archive_splits_large_linux_asset_and_keeps_checksum(tmp_path, monkeypatch):
    archive = tmp_path / "kaderblick-video-manager-linux-x64.tar.gz"
    archive.write_bytes(b"x" * 25)

    monkeypatch.setattr(build_portable, "MAX_RELEASE_ASSET_BYTES", 20)
    monkeypatch.setattr(build_portable, "CHUNK_SIZE_BYTES", 10)

    produced = build_portable._finalize_archive(archive)

    names = [path.name for path in produced]
    assert names == [
        "kaderblick-video-manager-linux-x64.tar.gz.part-00",
        "kaderblick-video-manager-linux-x64.tar.gz.part-01",
        "kaderblick-video-manager-linux-x64.tar.gz.part-02",
        "kaderblick-video-manager-linux-x64.tar.gz.sha256",
        "kaderblick-video-manager-linux-x64.tar.gz.parts.txt",
    ]
    assert archive.exists() is False
    instructions = produced[-1].read_text(encoding="utf-8")
    assert "cat kaderblick-video-manager-linux-x64.tar.gz.part-00" in instructions
    assert "sha256sum -c kaderblick-video-manager-linux-x64.tar.gz.sha256" in instructions


def test_finalize_archive_keeps_small_asset_intact(tmp_path, monkeypatch):
    archive = tmp_path / "kaderblick-video-manager-windows-x64.zip"
    archive.write_bytes(b"payload")

    monkeypatch.setattr(build_portable, "MAX_RELEASE_ASSET_BYTES", 100)

    produced = build_portable._finalize_archive(archive)

    assert [path.name for path in produced] == [
        "kaderblick-video-manager-windows-x64.zip",
        "kaderblick-video-manager-windows-x64.zip.sha256",
    ]
    assert archive.exists() is True