"""memlake-backed file storage.

Stores each uploaded file as a single-body document in memlake's dedicated document store — the
same content-addressed blob machinery that holds documents' extracted text and chunks (see
docs/documents-chunks.md). ``file_storage`` stays the seam the rest of Hindsight uses; this is
simply a backend for it, selected with ``HINDSIGHT_API_FILE_STORAGE_TYPE=memlake``.

Each storage key maps to one memlake "document" whose only body is the file: ``store`` uploads it
over a presigned PUT (dedup skips an unchanged re-upload), ``retrieve`` / ``get_download_url`` mint
a presigned GET, ``delete`` drops the record (its body is reclaimed by memlake's orphan sweep), and
``exists`` is a record lookup. The file lives in the file's OWN bank namespace (parsed from the
``banks/{bank_id}/…`` key), so dropping the bank drops its files too. Bodies move client↔S3
directly; the gRPC server only ever carries small metadata.
"""

from __future__ import annotations

import asyncio
import hashlib
from functools import partial

import memlake_client as mc  # type: ignore[unresolved-import]

from .base import FileStorage


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class MemlakeFileStorage(FileStorage):
    def __init__(self, target: "str | list[str]", namespace_prefix: str = "") -> None:
        self._client = mc.MemlakeClient(target)
        self._prefix = namespace_prefix
        self._ensured: set[str] = set()

    def _resolve(self, key: str) -> tuple[str, str]:
        """(namespace, document_id) for a storage key. The key is ``banks/{bank_id}/files/…``, so
        the file lands in that bank's namespace; the document id is a key-safe hash of the full key
        (prefixed so it never collides with a real document's record)."""
        parts = key.split("/")
        bank_id = parts[1] if len(parts) >= 2 and parts[0] == "banks" else "_files"
        namespace = f"{self._prefix}{bank_id}"
        document_id = "_file_" + _sha256(key.encode())
        return namespace, document_id

    async def _ensure(self, namespace: str) -> None:
        if namespace in self._ensured:
            return
        await asyncio.to_thread(partial(self._client.create_namespace, namespace))
        self._ensured.add(namespace)

    async def store(self, file_data: bytes, key: str, metadata: dict[str, str] | None = None) -> str:
        namespace, document_id = self._resolve(key)
        await self._ensure(namespace)
        meta = metadata or {}
        file_hash = _sha256(file_data)
        doc = mc.document(
            document_id,
            content_hash=file_hash,
            file_hash=file_hash,
            file_bytes=len(file_data),
            file_content_type=meta.get("content_type", "application/octet-stream"),
            file_original_name=meta.get("original_name", key.rsplit("/", 1)[-1]),
            metadata={"storage_key": key},
        )
        resp = await asyncio.to_thread(partial(self._client.put_documents, namespace, [doc]))
        # Upload the body only if memlake asked for it (missing — an identical re-store dedups).
        if resp.uploads and resp.uploads[0].file.url:
            await asyncio.to_thread(self._client.upload_blob, resp.uploads[0].file.url, file_data)
        return key

    async def retrieve(self, key: str) -> bytes:
        namespace, document_id = self._resolve(key)
        resp = await asyncio.to_thread(partial(self._client.get_document, namespace, document_id, include_file=True))
        if not resp.found or not resp.file_url:
            raise FileNotFoundError(key)
        return await asyncio.to_thread(self._client.download_blob, resp.file_url)

    async def delete(self, key: str) -> None:
        namespace, document_id = self._resolve(key)
        await asyncio.to_thread(partial(self._client.delete_document, namespace, document_id))

    async def exists(self, key: str) -> bool:
        namespace, document_id = self._resolve(key)
        resp = await asyncio.to_thread(partial(self._client.get_document, namespace, document_id))
        return bool(resp.found)

    async def get_download_url(self, key: str, expires_in: int = 3600) -> str:
        namespace, document_id = self._resolve(key)
        resp = await asyncio.to_thread(
            partial(
                self._client.get_document,
                namespace,
                document_id,
                include_file=True,
                url_ttl_seconds=expires_in,
            )
        )
        if not resp.found or not resp.file_url:
            raise FileNotFoundError(key)
        return resp.file_url
