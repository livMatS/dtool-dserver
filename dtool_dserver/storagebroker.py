"""Storage broker that accesses datasets through dserver signed URLs."""

import json
import logging
import os
import tempfile
from urllib.parse import urlparse, quote

import requests

from dtoolcore.storagebroker import BaseStorageBroker
from dtoolcore.filehasher import FileHasher, md5sum_hexdigest
from dtoolcore.utils import generate_identifier, mkdir_parents, get_config_value

try:
    from dtool_dserver import __version__
except ImportError:
    __version__ = "0.1.0"


logger = logging.getLogger(__name__)


# S3-compatible structure parameters (since dserver uploads to S3/Azure)
_STRUCTURE_PARAMETERS = {
    "dataset_registration_key": None,
    "data_key_infix": "data",
    "fragment_key_infix": "fragments",
    "overlays_key_infix": "overlays",
    "annotations_key_infix": "annotations",
    "tags_key_infix": "tags",
    "structure_key_suffix": "structure.json",
    "dtool_readme_key_suffix": "README.txt",
    "dataset_readme_key_suffix": "README.yml",
    "manifest_key_suffix": "manifest.json",
    "admin_metadata_key_suffix": "dtool",
    "http_manifest_key": "http_manifest.json",
    "storage_broker_version": __version__,
}

_DTOOL_README_TXT = """README
======
This is a Dtool dataset stored via dserver signed URL API.

Content provided during the dataset creation process
----------------------------------------------------

Dataset descriptive metadata: $UUID/README.yml
Dataset items prefixed by: $UUID/data/

The item identifiers are used to name the files using the data prefix.

Automatically generated files and directories
---------------------------------------------

This file: $UUID/README.txt
Administrative metadata describing the dataset: $UUID/dtool
Structural metadata describing the dataset: $UUID/structure.json
Structural metadata describing the data items: $UUID/manifest.json
Per item descriptive metadata: $UUID/overlays/
Dataset key/value pairs metadata: $UUID/annotations/
Dataset tags metadata: $UUID/tags/
"""


class DServerStorageBrokerError(Exception):
    """Base exception for DServerStorageBroker errors."""
    pass


class DServerAuthenticationError(DServerStorageBrokerError):
    """Authentication error with dserver."""
    pass


class DServerStorageBroker(BaseStorageBroker):
    """Storage broker that accesses datasets through dserver signed URLs.

    This broker doesn't directly access storage backends. Instead, it:
    1. Authenticates with dserver using a JWT token
    2. Requests signed URLs for read/write operations
    3. Uses the signed URLs to access the actual storage

    URI formats supported:
    1. Short: dserver://<server>/<uuid> (requires DSERVER_DEFAULT_BASE_URI)
    2. Full: dserver://<server>/<backend>/<bucket>/<uuid> (always works)

    Examples:
    - dserver://localhost:5000/550e8400-e29b-41d4-a716-446655440000
    - dserver://localhost:5000/s3/my-bucket/550e8400-e29b-41d4-a716-446655440000
    """

    #: Attribute used to define the type of storage broker.
    key = "dserver"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    #: Attribute used to document the structure of the dataset.
    _dtool_readme_txt = _DTOOL_README_TXT

    def __init__(self, uri, config_path=None):
        self._uri = uri
        self._config_path = config_path
        self._parse_uri(uri)

        # Structure parameters (copy to allow modification per instance)
        self._structure_parameters = _STRUCTURE_PARAMETERS.copy()

        # Data key prefix for items (will be set when uuid is known)
        self.data_key_prefix = None
        if self.uuid:
            self.data_key_prefix = f"{self.uuid}/data/"

        # Caches
        self._signed_urls_cache = None
        self._manifest_cache = None
        self._admin_metadata_cache = None
        self._readme_cache = None
        self._upload_urls_cache = None

        # Pending data for uploads
        self._pending_items = {}  # relpath -> {'fpath': path, 'identifier': id}
        self._name = None
        self._readme_content = None

    def _parse_uri(self, uri):
        """Parse dserver URI with flexible format support.

        Supports two formats:
        1. dserver://<server>/<uuid>  (short, requires default base_uri)
        2. dserver://<server>/<backend>/<bucket>/<uuid>  (full path)
        """
        parsed = urlparse(uri)

        if parsed.scheme != 'dserver':
            raise ValueError(f"Invalid scheme: {parsed.scheme} (expected 'dserver')")

        self._server = parsed.netloc
        if not self._server:
            raise ValueError("Missing server in URI")

        path_parts = [p for p in parsed.path.strip('/').split('/') if p]

        if len(path_parts) == 0:
            # Base URI for listing - no UUID specified
            # This is valid for list_dataset_uris() operations
            self.uuid = None
            self._backend_uri = None
            self._backend_base_uri = None
            self._resolve_mode = 'default'
            logger.debug(f"Parsed base URI (no UUID): server={self._server}, mode=default")
            return

        if len(path_parts) == 1:
            # Format 1: Short URI - dserver://server/uuid
            self.uuid = path_parts[0]
            self._backend_uri = None
            self._backend_base_uri = None
            self._resolve_mode = 'default'
            logger.debug(f"Parsed short URI: uuid={self.uuid}, mode=default")

        elif len(path_parts) == 2:
            # Two parts: Could be an error or incomplete path
            # Reject this to avoid ambiguity
            raise ValueError(
                f"Invalid dserver URI: {uri}. "
                f"Use short format (dserver://{self._server}/<uuid>) or "
                f"full format (dserver://{self._server}/<backend>/<bucket>/<uuid>)"
            )

        elif len(path_parts) == 3:
            # Format 3: Full path URI - dserver://server/backend/bucket/uuid
            backend = path_parts[0]
            bucket = path_parts[1]
            self.uuid = path_parts[2]
            self._backend_base_uri = f"{backend}://{bucket}"
            self._backend_uri = f"{self._backend_base_uri}/{self.uuid}"
            self._resolve_mode = 'full'
            logger.debug(f"Parsed full URI: backend_uri={self._backend_uri}, mode=full")

        else:
            # More than 3 parts - reject to avoid ambiguity
            raise ValueError(
                f"Invalid dserver URI: {uri}. "
                f"Too many path components. "
                f"Use short format (dserver://{self._server}/<uuid>) or "
                f"full format (dserver://{self._server}/<backend>/<bucket>/<uuid>)"
            )

    def _get_token(self):
        """Get authentication token from environment or file."""
        # First try environment variable
        token = os.environ.get('DSERVER_TOKEN')
        if token:
            return token

        # Then try token file
        token_file = os.environ.get('DSERVER_TOKEN_FILE')
        if token_file and os.path.exists(token_file):
            with open(token_file) as f:
                return f.read().strip()

        raise DServerAuthenticationError(
            "No dserver token found. Set DSERVER_TOKEN environment variable "
            "or DSERVER_TOKEN_FILE pointing to a file containing the token."
        )

    def _get_server_url(self, path):
        """Construct full URL for dserver API."""
        protocol = os.environ.get('DSERVER_PROTOCOL', 'http')
        return f"{protocol}://{self._server}{path}"

    def _get_default_base_uri(self):
        """Get default base URI from configuration.

        Checks in order:
        1. Server-specific environment variable
        2. Generic environment variable
        3. dtool config file (if available)
        """
        # First try server-specific config
        server_key = self._server.replace(':', '_').replace('.', '_')
        key = f"DSERVER_DEFAULT_BASE_URI_{server_key}"
        base_uri = get_config_value(key, config_path=self._config_path)
        if base_uri:
            logger.debug(f"Found default base_uri from {key}: {base_uri}")
            return base_uri

        # Then try generic config
        base_uri = get_config_value('DSERVER_DEFAULT_BASE_URI', config_path=self._config_path)
        if base_uri:
            logger.debug(f"Found default base_uri from DSERVER_DEFAULT_BASE_URI: {base_uri}")
            return base_uri

        return None


    def _query_backend_uri_from_dserver(self):
        """Query dserver to find the backend URI for this UUID.

        This is used when the URI format doesn't contain enough information
        to determine the backend URI (short format or path format).
        """
        try:
            # Query the /dataset/lookup/<uuid> endpoint
            response = self._api_request('GET', f'/dataset/lookup/{self.uuid}')
            data = response.json()
            backend_uri = data.get('uri')
            if backend_uri:
                logger.debug(f"Resolved UUID {self.uuid} to backend URI: {backend_uri}")
                return backend_uri
        except Exception as e:
            logger.warning(f"Failed to query dserver for UUID {self.uuid}: {e}")

        return None

    def _resolve_backend_uri(self):
        """Resolve the actual backend URI based on the parse mode.

        Returns the backend URI (e.g., s3://bucket/uuid) that can be used
        to instantiate the actual storage broker.
        """
        if self._backend_uri:
            # Already resolved (full format)
            return self._backend_uri

        if self._resolve_mode == 'default':
            # Use default base URI
            base_uri = self._get_default_base_uri()
            if not base_uri:
                raise DServerStorageBrokerError(
                    f"Short URI format requires DSERVER_DEFAULT_BASE_URI "
                    f"configuration for server {self._server}. "
                    f"Set DSERVER_DEFAULT_BASE_URI environment variable or add to dtool config."
                )
            self._backend_base_uri = base_uri
            self._backend_uri = f"{base_uri}/{self.uuid}"
            logger.info(f"Resolved short URI using default base_uri: {self._backend_uri}")

        return self._backend_uri

    def _api_request(self, method, path, **kwargs):
        """Make authenticated request to dserver."""
        url = self._get_server_url(path)
        headers = kwargs.pop('headers', {})
        headers['Authorization'] = f'Bearer {self._get_token()}'

        try:
            response = requests.request(method, url, headers=headers, **kwargs)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                raise DServerAuthenticationError(
                    "Authentication failed. Check your token."
                )
            raise DServerStorageBrokerError(f"API request failed: {e}")
        except requests.exceptions.RequestException as e:
            raise DServerStorageBrokerError(f"Connection error: {e}")

    # === Cache Directory ===

    def _get_cache_dir(self):
        """Get or create cache directory for downloaded items."""
        cache_base = os.environ.get(
            'DSERVER_CACHE_DIR',
            os.path.join(tempfile.gettempdir(), 'dtool-dserver-cache')
        )
        cache_dir = os.path.join(cache_base, self.uuid)
        mkdir_parents(cache_dir)
        return cache_dir

    # === Signed URL Fetching ===

    def _fetch_signed_urls(self):
        """Fetch signed URLs for reading the dataset."""
        if self._signed_urls_cache is None:
            if self.uuid is None:
                raise ValueError("No dataset UUID set")

            # Resolve backend URI if needed
            backend_uri = self._resolve_backend_uri()

            encoded_uri = quote(backend_uri, safe='')
            response = self._api_request(
                'GET',
                f'/signed-urls/dataset/{encoded_uri}'
            )
            self._signed_urls_cache = response.json()
            logger.debug(f"Fetched signed URLs for {backend_uri}")
        return self._signed_urls_cache

    def _fetch_upload_urls(self, items):
        """Fetch signed URLs for uploading a dataset."""
        if self._upload_urls_cache is None:
            if self.uuid is None:
                raise ValueError("No dataset UUID set")

            # Resolve backend base URI if needed
            if self._backend_base_uri is None:
                self._resolve_backend_uri()

            encoded_base_uri = quote(self._backend_base_uri, safe='')
            item_data = [
                {'relpath': relpath}
                for relpath in self._pending_items.keys()
            ]

            response = self._api_request(
                'POST',
                f'/signed-urls/upload/{encoded_base_uri}',
                json={
                    'uuid': self.uuid,
                    'name': self._name or self.uuid,
                    'items': item_data
                }
            )
            self._upload_urls_cache = response.json()
            logger.debug(f"Fetched upload URLs for {self._backend_base_uri}/{self.uuid}")
        return self._upload_urls_cache

    # === Read Operations ===

    def has_admin_metadata(self):
        """Check if dataset exists by attempting to fetch admin metadata.

        Returns False if no UUID is specified (base URI for listing).
        """
        if self.uuid is None:
            # This is a base URI for listing, not a dataset URI
            return False

        try:
            self.get_admin_metadata()
            return True
        except Exception:
            return False

    def get_admin_metadata(self):
        """Fetch admin metadata using signed URL."""
        if self._admin_metadata_cache is None:
            urls = self._fetch_signed_urls()
            response = requests.get(urls['admin_metadata_url'])
            response.raise_for_status()
            self._admin_metadata_cache = response.json()
        return self._admin_metadata_cache

    def get_manifest(self):
        """Fetch manifest using signed URL."""
        if self._manifest_cache is None:
            urls = self._fetch_signed_urls()
            response = requests.get(urls['manifest_url'])
            response.raise_for_status()
            self._manifest_cache = response.json()
        return self._manifest_cache

    def get_readme_content(self):
        """Fetch README using signed URL."""
        if self._readme_cache is None:
            urls = self._fetch_signed_urls()
            response = requests.get(urls['readme_url'])
            response.raise_for_status()
            self._readme_cache = response.text
        return self._readme_cache

    def get_text(self, key):
        """Get text content from storage."""
        # Map key to appropriate URL from signed URLs
        urls = self._fetch_signed_urls()

        if key.endswith('README.yml'):
            return self.get_readme_content()
        elif key.endswith('manifest.json'):
            return json.dumps(self.get_manifest())
        elif key.endswith('dtool'):
            return json.dumps(self.get_admin_metadata())
        else:
            raise ValueError(f"Unknown key: {key}")

    def put_text(self, key, content):
        """Store text content (for upload)."""
        # For now, store in memory until post_freeze_hook
        if key.endswith('README.yml'):
            self._readme_content = content
        elif key.endswith('manifest.json'):
            self._manifest_cache = json.loads(content)
        elif key.endswith('dtool'):
            self._admin_metadata_cache = json.loads(content)
        logger.debug(f"Staged text for upload: {key}")

    def get_overlay(self, overlay_name):
        """Fetch overlay using signed URL."""
        urls = self._fetch_signed_urls()
        overlay_url = urls.get('overlay_urls', {}).get(overlay_name)
        if not overlay_url:
            raise KeyError(f"Overlay {overlay_name} not found")
        response = requests.get(overlay_url)
        response.raise_for_status()
        return response.json()

    def get_annotation(self, annotation_name):
        """Fetch annotation using signed URL."""
        urls = self._fetch_signed_urls()
        annotation_url = urls.get('annotation_urls', {}).get(annotation_name)
        if not annotation_url:
            raise KeyError(f"Annotation {annotation_name} not found")
        response = requests.get(annotation_url)
        response.raise_for_status()
        return response.json()

    def list_overlay_names(self):
        """List available overlays."""
        urls = self._fetch_signed_urls()
        return list(urls.get('overlay_urls', {}).keys())

    def list_annotation_names(self):
        """List available annotations."""
        urls = self._fetch_signed_urls()
        return list(urls.get('annotation_urls', {}).keys())

    def list_tags(self):
        """List dataset tags."""
        urls = self._fetch_signed_urls()
        return urls.get('tags', [])

    def get_item_abspath(self, identifier):
        """Download item to cache and return local path."""
        cache_dir = self._get_cache_dir()

        # Get extension from manifest
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        relpath = item.get('relpath', identifier)
        _, ext = os.path.splitext(relpath)

        cache_path = os.path.join(cache_dir, identifier + ext)

        if not os.path.exists(cache_path):
            urls = self._fetch_signed_urls()
            item_url = urls.get('item_urls', {}).get(identifier)
            if not item_url:
                raise KeyError(f"Item {identifier} not found")

            logger.info(f"Downloading item {identifier} to cache")
            response = requests.get(item_url, stream=True)
            response.raise_for_status()

            tmp_path = cache_path + '.tmp'
            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            os.rename(tmp_path, cache_path)

        return cache_path

    def get_size_in_bytes(self, handle):
        """Get size of item.

        For new datasets being created, reads from local file.
        For existing datasets, reads from manifest.
        """
        # Check if this is a pending item (being uploaded)
        if handle in self._pending_items:
            fpath = self._pending_items[handle]['fpath']
            return os.path.getsize(fpath)

        # Otherwise get from manifest
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('size_in_bytes', 0)

    def get_utc_timestamp(self, handle):
        """Get timestamp of item.

        For new datasets being created, reads from local file.
        For existing datasets, reads from manifest.
        """
        # Check if this is a pending item (being uploaded)
        if handle in self._pending_items:
            fpath = self._pending_items[handle]['fpath']
            return os.path.getmtime(fpath)

        # Otherwise get from manifest
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('utc_timestamp', 0)

    def get_hash(self, handle):
        """Get hash of item.

        For new datasets being created, computes hash from local file.
        For existing datasets, reads from manifest.
        """
        # Check if this is a pending item (being uploaded)
        if handle in self._pending_items:
            fpath = self._pending_items[handle]['fpath']
            return self.hasher(fpath)

        # Otherwise get from manifest
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('hash', '')

    def get_relpath(self, handle):
        """Get relpath of item.

        For new datasets being created, handle is the relpath.
        For existing datasets, reads from manifest.
        """
        # Check if this is a pending item (being uploaded)
        if handle in self._pending_items:
            return handle  # handle is the relpath for pending items

        # Otherwise get from manifest
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('relpath', handle)

    # === Write Operations ===

    def create_structure(self):
        """Create necessary structure to hold a dataset.

        For dserver broker, actual creation is deferred to post_freeze_hook
        when we upload to storage. Here we just prepare the data key prefix.
        """
        logger.debug(f"Creating dataset structure for {self.uuid}")

        # Set up data key prefix now that we have uuid
        self.data_key_prefix = f"{self.uuid}/data/"

        # Structure will be uploaded during post_freeze_hook
        logger.debug("Structure creation deferred to upload")

    def put_item(self, fpath, relpath):
        """Stage item for upload. Actual upload happens on freeze."""
        identifier = generate_identifier(relpath)
        self._pending_items[relpath] = {
            'fpath': fpath,
            'identifier': identifier
        }
        logger.debug(f"Staged item for upload: {relpath} ({identifier})")
        return relpath

    def iter_item_handles(self):
        """Iterate over staged item handles."""
        for relpath in self._pending_items:
            yield relpath

    def add_item_metadata(self, handle, key, value):
        """Store item metadata (fragment)."""
        # Fragments are handled locally, stored in manifest during freeze
        if not hasattr(self, '_item_metadata'):
            self._item_metadata = {}
        identifier = generate_identifier(handle)
        if identifier not in self._item_metadata:
            self._item_metadata[identifier] = {}
        self._item_metadata[identifier][key] = value

    def get_item_metadata(self, handle):
        """Get item metadata (fragments)."""
        identifier = generate_identifier(handle)
        if hasattr(self, '_item_metadata'):
            return self._item_metadata.get(identifier, {})
        return {}

    def put_admin_metadata(self, admin_metadata):
        """Store admin metadata (uploaded on freeze)."""
        self._admin_metadata_cache = admin_metadata

    def put_manifest(self, manifest):
        """Store manifest (uploaded on freeze)."""
        self._manifest_cache = manifest

    def put_readme(self, content):
        """Store README content (uploaded on freeze)."""
        self._readme_content = content

    def pre_freeze_hook(self):
        """Called before freezing."""
        pass

    def post_freeze_hook(self):
        """Upload all data after dataset is frozen."""
        logger.info(f"Starting upload for dataset {self.uuid}")

        # Get upload URLs
        upload_info = self._fetch_upload_urls(self._pending_items)
        upload_urls = upload_info['upload_urls']

        # Upload admin metadata
        logger.debug("Uploading admin metadata")
        response = requests.put(
            upload_urls['admin_metadata'],
            data=json.dumps(self._admin_metadata_cache),
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()

        # Upload manifest
        logger.debug("Uploading manifest")
        response = requests.put(
            upload_urls['manifest'],
            data=json.dumps(self._manifest_cache),
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()

        # Upload structure.json
        if 'structure' in upload_urls:
            logger.debug("Uploading structure.json")
            structure_data = json.dumps(self._structure_parameters, indent=2, sort_keys=True)
            response = requests.put(
                upload_urls['structure'],
                data=structure_data,
                headers={'Content-Type': 'application/json'}
            )
            response.raise_for_status()

        # Upload README
        if self._readme_content:
            logger.debug("Uploading README")
            response = requests.put(
                upload_urls['readme'],
                data=self._readme_content,
                headers={'Content-Type': 'text/plain; charset=utf-8'}
            )
            response.raise_for_status()

        # Upload items
        for relpath, info in self._pending_items.items():
            identifier = info['identifier']
            item_upload_info = upload_urls['items'].get(identifier)
            if item_upload_info:
                logger.debug(f"Uploading item: {relpath}")
                with open(info['fpath'], 'rb') as f:
                    response = requests.put(item_upload_info['url'], data=f)
                    response.raise_for_status()

        # Signal upload complete to trigger indexing
        self._signal_upload_complete()
        logger.info(f"Upload complete for dataset {self.uuid}")

    def _signal_upload_complete(self):
        """Notify dserver that upload is complete."""
        logger.debug("Signaling upload complete")
        # Make sure backend URI is resolved
        backend_uri = self._resolve_backend_uri()
        self._api_request(
            'POST',
            '/signed-urls/upload-complete',
            json={'uri': backend_uri}
        )

    def delete_key(self, key):
        """Delete a key - not supported for dserver broker."""
        raise NotImplementedError(
            "Delete operations not supported through dserver broker"
        )

    # === Key Generation Methods ===

    def get_structure_key(self):
        """Return the key for structure.json."""
        return f"{self.uuid}/structure.json"

    def get_dtool_readme_key(self):
        """Return the key for dtool README."""
        return f"{self.uuid}/README.txt"

    def get_readme_key(self):
        """Return the key for dataset README."""
        return f"{self.uuid}/README.yml"

    def get_manifest_key(self):
        """Return the key for manifest."""
        return f"{self.uuid}/manifest.json"

    def get_admin_metadata_key(self):
        """Return the key for admin metadata."""
        return f"{self.uuid}/dtool"

    def get_overlay_key(self, overlay_name):
        """Return the key for an overlay."""
        return f"{self.uuid}/overlays/{overlay_name}.json"

    def get_annotation_key(self, annotation_name):
        """Return the key for an annotation."""
        return f"{self.uuid}/annotations/{annotation_name}.json"

    def get_tag_key(self, tag):
        """Return the key for a tag."""
        return f"{self.uuid}/tags/{tag}"

    # === Class Methods ===

    @classmethod
    def _get_default_base_uri_for_server(cls, server, config_path=None):
        """Get default base URI for a specific server (class method version)."""
        server_key = server.replace(':', '_').replace('.', '_')
        key = f"DSERVER_DEFAULT_BASE_URI_{server_key}"
        base_uri = get_config_value(key, config_path=config_path)
        if base_uri:
            return base_uri
        return get_config_value('DSERVER_DEFAULT_BASE_URI', config_path=config_path)


    @classmethod
    def list_dataset_uris(cls, base_uri, config_path=None):
        """List datasets by querying dserver API.

        Returns URIs in the shortest format based on configuration.
        """
        parsed = urlparse(base_uri)
        server = parsed.netloc
        path_parts = [p for p in parsed.path.strip('/').split('/') if p]

        # Determine the backend base URI to query
        if len(path_parts) == 0:
            # Base URI is just dserver://server/ - use default
            backend_base_uri = cls._get_default_base_uri_for_server(server, config_path)
            if not backend_base_uri:
                raise ValueError(
                    f"Cannot list datasets from dserver://{server}/ without "
                    "DSERVER_DEFAULT_BASE_URI configuration"
                )
        elif len(path_parts) == 1:
            # Single path component - reject as ambiguous
            raise ValueError(
                f"Invalid base URI: {base_uri}. "
                f"Use short format (dserver://{server}/) or "
                f"full format (dserver://{server}/backend/bucket/)"
            )
        elif len(path_parts) == 2:
            # Full format: backend/bucket
            backend = path_parts[0]
            bucket = path_parts[1]
            backend_base_uri = f"{backend}://{bucket}"
        else:
            raise ValueError(f"Invalid base URI format: {base_uri}")

        # Get token (check config file as well as environment)
        token = get_config_value('DSERVER_TOKEN', config_path=config_path)
        if not token:
            token_file = get_config_value('DSERVER_TOKEN_FILE', config_path=config_path)
            if token_file and os.path.exists(token_file):
                with open(token_file) as f:
                    token = f.read().strip()

        if not token:
            raise DServerAuthenticationError(
                "No dserver token found. Set DSERVER_TOKEN in environment or config file."
            )

        protocol = get_config_value('DSERVER_PROTOCOL', config_path=config_path, default='http')

        # Query dserver for URIs
        response = requests.post(
            f"{protocol}://{server}/uris",
            headers={'Authorization': f'Bearer {token}'},
            json={'base_uris': [backend_base_uri]}
        )
        response.raise_for_status()

        # Convert backend URIs to dserver URIs using shortest format
        uris = []
        default_base_uri = cls._get_default_base_uri_for_server(server, config_path)

        for item in response.json():
            backend_uri = item['uri']
            uuid = backend_uri.split('/')[-1]
            item_base_uri = backend_uri.rsplit('/', 1)[0]

            # Generate shortest URI format
            if default_base_uri and item_base_uri == default_base_uri:
                # Use short format
                dserver_uri = f"dserver://{server}/{uuid}"
            else:
                # Use full format
                parsed_backend = urlparse(backend_uri)
                backend_type = parsed_backend.scheme
                bucket = parsed_backend.netloc
                dserver_uri = f"dserver://{server}/{backend_type}/{bucket}/{uuid}"

            uris.append(dserver_uri)

        return uris

    @classmethod
    def generate_uri(cls, name, uuid, base_uri, config_path=None):
        """Generate URI for a new dataset.

        Generates the shortest valid URI based on configuration.
        """
        parsed = urlparse(base_uri)
        server = parsed.netloc

        # Parse the base_uri to determine backend base URI
        path_parts = [p for p in parsed.path.strip('/').split('/') if p]

        if len(path_parts) == 0:
            # Base URI is dserver://server/ - use default
            backend_base_uri = cls._get_default_base_uri_for_server(server, config_path)
            if not backend_base_uri:
                raise ValueError(
                    f"Cannot generate URI from dserver://{server}/ without "
                    "DSERVER_DEFAULT_BASE_URI configuration"
                )
            # Use short format
            return f"dserver://{server}/{uuid}"
        elif len(path_parts) == 1:
            # Single path component - reject as ambiguous
            raise ValueError(
                f"Invalid base URI: {base_uri}. "
                f"Use short format (dserver://{server}/) or "
                f"full format (dserver://{server}/backend/bucket/)"
            )
        elif len(path_parts) == 2:
            # Full format: backend/bucket
            backend = path_parts[0]
            bucket = path_parts[1]
            backend_base_uri = f"{backend}://{bucket}"

            # Check if this matches the default - if so, use short format
            default_base_uri = cls._get_default_base_uri_for_server(server, config_path)
            if default_base_uri and backend_base_uri == default_base_uri:
                return f"dserver://{server}/{uuid}"

            # Otherwise use full format
            return f"dserver://{server}/{backend}/{bucket}/{uuid}"
        else:
            raise ValueError(f"Invalid base URI format: {base_uri}")
