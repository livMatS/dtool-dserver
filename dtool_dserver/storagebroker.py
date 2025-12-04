"""Storage broker that accesses datasets through dserver signed URLs."""

import json
import logging
import os
import tempfile
from urllib.parse import urlparse, quote

import requests

from dtoolcore.storagebroker import BaseStorageBroker
from dtoolcore.filehasher import FileHasher, md5sum_hexdigest
from dtoolcore.utils import generate_identifier, mkdir_parents


logger = logging.getLogger(__name__)


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

    URI format: dserver://<server>/<backend>/<bucket>/<uuid>
    Example: dserver://localhost:5000/s3/my-bucket/uuid
    """

    #: Attribute used to define the type of storage broker.
    key = "dserver"

    #: Attribute used by :class:`dtoolcore.ProtoDataSet` to write the hash
    #: function name to the manifest.
    hasher = FileHasher(md5sum_hexdigest)

    def __init__(self, uri, config_path=None):
        self._uri = uri
        self._config_path = config_path
        self._parse_uri(uri)

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
        """Parse dserver URI into components.

        URI format: dserver://<server>/<backend>/<bucket>/<uuid>
        """
        parsed = urlparse(uri)

        if parsed.scheme != 'dserver':
            raise ValueError(f"Invalid scheme: {parsed.scheme} (expected 'dserver')")

        self._server = parsed.netloc
        if not self._server:
            raise ValueError("Missing server in URI")

        path_parts = parsed.path.strip('/').split('/')
        if len(path_parts) < 2:
            raise ValueError(
                f"Invalid dserver URI: {uri}. "
                "Expected format: dserver://<server>/<backend>/<bucket>[/<uuid>]"
            )

        self._backend = path_parts[0]  # e.g., "s3" or "azure"
        self._bucket = path_parts[1]

        if len(path_parts) >= 3:
            self.uuid = path_parts[2]
        else:
            self.uuid = None

        # Reconstruct the backend URI
        self._backend_base_uri = f"{self._backend}://{self._bucket}"
        if self.uuid:
            self._backend_uri = f"{self._backend_base_uri}/{self.uuid}"
        else:
            self._backend_uri = None

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
            if self._backend_uri is None:
                raise ValueError("No dataset UUID set")

            encoded_uri = quote(self._backend_uri, safe='')
            response = self._api_request(
                'GET',
                f'/signed-urls/dataset/{encoded_uri}'
            )
            self._signed_urls_cache = response.json()
            logger.debug(f"Fetched signed URLs for {self._backend_uri}")
        return self._signed_urls_cache

    def _fetch_upload_urls(self, items):
        """Fetch signed URLs for uploading a dataset."""
        if self._upload_urls_cache is None:
            if self.uuid is None:
                raise ValueError("No dataset UUID set")

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
            logger.debug(f"Fetched upload URLs for {self._backend_uri}")
        return self._upload_urls_cache

    # === Read Operations ===

    def has_admin_metadata(self):
        """Check if dataset exists by attempting to fetch admin metadata."""
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
        """Get size of item from manifest."""
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('size_in_bytes', 0)

    def get_utc_timestamp(self, handle):
        """Get timestamp of item from manifest."""
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('utc_timestamp', 0)

    def get_hash(self, handle):
        """Get hash of item from manifest."""
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('hash', '')

    def get_relpath(self, handle):
        """Get relpath of item from manifest."""
        identifier = generate_identifier(handle)
        manifest = self.get_manifest()
        item = manifest.get('items', {}).get(identifier, {})
        return item.get('relpath', handle)

    # === Write Operations ===

    def _create_structure(self):
        """Initialize dataset structure (for new datasets)."""
        # Structure will be created during post_freeze_hook upload
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
        self._api_request(
            'POST',
            '/signed-urls/upload-complete',
            json={'uri': self._backend_uri}
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
    def list_dataset_uris(cls, base_uri, config_path=None):
        """List datasets by querying dserver API."""
        parsed = urlparse(base_uri)
        server = parsed.netloc
        path_parts = parsed.path.strip('/').split('/')

        if len(path_parts) < 2:
            raise ValueError(f"Invalid base URI: {base_uri}")

        backend = path_parts[0]
        bucket = path_parts[1]

        backend_base_uri = f"{backend}://{bucket}"

        # Get token
        token = os.environ.get('DSERVER_TOKEN')
        if not token:
            token_file = os.environ.get('DSERVER_TOKEN_FILE')
            if token_file and os.path.exists(token_file):
                with open(token_file) as f:
                    token = f.read().strip()

        if not token:
            raise DServerAuthenticationError("No dserver token found")

        protocol = os.environ.get('DSERVER_PROTOCOL', 'http')

        # Query dserver for URIs
        response = requests.post(
            f"{protocol}://{server}/uris",
            headers={'Authorization': f'Bearer {token}'},
            json={'base_uris': [backend_base_uri]}
        )
        response.raise_for_status()

        # Convert backend URIs to dserver URIs
        uris = []
        for item in response.json():
            backend_uri = item['uri']
            uuid = backend_uri.split('/')[-1]
            dserver_uri = f"dserver://{server}/{backend}/{bucket}/{uuid}"
            uris.append(dserver_uri)

        return uris

    @classmethod
    def generate_uri(cls, name, uuid, base_uri):
        """Generate URI for a new dataset."""
        parsed = urlparse(base_uri)
        server = parsed.netloc
        path = parsed.path.strip('/')
        return f"dserver://{server}/{path}/{uuid}"
