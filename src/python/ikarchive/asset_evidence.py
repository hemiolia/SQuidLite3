"""画像URLの取得状態と、保存済み本文参照の限定的な証拠集計。"""

from urllib.parse import urlsplit


_KNOWN_STATES = ("pending", "retry", "done")
_ASSET_URL_EXPIRED = "AssetUrlExpired"


def _address(url):
    """Return the URL address tuple, or None when it cannot be compared safely.

    Query and fragment are intentionally excluded. The path is kept exactly as
    parsed: no decoding, case folding, slash collapsing, or other normalization.
    """
    if type(url) is not str or not url or any(ord(char) <= 0x20 or ord(char) == 0x7F for char in url):
        return None
    try:
        parsed = urlsplit(url)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    if (not scheme or not parsed.netloc or not hostname
            or parsed.username is not None or parsed.password is not None
            or "\\" in parsed.netloc):
        return None
    if port is None:
        if scheme == "https":
            port = 443
        elif scheme == "http":
            port = 80
    return scheme, hostname.lower(), port, parsed.path


def asset_acquisition_evidence(db):
    """Count asset URL rows and indexed body references without reading payloads.

    This function executes SELECTs only and does not begin, finish, or roll back
    a transaction. It examines ``assets`` rows and looks up ``bodies.sha256``
    through its primary key. It never reads ``bodies.body`` or ``asset_refs``.
    A matching body row proves only that the indexed reference exists; the BLOB
    digest and stored byte length are not revalidated here.
    """
    state_counts = {state: 0 for state in _KNOWN_STATES}
    state_counts["other"] = 0
    url_rows = []
    body_backed_done_addresses = set()
    done_body_present_rows = 0
    done_body_missing_rows = 0
    unsafe_address_rows = 0
    expired_url_rows = 0
    expired_unsafe_rows = 0
    expired_addresses = set()
    expired_with_saved_same_address = 0
    expired_without_saved_same_address = 0

    # Select only URL and acquisition metadata plus the bodies primary key.
    # The body payload is intentionally absent from this query.
    for url, state, last_error, body_sha256, body_record_sha256 in db.execute(
        """SELECT a.url, a.state, a.last_error, a.body_sha256, b.sha256
           FROM assets AS a
           LEFT JOIN bodies AS b ON b.sha256 = a.body_sha256"""
    ):
        state_key = state if type(state) is str and state in _KNOWN_STATES else "other"
        state_counts[state_key] += 1
        address = _address(url)
        if address is None:
            unsafe_address_rows += 1
        if state == "done" and body_sha256 is not None and body_record_sha256 == body_sha256:
            done_body_present_rows += 1
            if address is not None:
                body_backed_done_addresses.add(address)
        elif state == "done":
            done_body_missing_rows += 1
        is_expired = last_error == _ASSET_URL_EXPIRED and state != "done"
        if is_expired:
            expired_url_rows += 1
            if address is None:
                expired_unsafe_rows += 1
            else:
                expired_addresses.add(address)
        url_rows.append((is_expired, address))

    # The completed-address set may gain entries later in the SQL cursor, so
    # classify expired URLs only after all assets have been examined.
    for is_expired, address in url_rows:
        if not is_expired or address is None:
            continue
        if address in body_backed_done_addresses:
            expired_with_saved_same_address += 1
        else:
            expired_without_saved_same_address += 1

    return {
        "asset_url_rows": sum(state_counts.values()),
        "state_url_rows": state_counts,
        "done_body_reference_present_url_rows": done_body_present_rows,
        "done_body_reference_missing_url_rows": done_body_missing_rows,
        "expired_url_rows": expired_url_rows,
        "expired_urls_with_saved_same_address": expired_with_saved_same_address,
        "expired_urls_without_saved_same_address": expired_without_saved_same_address,
        "distinct_expired_addresses": len(expired_addresses),
        "unsafe_address_url_rows": unsafe_address_rows,
        "expired_unsafe_address_url_rows": expired_unsafe_rows,
        "historical_image_bytes_equivalence_verified": False,
        "all_server_images_verified": False,
        "limit": (
            "Counts describe assets URL rows and indexed body references only. "
            "asset_refs edges are not read or counted. Address matching uses "
            "lowercase scheme and hostname, effective HTTP(S) port, and the "
            "unchanged parsed path; query and fragment are ignored. A shared "
            "address does not prove equal bytes, generation, or transformation. "
            "A matching bodies.sha256 row is not a BLOB digest or byte-length "
            "verification. This does not verify all server images or historical equivalence."
        ),
    }
