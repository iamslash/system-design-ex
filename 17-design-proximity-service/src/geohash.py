"""Geohash encode/decode with neighbor finding.

Geohash is a hierarchical spatial indexing system that encodes latitude and
longitude into a short alphanumeric string.  Each additional character narrows
the bounding box, giving configurable precision.
"""

from __future__ import annotations

# Base-32 alphabet used by the standard geohash scheme.
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_DECODE_MAP: dict[str, int] = {c: i for i, c in enumerate(_BASE32)}



def encode(lat: float, lng: float, precision: int = 6) -> str:
    """Encode latitude/longitude into a geohash string.

    Args:
        lat: Latitude in degrees (-90 to 90).
        lng: Longitude in degrees (-180 to 180).
        precision: Number of characters in the resulting geohash (1-12).

    Returns:
        Geohash string of the requested precision.

    Raises:
        ValueError: If coordinates are out of range or precision is invalid.
    """
    if not 1 <= precision <= 12:
        raise ValueError(f"Precision must be between 1 and 12, got {precision}")
    if not -90.0 <= lat <= 90.0:
        raise ValueError(f"Latitude must be between -90 and 90, got {lat}")
    if not -180.0 <= lng <= 180.0:
        raise ValueError(f"Longitude must be between -180 and 180, got {lng}")

    lat_range = (-90.0, 90.0)
    lng_range = (-180.0, 180.0)
    is_lng = True  # Start with longitude bit
    bit = 0
    ch_idx = 0
    geohash: list[str] = []

    while len(geohash) < precision:
        if is_lng:
            mid = (lng_range[0] + lng_range[1]) / 2
            if lng >= mid:
                ch_idx = ch_idx * 2 + 1
                lng_range = (mid, lng_range[1])
            else:
                ch_idx = ch_idx * 2
                lng_range = (lng_range[0], mid)
        else:
            mid = (lat_range[0] + lat_range[1]) / 2
            if lat >= mid:
                ch_idx = ch_idx * 2 + 1
                lat_range = (mid, lat_range[1])
            else:
                ch_idx = ch_idx * 2
                lat_range = (lat_range[0], mid)

        is_lng = not is_lng
        bit += 1

        if bit == 5:
            geohash.append(_BASE32[ch_idx])
            bit = 0
            ch_idx = 0

    return "".join(geohash)


def decode(geohash: str) -> tuple[float, float]:
    """Decode a geohash string back to latitude/longitude (center of cell).

    Args:
        geohash: A valid geohash string.

    Returns:
        (latitude, longitude) tuple representing the center of the cell.

    Raises:
        ValueError: If the geohash contains invalid characters.
    """
    if not geohash:
        raise ValueError("Geohash string must not be empty")

    lat_range = (-90.0, 90.0)
    lng_range = (-180.0, 180.0)
    is_lng = True

    for ch in geohash.lower():
        if ch not in _DECODE_MAP:
            raise ValueError(f"Invalid geohash character: {ch!r}")
        val = _DECODE_MAP[ch]
        for bit_pos in (16, 8, 4, 2, 1):
            if is_lng:
                mid = (lng_range[0] + lng_range[1]) / 2
                if val & bit_pos:
                    lng_range = (mid, lng_range[1])
                else:
                    lng_range = (lng_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if val & bit_pos:
                    lat_range = (mid, lat_range[1])
                else:
                    lat_range = (lat_range[0], mid)
            is_lng = not is_lng

    lat = (lat_range[0] + lat_range[1]) / 2
    lng = (lng_range[0] + lng_range[1]) / 2
    return (lat, lng)


def bounding_box(geohash: str) -> tuple[float, float, float, float]:
    """Return the bounding box of a geohash cell.

    Args:
        geohash: A valid geohash string.

    Returns:
        (min_lat, min_lng, max_lat, max_lng) tuple.
    """
    if not geohash:
        raise ValueError("Geohash string must not be empty")

    lat_range = (-90.0, 90.0)
    lng_range = (-180.0, 180.0)
    is_lng = True

    for ch in geohash.lower():
        if ch not in _DECODE_MAP:
            raise ValueError(f"Invalid geohash character: {ch!r}")
        val = _DECODE_MAP[ch]
        for bit_pos in (16, 8, 4, 2, 1):
            if is_lng:
                mid = (lng_range[0] + lng_range[1]) / 2
                if val & bit_pos:
                    lng_range = (mid, lng_range[1])
                else:
                    lng_range = (lng_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if val & bit_pos:
                    lat_range = (mid, lat_range[1])
                else:
                    lat_range = (lat_range[0], mid)
            is_lng = not is_lng

    return (lat_range[0], lng_range[0], lat_range[1], lng_range[1])


def neighbors(geohash: str) -> list[str]:
    """Find the 8 neighboring geohash cells.

    Uses an arithmetic approach: decode the cell center and bounding box,
    then offset by the cell dimensions to find the centers of the 8
    surrounding cells, and re-encode each.

    Returns geohashes in order: N, NE, E, SE, S, SW, W, NW.

    Args:
        geohash: Source geohash string.

    Returns:
        List of 8 neighbor geohash strings.
    """
    if not geohash:
        raise ValueError("Geohash string must not be empty")

    precision = len(geohash)
    min_lat, min_lng, max_lat, max_lng = bounding_box(geohash)
    lat_delta = max_lat - min_lat
    lng_delta = max_lng - min_lng
    center_lat = (min_lat + max_lat) / 2
    center_lng = (min_lng + max_lng) / 2

    # 8 direction offsets: (dlat, dlng) for N, NE, E, SE, S, SW, W, NW
    offsets = [
        (lat_delta, 0),           # N
        (lat_delta, lng_delta),   # NE
        (0, lng_delta),           # E
        (-lat_delta, lng_delta),  # SE
        (-lat_delta, 0),          # S
        (-lat_delta, -lng_delta), # SW
        (0, -lng_delta),          # W
        (lat_delta, -lng_delta),  # NW
    ]

    result: list[str] = []
    for dlat, dlng in offsets:
        nlat = center_lat + dlat
        nlng = center_lng + dlng

        # Clamp latitude to valid range.
        nlat = max(-89.999999, min(89.999999, nlat))
        # Wrap longitude to [-180, 180).
        if nlng > 180.0:
            nlng -= 360.0
        elif nlng < -180.0:
            nlng += 360.0

        result.append(encode(nlat, nlng, precision))

    return result


# Approximate cell dimensions for each precision level (in km).
PRECISION_TABLE: dict[int, tuple[float, float]] = {
    1: (5000.0, 5000.0),
    2: (1250.0, 625.0),
    3: (156.0, 156.0),
    4: (39.1, 19.5),
    5: (4.9, 4.9),
    6: (1.2, 0.61),
    7: (0.15, 0.15),
    8: (0.038, 0.019),
    9: (0.0048, 0.0048),
}


def precision_for_radius_km(radius_km: float) -> int:
    """Choose the smallest geohash precision whose cell covers the radius.

    Args:
        radius_km: Search radius in kilometers.

    Returns:
        Geohash precision level (1-9).
    """
    for prec in range(9, 0, -1):
        w, h = PRECISION_TABLE[prec]
        if w >= radius_km and h >= radius_km:
            return prec
    return 1
