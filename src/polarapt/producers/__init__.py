"""Producers: fetch() -> (payload, meta). They never transform; raw stays raw."""
from . import bls, fx, usajobs

PRODUCERS = {"usajobs": usajobs, "bls": bls, "fx": fx}
