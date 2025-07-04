from dataclasses import dataclass
from typing import List, Set


@dataclass
class Area:
    code: str
    name: str
    level: str
    variants: Set[str]


@dataclass
class Ward(Area):
    pass


@dataclass
class District(Area):
    pass


@dataclass
class Province(Area):
    pass


@dataclass
class RawAddr:
    index: int
    content: str


@dataclass
class AddrMatch:
    raw_addr: RawAddr
    area: Area
    start_idx: int
    end_idx: int


@dataclass
class SubRawAddr:
    raw_addr: RawAddr
    start_idx: int
    end_idx: int


@dataclass
class CombinedRawAddr:
    content: str
    schema: List[SubRawAddr]
