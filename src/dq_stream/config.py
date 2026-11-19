"""
Loads config/feeds.yaml into small dataclasses.

Per-feed thresholds are merged on top of the defaults, so a feed only
has to list the values it wants to override.
"""
from dataclasses import dataclass, field
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = ROOT / "config" / "feeds.yaml"


@dataclass
class Thresholds:
    min_completeness_pct: float = 98.0
    max_error_rate_pct: float = 2.0
    max_p95_lag_s: float = 300
    volume_change_pct: float = 50


@dataclass
class FeedConfig:
    name: str
    topic: str
    required: list
    rules: dict = field(default_factory=dict)
    freshness_sla_s: int = 300
    thresholds: Thresholds = field(default_factory=Thresholds)


@dataclass
class PipelineConfig:
    window_seconds: int
    feeds: dict

    def feed_for_topic(self, topic):
        for feed in self.feeds.values():
            if feed.topic == topic:
                return feed
        raise KeyError(f"no feed configured for topic {topic}")


def load_config(path=DEFAULT_CONFIG):
    raw = yaml.safe_load(Path(path).read_text())
    defaults = raw.get("defaults", {})
    default_thresholds = defaults.get("thresholds", {})

    feeds = {}
    for name, f in raw["feeds"].items():
        if "required" not in f or "topic" not in f:
            raise ValueError(f"feed '{name}' needs at least 'topic' and 'required'")
        merged = {**default_thresholds, **f.get("thresholds", {})}
        feeds[name] = FeedConfig(
            name=name,
            topic=f["topic"],
            required=list(f["required"]),
            rules=f.get("rules", {}) or {},
            freshness_sla_s=int(f.get("freshness_sla_s", 300)),
            thresholds=Thresholds(**merged),
        )

    return PipelineConfig(window_seconds=int(defaults.get("window_seconds", 60)), feeds=feeds)
