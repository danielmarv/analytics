"""Shared chart styling constants for the analytics plotting layer."""

from __future__ import annotations

# --------------------------------------------------
# Figure configuration
# --------------------------------------------------
DEFAULT_DPI: int = 300
DEFAULT_FIGSIZE: tuple[int, int] = (12, 7)

# --------------------------------------------------
# Base style
# --------------------------------------------------
DEFAULT_STYLE: str = "default"
FONT_FAMILY: str = "DejaVu Sans"
TITLE_FONT_SIZE: int = 16
LABEL_FONT_SIZE: int = 11
TICK_FONT_SIZE: int = 10
LEGEND_FONT_SIZE: int = 10
ANNOTATION_FONT_SIZE: int = 10
CENTER_TOTAL_FONT_SIZE: int = 20
FONT_WEIGHT_SEMIBOLD: str = "semibold"

# --------------------------------------------------
# Surface + typography colors
# --------------------------------------------------
FIGURE_BACKGROUND_COLOR = "#F6F8FB"
PLOT_BACKGROUND_COLOR = "#FFFFFF"
TITLE_COLOR = "#0F172A"
TEXT_COLOR = "#334155"
MUTED_TEXT_COLOR = "#64748B"
AXIS_LINE_COLOR = "#D7E0EA"
CARD_BORDER_COLOR = "#DCE5EF"

# --------------------------------------------------
# Grid styling
# --------------------------------------------------
GRID_ENABLED: bool = True
GRID_ALPHA: float = 1.0
GRID_STYLE: str = "-"
GRID_COLOR: str = "#E8EEF5"
GRID_LINE_WIDTH: float = 0.8

# --------------------------------------------------
# Legend styling
# --------------------------------------------------
LEGEND_BACKGROUND_COLOR = "#FFFFFF"
LEGEND_EDGE_COLOR = "#E2E8F0"
LEGEND_BOX_STYLE = "round,pad=0.35,rounding_size=1.4"

# --------------------------------------------------
# Annotation styling
# --------------------------------------------------
ENDPOINT_LABEL_BOX_STYLE = "round,pad=0.28,rounding_size=0.8"

# --------------------------------------------------
# Donut / pie styling
# --------------------------------------------------
DONUT_START_ANGLE = 110
DONUT_RADIUS = 0.92
DONUT_WIDTH = 0.34
DONUT_PERCENTAGE_DISTANCE = 0.8
DONUT_EDGE_LINE_WIDTH = 2.0

# --------------------------------------------------
# Accent palette for charts without a domain-specific color mapping
# --------------------------------------------------
PRIMARY_PALETTE = [
    "#F97316",
    "#14B8A6",
    "#0EA5E9",
    "#F59E0B",
    "#EF4444",
]

# Preserve the original domain colors for the analytics charts that already
# have established meaning in project discussions and screenshots.
DIFFICULTY_COLORS = {
    "Advanced": "#E78AC3",
    "Intermediate": "#FFD92F",
    "Beginner": "#8DA0CB",
    "Good First Issue": "#66C2A5",
    "Unknown": "#B3B3B3",
}

# Onboarding charts already use these colors across the existing exports.
ONBOARDING_COLORS = {
    "Good First Issues": "#2E749F",
    "Good First Issue Candidates": "#D8A251",
}

# State lines keep their original semantic mapping as well.
STATE_COLORS = {
    "total": "#3D3D3D",
    "closed": "#28A197",
    "open": "#F46A25",
}

RESPONSIBILITY_COLORS = {
    "General users": "#94A3B8",
    "Triage contributors": "#0EA5E9",
    "Maintainers": "#F97316",
}
