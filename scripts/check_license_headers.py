#!/usr/bin/env python3
# Copyright 2026 Ori Nexus Systems LTD
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import sys
from pathlib import Path

COPYRIGHT = "Copyright 2026 Ori Nexus Systems LTD"
SPDX = "SPDX-License-Identifier: Apache-2.0"
ROOTS = {"cmd", "internal"}

missing: list[str] = []
for path in Path(".").rglob("*.go"):
    if not path.parts or path.parts[0] not in ROOTS:
        continue
    text = path.read_text()
    if COPYRIGHT not in text or SPDX not in text:
        missing.append(str(path))

if missing:
    print("Missing required license header:")
    for path in missing:
        print(f"  {path}")
    raise SystemExit(1)
