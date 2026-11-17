#!/usr/bin/env python3
"""Run the repcache scraping drill against the local bilingual service."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from repcache.scrape_demo import main

if __name__ == "__main__":
    main()
