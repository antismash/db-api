#!/usr/bin/env python3

import sys

from api import app

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        output_dir = sys.argv[1]
        print(" * Serving files from", output_dir)
        app.config["OUTPUT_FOLDER"] = output_dir
    app.run(debug=True, host='::', port=5566)
