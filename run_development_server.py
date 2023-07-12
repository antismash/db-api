#!/usr/bin/env python
from api import app

if __name__ == "__main__":
    app.run(debug=True, host='::', port=5566)
