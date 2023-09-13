#!/usr/bin/env python3

from argparse import ArgumentParser, SUPPRESS
import os

from api import app

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-o", "--output-dir", default=SUPPRESS, help="Directory to serve antiSMASH result files from")
    parser.add_argument("-j", "--job-dir", default=SUPPRESS, help="Directory to serve background job result files from")
    args = parser.parse_args()

    if "output_dir" in args:
        output_dir = os.path.abspath(args.output_dir)
        print(" * Serving antiSMASH files from", output_dir)
        app.config["OUTPUT_FOLDER"] = output_dir

    if "job_dir" in args:
        job_dir = os.path.abspath(args.job_dir)
        print(" * Serving job files from", job_dir)
        app.config["JOBS_FOLDER"] = job_dir

    app.run(debug=True, host='::', port=5566)
