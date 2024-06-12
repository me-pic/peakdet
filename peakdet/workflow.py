#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
`peakdet` main workflow and related functions.
"""
import datetime
import json
import logging
import os
import sys
from pathlib import Path

import pandas as pd

from peakdet import Physio, _version, save_history, save_physio
from peakdet.blocks import manual_peaks, process_signals
from peakdet.cli.run import _get_parser
from peakdet.utils import find_chtrig

LGR = logging.getLogger(__name__)
LGR.setLevel(logging.INFO)


def save_bash_call(filename, outdir, outfile):

    if outdir is None:
        if outfile is None:
            if len(filename) == 1:
                outdir = os.path.dirname(filename[0])
            else:
                outdir = os.path.commonpath(filename)
        else:
            outdir = os.path.split(outfile)[0]

        if outdir == "" or outdir == "/":
            outdir = "."
        outdir = os.path.join(outdir, "peakdet")

    outdir = os.path.abspath(outdir)
    log_path = os.path.join(outdir, "logs")
    os.makedirs(log_path, exist_ok=True)
    arg_str = " ".join(sys.argv[1:])
    call_str = f"peakdet {arg_str}"
    outdir = os.path.abspath(outdir)
    log_path = os.path.join(outdir, "logs")
    os.makedirs(log_path, exist_ok=True)
    isotime = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
    f = open(os.path.join(log_path, f"peakdet_call_{isotime}.sh"), "a")
    f.write(f"#!bin/bash \n{call_str}")
    f.close()


def peakdet(
    filename,
    config,
    indir=".",
    outfile=None,
    outdir=".",
    phys_idx=None,
    chtrig=None,
    manual_detector=True,
    lgr_degree="info",
):
    """
    filename : str
        The name of the file containing the physiological data
    config : str
        Path to config file specifying the processing steps for each modality. For config
        file examples, check `peakdet/configs/`
    indir : str
        Folder containing input
    outfile : str
        Path to the output file - or just its full name. If an extension is *not* declared,
        the program will automatically append .phys to the specified name. It is *not* necessary
        to declare both this and `outdir` - the full path can be specified here.
    outdir : str
        Path to the output folder. If it doesn't exist, it will be created.
        If both `outdir` and `outfile` are declared, `outdir` overrides the path
        specified in `outfile` (but not the filename!)
    phys_idx : int or list of int
        Index(es) of the column(s) in the filename containing the timeserie to clean and process.
        If None, the workflow will go through all the columns of the filename file in `source`.
        If you run the workflow on Phys2Bids outputs, please keep in mind the channel 0 is the time.
    chtrig :
        The column number of the trigger channel. Default is None. If chtrig is left as None peakdet will
        perform an automatic trigger channel search by channel names.
    manual_detector : bool
        Flag for manual peaks check. Default to True.
    lgr_degree : 'debug', 'info', or 'quiet', optional
        The degree of verbosity of the logger. Default is 'info'.
    """
    # Prepare folders
    if outdir is None:
        if outfile is None:
            outdir = os.path.commonpath(filename)
        else:
            outdir = os.path.split(outfile)[0]

        if outdir == "" or outdir == "/":
            outdir = "."
        outdir = os.path.join(outdir, "peakdet")

    outdir = os.path.abspath(outdir)
    outdir = Path(outdir)
    log_path = outdir / "logs"
    os.makedirs(log_path, exist_ok=True)

    # Create logfile name
    basename = "peakdet_"
    extension = "tsv"
    isotime = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S")
    logname = os.path.join(log_path, f"{basename}{isotime}.{extension}")

    # Set logging format
    log_formatter = logging.Formatter(
        "%(asctime)s\t%(name)-12s\t%(levelname)-8s\t%(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # Set up logging file and open it for writing
    log_handler = logging.FileHandler(logname)
    log_handler.setFormatter(log_formatter)
    sh = logging.StreamHandler()

    if lgr_degree == "quiet":
        logging.basicConfig(
            level=logging.WARNING,
            handlers=[log_handler, sh],
            format="%(levelname)-10s %(message)s",
        )
    elif lgr_degree == "debug":
        logging.basicConfig(
            level=logging.DEBUG,
            handlers=[log_handler, sh],
            format="%(levelname)-10s %(message)s",
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            handlers=[log_handler, sh],
            format="%(levelname)-10s %(message)s",
        )

    version_number = _version.get_versions()["version"]
    LGR.info(f"Currently running peakdet version {version_number}")

    # Checks arguments (e.g. config file structure)
    # TODO

    # Load config file
    with open(config) as c:
        config = json.load(c)
        c.close()

    # Load data
    indir = Path(indir)
    filename = Path(filename)
    with open(str(indir / filename).rstrip("".join(filename.suffixes)) + ".json") as p:
        info = json.load(p)
        p.close()
    data = pd.read_csv(indir / filename, names=info["Columns"], sep="\t")

    # If phys_idx not None, keep only the specified columns
    if phys_idx is not None:
        data = pd.DataFrame(data.iloc[:, phys_idx])
    else:
        # Remove time and trigger columns
        if chtrig != 0:
            chtime = data.columns.get_loc("time")
            data.drop(data.columns[chtime, chtrig], axis=1, inplace=True)
        else:
            # find automatically trigger channel index
            LGR.info("Running automatic trigger detection.")
            chtrig = find_chtrig(data)
            if chtrig is not None:
                data.drop(data.columns[[chtime, chtrig]], axis=1, inplace=True)
            else:
                LGR.warning(
                    "No trigger channel specified nor found, the workflow will be run on all columns of the dataframe"
                )
                data.drop(data.columns[chtime], axis=1, inplace=True)

    # Looping through timeseries to clean and process each modality one at a time
    for idx, col in enumerate(data.columns):
        # Get the sampling frequency
        if isinstance(info["SamplingFrequency"], (list)):
            fs = info["SamplingFrequency"][idx]
        else:
            fs = info["SamplingFrequency"]
        # Create Physio obj
        physio_obj = Physio(data[col], fs=fs)
        # Call process_signals specifying the processing steps for the given modality
        physio_obj = process_signals(physio_obj, config[col])

        # Call manual_peaks function if manual_detector flag set to True
        if manual_detector:
            # Perform manual peaks detection and saving output
            manual_peaks(
                physio_obj, os.path.join(outdir, str(outfile + f"_{col}.phys"))
            )
        else:
            # Save outputs
            save_physio(os.path.join(outdir, str(outfile + f"_{col}")), physio_obj)
            save_history(
                os.path.join(outdir, str(outfile + f"_history_{col}")), physio_obj
            )

    LGR.info(f"peakdet finished! Check results in {outdir}.")


def _main(argv=None):
    options = _get_parser().parse_args(argv)

    save_bash_call(options.filename, options.outdir, options.outfile)

    peakdet(**vars(options))


if __name__ == "__main__":
    _main(sys.argv[1:])
