# -*- coding: utf-8 -*-
import glob
import os
import warnings

import matplotlib

matplotlib.use("WXAgg")
import argparse

import peakdet

LOADERS = dict(rtpeaks=peakdet.load_rtpeaks, MRI=peakdet.load_physio)

MODALITIES = dict(
    ECG=([5.0, 15.0], "bandpass"), PPG=(2, "lowpass"), RESP=([0.05, 0.5], "bandpass")
)

ATTR_CONV = {
    "Average NN intervals": "avgnn",
    "Root mean square of successive differences": "rmssd",
    "Standard deviation of NN intervals": "sdnn",
    "Standard deviation of successive differences": "sdsd",
    "Number of successive differences >50 ms": "nn50",
    "Percent of successive differences >50 ms": "pnn50",
    "Number of successive differences >20 ms": "nn20",
    "Percent of successive differences >20 ms": "pnn20",
    "High frequency HRV hfHRV": "hf",
    "Log of high frequency HRV, log(hfHRV)": "hf_log",
    "Low frequency HRV, lfHRV": "lf",
    "Log of low frequency HRV, log(lfHRV)": "lf_log",
    "Very low frequency HRV, vlfHRV": "vlf",
    "Log of very low frequency HRV, log(vlfHRV)": "vlf_log",
    "Ratio of lfHRV : hfHRV": "lftohf",
    "Peak frequency of hfHRV": "hf_peak",
    "Peak frequency of lfHRV": "lf_peak",
}


def _get_parser():
    """Parser for GUI and command-line arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-in",
        "--input-file",
        dest="filename",
        help="Path to the physiological data file",
        required=True,
    )
    parser.add_argument(
        "-config" "--config-file",
        dest="config",
        type=str,
        help="Path to config file specifying the processing steps for each modality.",
        required=True,
    )
    parser.add_argument(
        "-indir", "--input-dir", dest="indir", type=str, help="", default="."
    )
    parser.add_argument(
        "-outfile" "--output-file",
        dest="outfile",
        default=None,
        help="Path to the output file - or just its full name. If an extension is *not* declared, "
        "the program will automatically append .phys to the specified name.",
    )
    parser.add_argument(
        "-outdir",
        "--output-dir",
        dest="outdir",
        default=".",
        help="Path to the output folder. If it does not exist, it will be created.",
    )
    parser.add_argument(
        "-phys",
        "--phys-idx",
        dest="phys_idx",
        default=None,
        type=int,
        help="Index(es) of the column(s) in the fname containing the timeserie to clean and process."
        "If None, the workflow will go through all the columns of the fname file in `source`. "
        "If you run the workflow on Phys2Bids outputs, please keep in mind the channel 0 is the time.",
    )
    parser.add_argument(
        "-chtrig",
        "--channel-trigger",
        dest="chtrig",
        default=0,
        type=int,
        help="The column number of the trigger channel. Default is None. If chtrig is left as None peakdet will "
        "perform an automatic trigger channel search by channel names.",
    )
    parser.add_argument(
        "-detector" "--manual-detector",
        dest="manual_detector",
        action="store_true",
        default=False,
        help="Flag for manual peaks check. Default is False.",
    )
    parser.add_argument(
        "-lgr",
        "--lgr-degree",
        dest="lgr_degree",
        default="info",
        choices=["debug", "info", "quiet"],
        help="The degree of verbosity of the logger. Default is `info`.",
    )

    return parser


def workflow(
    *,
    file_template,
    modality,
    fs,
    source="MRI",
    channel=1,
    output="peakdet.csv",
    savehistory=True,
    noedit=False,
    thresh=0.2,
    measurements=ATTR_CONV.keys()
):
    """
    Basic workflow for physiological data

    Parameters
    ----------
    file_template : str
        Template filename for data inputs
    modality : {'ECG', 'PPG', 'RESP'}
        Currently support data modalities
    fs : float
        Sampling rate of input data
    source : {'rtpeaks', 'MRI'}, optional
        How data were acquired. Default: 'MRI'
    channel : int, optional
        Which channel of data to analyze; only applies if source is 'rtpeaks'.
        Default: 1
    output : str, optional
        Desired output filename. Default: 'peakdet.csv'
    savehistory : bool, optional
        Whether to save editing history of each file with
        ``peakdet.save_history``. History will be used if this workflow is
        run again on the samed data files. Default: True
    noedit : bool, optional
        Whether to disable interactive editing of physio data. Default: False
    thresh : [0, 1] float, optional
        Threshold for peak detection. Default: 0.2
    measurements : list, optional
        Which HRV-related measurements to save from data. See ``peakdet.HRV``
        for available measurements. Default: all available measurements.
    """

    # output file
    print("OUTPUT FILE:\t\t{}\n".format(output))
    # grab files from file template
    print("FILE TEMPLATE:\t{}\n".format(file_template))
    files = glob.glob(file_template, recursive=True)

    # convert measurements to peakdet.HRV attribute friendly names
    try:
        print("REQUESTED MEASUREMENTS: {}\n".format(", ".join(measurements)))
    except TypeError:
        raise TypeError(
            "It looks like you didn't select any of the options "
            "specifying desired output measurements. Please "
            "select at least one measurement and try again."
        )
    measurements = [ATTR_CONV[attr] for attr in measurements]

    # get appropriate loader
    load_func = LOADERS[source]

    # check if output file exists -- if so, ensure headers will match
    head = "filename," + ",".join(measurements)
    if os.path.exists(output):
        with open(output, "r") as src:
            eheader = src.readlines()[0]
        # if existing output file does not have same measurements are those
        # requested on command line, warn and use existing measurements so
        # as not to totally fork up existing file
        if eheader != head:
            warnings.warn(
                "Desired output file already exists and requested "
                "measurements do not match with measurements in "
                "existing output file. Using the pre-existing "
                "measurements, instead."
            )
            measurements = [f.strip() for f in eheader.split(",")[1:]]
        head = ""
    # if output file doesn't exist, nbd
    else:
        head += "\n"

    with open(output, "a+") as dest:
        dest.write(head)
        # iterate through all files and do peak detection with manual editing
        for fname in files:
            fname = os.path.relpath(fname)
            print("Currently processing {}".format(fname))

            # if we want to save history, this is the output name it would take
            outname = os.path.join(
                os.path.dirname(fname), "." + os.path.basename(fname) + ".json"
            )

            # let's check if history already exists and load that file, if so
            if os.path.exists(outname):
                data = peakdet.load_history(outname)
            else:
                # load data with appropriate function, depending on source
                if source == "rtpeaks":
                    data = load_func(fname, fs=fs, channel=channel)
                else:
                    data = load_func(fname, fs=fs)

                # filter
                flims, method = MODALITIES[modality]
                data = peakdet.filter_physio(data, cutoffs=flims, method=method)

                # perform peak detection
                data = peakdet.peakfind_physio(data, thresh=thresh)

            # edit peaks, if desired (HIGHLY RECOMMENDED)
            # we'll do this even if we loaded from history
            # just to give another chance to check things over
            if not noedit:
                data = peakdet.edit_physio(data)

            # save back out to history, if desired
            if savehistory:
                peakdet.save_history(outname, data)

            # keep requested outputs
            hrv = peakdet.HRV(data)
            outputs = ["{:.5f}".format(getattr(hrv, attr, "")) for attr in measurements]

            # save as we go so that interruptions don't screw everything up
            dest.write(",".join([fname] + outputs) + "\n")


if __name__ == "__main__":
    raise RuntimeError(
        "peakdet/cli/run.py should not be run directly;\n"
        "Please `pip install` peakdet and use the "
        "`peakdet` command"
    )
