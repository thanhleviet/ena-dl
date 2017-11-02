#! /usr/bin/env python

from __future__ import print_function
import json
import os
import subprocess
import sys
import time

ENA_URL = ('https://www.ebi.ac.uk/ena/data/warehouse/search?result=read_run&'
           'display=report')
FIELDS = [
    'study_accession', 'secondary_study_accession', 'sample_accession',
    'secondary_sample_accession', 'experiment_accession', 'run_accession',
    'submission_accession', 'tax_id', 'scientific_name',
    'instrument_platform', 'instrument_model', 'library_name',
    'library_layout', 'nominal_length', 'library_strategy',
    'library_source', 'library_selection', 'read_count',
    'base_count', 'center_name', 'first_public', 'last_updated',
    'experiment_title', 'study_title', 'study_alias', 'experiment_alias',
    'run_alias', 'fastq_bytes', 'fastq_md5', 'fastq_ftp', 'fastq_aspera',
    'fastq_galaxy', 'submitted_bytes', 'submitted_md5', 'submitted_ftp',
    'submitted_aspera', 'submitted_galaxy', 'submitted_format',
    'sra_bytes', 'sra_md5', 'sra_ftp', 'sra_aspera', 'sra_galaxy',
    'cram_index_ftp', 'cram_index_aspera', 'cram_index_galaxy',
    'sample_alias', 'broker_name', 'sample_title', 'nominal_sdev',
    'first_created'
]


def log_stdout(message, quiet=False):
    if not quiet:
        print(message, file=sys.stdout)


def run_command(cmd, stdout=subprocess.PIPE):
    """Execute a single command and return STDOUT and STDERR."""
    p = subprocess.Popen(cmd, stdout=stdout)

    return p.communicate()


def md5sum(file):
    """Return the MD5SUM of an input file."""
    stdout, stderr = run_command(['md5sum', file])
    if stdout:
        md5sum, filename = stdout.split()
        return md5sum
    else:
        return None


def download_fastq(fasp, ftp, outdir, md5, max_retry=10):
    """Download FASTQ from ENA using Apera Connect."""
    success = False
    use_ftp = False
    retries = 0
    fastq = '{0}/{1}'.format(
        outdir, format(os.path.basename(fasp))
    )

    if not os.path.exists(fastq):
        if not os.path.isdir(outdir):
            run_command(['mkdir', '-p', outdir])

        while not success:
            if use_ftp:
                run_command(['wget', '-O', fastq, ftp])
            else:
                run_command(['ena-ascp.sh', 'era-fasp@{0}'.format(fasp),
                             outdir])
            if md5sum(fastq) != md5:
                retries += 1
                os.remove(fastq)
                if retries > max_retry:
                    if not use_ftp:
                        use_ftp = True
                        retries = 0
                    else:
                        break
                time.sleep(10)
            else:
                success = True
    else:
        success = True

    return [success, fastq]


def merge_runs(runs, output):
    """Merge runs from an experiment."""
    if len(runs) > 1:
        cat_cmd = ['cat']
        rm_cmd = ['rm']
        for run in runs:
            cat_cmd.append(run)
            rm_cmd.append(run)
        print(cat_cmd)
        run_command(cat_cmd, stdout=output)
        run_command(rm_cmd)
    else:
        run_command(['mv', runs[0], output])


def get_run_info(experiment):
    """Retreive a list of unprocessed samples avalible from ENA."""
    import requests
    url = '{0}&query="{1}"&fields={2}'.format(ENA_URL, query, ",".join(FIELDS))
    r = requests.get(url)
    if r.status_code == requests.codes.ok:
        data = []
        col_names = None
        for line in r.text.split('\n'):
            cols = line.rstrip().split('\t')
            if line:
                if col_names:
                    data.append(dict(zip(col_names, cols)))
                else:
                    col_names = cols
        return data
    else:
        return False


def write_json(data, output):
    """Write input data structure to a json file."""
    with open(output, 'w') as fh:
        json.dump(data, fh, indent=4, sort_keys=True)


if __name__ == '__main__':
    import argparse as ap

    parser = ap.ArgumentParser(
        prog='ena-dl',
        conflict_handler='resolve',
        description=(''))
    group1 = parser.add_argument_group('Options', '')
    group1.add_argument('query', metavar="STRING", type=str,
                        help=('ENA accession to query. (Study, Experiment, or '
                              'Run accession)'))
    group1.add_argument('output', metavar="OUTPUT_DIR", type=str,
                        help=('Directory to output downloads to.'))
    group1.add_argument('--quiet', action='store_true', default=False,
                        help='Do not print current status.')
    group1.add_argument('--group_by_experiment', action='store_true',
                        default=False,
                        help='Group runs by experiment accession.')
    group1.add_argument('--group_by_sample', action='store_true',
                        default=False, help='Group runs by sample accession.')
    group1.add_argument('--is_study', action='store_true', default=False,
                        help='Query is a study accession.')
    group1.add_argument('--is_experiment', action='store_true', default=False,
                        help='Query is an experiment accession.')
    group1.add_argument('--is_run', action='store_true', default=False,
                        help='Query is a run accession.')
    group1.add_argument('--debug', action='store_true', default=False,
                        help='Skip downloads, print what will be downloaded.')

    args = parser.parse_args()
    query = None
    if args.is_study:
        query = 'study_accession={0}'.format(args.query)
    elif args.is_experiment:
        query = 'experiment_accession={0}'.format(args.query)
    elif args.is_run:
        query = 'run_accession={0}'.format(args.query)
    else:
        # Try to guess...
        if args.query[1:3] == 'RR':
            query = 'run_accession={0}'.format(args.query)
        elif args.query[1:3] == 'RX':
            query = 'experiment_accession={0}'.format(args.query)
        else:
            query = 'study_accession={0}'.format(args.query)

    ena_data = get_run_info(query)
    outdir = '{0}'.format(args.output)
    log_stdout('Query: {0}'.format(args.query), quiet=args.quiet)
    log_stdout('Total Runs To Download: {0}'.format(
        len(ena_data)),
        quiet=args.quiet
    )

    # FASTQ file names
    runs = None
    if args.group_by_experiment or args.group_by_sample:
        runs = {}
    for run in ena_data:
        log_stdout('\tWorking on run {0}...'.format(run['run_accession']),
                   quiet=args.quiet)
        aspera = run['fastq_aspera'].split(';')
        ftp = run['fastq_ftp'].split(';')
        md5 = run['fastq_md5'].split(';')

        for i in range(len(aspera)):
            # Download Run
            if md5[i] and not args.debug:
                success, fastq = download_fastq(aspera[i], ftp[i], outdir,
                                                md5[i])

                if success:
                    if args.group_by_experiment or args.group_by_sample:
                        name = run["sample_accession"]
                        if args.group_by_experiment:
                            name = run["experiment_accession"]

                        if name not in runs:
                            runs[name] = {'r1': [], 'r2': []}

                        if i:
                            runs[name]['r2'].append(fastq)
                        else:
                            runs[name]['r1'].append(fastq)
                else:
                    print(
                        "Failed to download matching files after 20 attempts "
                        "(10 via Aspera Connect, and 10 via FTP). Please try "
                        "again later or manually download from ENA."
                    )
                    sys.exit()

    # If applicable, merge runs
    if runs and not args.debug:
        for name, vals in runs.items():
            if len(vals['r1']) and len(vals['r2']):
                # Not all runs labled as paired are actually paired...
                log_stdout("\tMerging paired end runs to {0}...".format(name),
                           quiet=args.quiet)
                r1 = '{0}/{1}_R1.fastq.gz'.format(outdir, name)
                r2 = '{0}/{1}_R2.fastq.gz'.format(outdir, name)
                merge_runs(vals['r1'], r1)
                merge_runs(vals['r2'], r2)
            else:
                log_stdout("\tMerging single end runs to experiment...",
                           quiet=args.quiet)
                fastq = '{0}/{1}.fastq.gz'.format(outdir, name)
                merge_runs(vals['r1'], fastq)
        write_json(runs, "{0}/ena-run-mergers.json".format(outdir))
    write_json(ena_data, "{0}/ena-run-info.json".format(outdir))
