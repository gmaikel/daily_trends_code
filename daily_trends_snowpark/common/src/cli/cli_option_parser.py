import argparse
from common.src.cli.job_config import JobConfig
from datetime import datetime


def parse_arguments():
    parser = argparse.ArgumentParser(description="spark job DSP")

    parser.add_argument('-s', '--source', required=True, help="Source DSP daily stats folder")
    parser.add_argument('-d', '--destination', required=True, help="Pivot destination folder")
    parser.add_argument('--distributor', required=True, help="Entity that sells musical content to store")
    parser.add_argument('--dsp', required=True, help="Entity that sells musical content to store")
    parser.add_argument('--id_sub_platform', type=int, help="Internal ID in Believe Reference Data")
    parser.add_argument('--date', required=True, type=lambda d: datetime.strptime(d, "%Y-%m-%d"), help="DSP Report date (format: YYYY-MM-DD)")
    parser.add_argument('-r', '--report_id', required=True, help="Correlation ID for the daily stats ingestion")
    parser.add_argument('--report_iteration', type=int, required=True, help="Correlation ID iteration for the daily stats ingestion")
    parser.add_argument('--debug', action='store_true', help="Activate debug feature")
    parser.add_argument('--local', action='store_true', help="Start Spark in local mode")
    parser.add_argument('--local-catalog', help="Local catalog (only in dev mode)")
    parser.add_argument('--retry', action='store_true', help="Activate retry feature")
    parser.add_argument('--retry_number', type=int, help="Retry Number for the daily stats ingestion")

    args = parser.parse_args()
    return JobConfig(
        source=args.source,
        destination=args.destination,
        distributor=args.distributor,
        dsp=args.dsp,
        id_sub_platform=args.id_sub_platform,
        date=args.date,
        report_id=args.report_id,
        report_iteration=args.report_iteration,
        debug=args.debug,
        local=args.local,
        local_catalog=args.local_catalog,
        retry=args.retry,
        retry_number=args.retry_number
    )