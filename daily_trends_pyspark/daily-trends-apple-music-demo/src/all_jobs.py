from src.cli.cli_option_parser import parse_arguments
from structuring_at_2023 import StructuringAt20230424
from enriching import Enriching

if __name__=='__main__':
    config = parse_arguments()

    if not config.retry:
        # Structuring
        structuring = StructuringAt20230424(config=config)
        #structuring.run()

        # Enriching
        enriching = Enriching(config=config)
        enriching.run()

