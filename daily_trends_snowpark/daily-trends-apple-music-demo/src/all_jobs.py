from structuring_at_2023 import StructuringAt20230424
from common.src.cli.cli_option_parser import parse_arguments

if __name__=='__main__':
    config = parse_arguments()

    if not config.retry:
        # Structuring
        structuring = StructuringAt20230424(config=config)
        structuring.run()

        # Enriching
        #enriching = Enriching(config=config)
        #enriching.run()

    import sys
    print(sys.path)
