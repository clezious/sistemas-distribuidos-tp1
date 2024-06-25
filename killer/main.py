import argparse
import logging
from killer import Killer


MIN_VAL = 0
MAX_VAL = 100
LOG_LEVEL = logging.INFO

DEFAULT_INTERVAL = 0.5
DEFAULT_KILL_PROBABILITY = 1


def percentage(arg):
    """ Type function for argparse - a float within some predefined bounds """
    try:
        f = float(arg)
    except ValueError:
        raise argparse.ArgumentTypeError("Must be a floating point number")
    if f < MIN_VAL or f > MAX_VAL:
        raise argparse.ArgumentTypeError(f"Argument must be <= {str(MAX_VAL)} and >= {str(MIN_VAL)}")
    return f


def main():
    logging.basicConfig(level=LOG_LEVEL)

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="Subcommands")
    nuke_subparser = subparsers.add_parser(
        'nuke', help="Nuke all containers except the ones in the EXCLUDED_CONTAINERS list and 1 docktor")
    roulette_subparser = subparsers.add_parser('roulette', help="Russian roulette mode")

    nuke_subparser.set_defaults(func=run_nuke)

    roulette_subparser.add_argument("-k", "--kill_probability", type=percentage,
                                    default=DEFAULT_KILL_PROBABILITY, help="Probability of killing a container")
    roulette_subparser.add_argument("-t", "--time_interval", type=float, default=DEFAULT_INTERVAL, help="Time interval between kills")
    roulette_subparser.set_defaults(func=run_roulette)

    args = parser.parse_args()
    logging.info("Killer starting")
    return args.func(args)


def run_nuke(_args):
    killer = Killer()
    killer.nuke()


def run_roulette(args):
    killer = Killer()
    killer.russian_roulette(args.kill_probability, args.time_interval)


if __name__ == '__main__':
    main()
