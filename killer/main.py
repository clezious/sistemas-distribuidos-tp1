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


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--kill_probability", type=percentage,
                        default=DEFAULT_KILL_PROBABILITY, help="Probability of killing a container")
    parser.add_argument("-t", "--time_interval", type=float, default=DEFAULT_INTERVAL, help="Time interval between kills")
    args = parser.parse_args()
    return args.kill_probability, args.time_interval


def main():
    logging.basicConfig(level=LOG_LEVEL)
    kill_probability, time_interval = parse_args()
    logging.info("Killer starting")
    killer = Killer()
    killer.russian_roulette(kill_probability, time_interval)


if __name__ == '__main__':
    main()
