import argparse
from Direct import *


def main(args):
    print(args)
    # TODO parse the args into variables

    if args.advance:
        print('SketchRefine Mode')
        # TODO add code for SketchRefine here
    else:
        print('Direct Mode')
        # TODO add code for Direct here



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='pqql')

    parser.add_argument("-a", "--advance", action="store_true",
                        help="Advance mode - Use SketchRefine instead of Direct")

    # TODO add argument for default read in data file address

    # TODO add argument for default input(json file) address

    # TODO add argument for default output directory and file name

    # print(parser.parse_args())
    main(parser.parse_args())