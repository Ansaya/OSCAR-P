# author: Asmaa Mirkhan ~ 2019

import os
import argparse
import cv2 as cv
import time
from IntelAPI import IntelDetectorAPI
from datetime import datetime

def blurBoxes(image, boxes):
    """
    Argument:
    image -- the image that will be edited as a matrix
    boxes -- list of boxes that will be blurred, each box must be int the format (x_top_left, y_top_left, x_bottom_right, y_bottom_right)

    Returns:
    image -- the blurred image as a matrix
    """

    for box in boxes:
        # unpack each box
        x1, y1, x2, y2 = [d for d in box]

        # crop the image due to the current box
        sub = image[y1:y2, x1:x2]

        # apply GaussianBlur on cropped area
        blur = cv.blur(sub, (10, 10))

        # paste blurred image on the original image
        image[y1:y2, x1:x2] = blur

    return image


def main(args):
    threshold = args.threshold
    
    # open image
    print("#load_image: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))
    image = cv.imread(args.input_image)

    model_path = args.model_path
    intelapi = IntelDetectorAPI(model_path)
    boxes = intelapi.processImage(image, threshold)

    # apply blurring
    image = blurBoxes(image, boxes)

    # # show image
    # cv.imshow('blurred', image)

    print("#image_write: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))

    cv.imwrite(args.output_image, image)
    print('Image has been saved successfully at', args.output_image,
              'path')
    print("#python_script_end: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))


if __name__ == "__main__":
    print("#python_script_start: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))
    # creating argument parser
    parser = argparse.ArgumentParser(description='Image blurring parameters')

    # adding arguments
    parser.add_argument('-i',
                        '--input_image',
                        help='Path to your image',
                        type=str,
                        required=True)
    parser.add_argument('-m',
                        '--model_path',
                        default='/opt/blurry-faces/model/face.xml',
                        help='Path to .pb model',
                        type=str)
    parser.add_argument('-o',
                        '--output_image',
                        help='Output file path',
                        type=str)
    parser.add_argument('-t',
                        '--threshold',
                        help='Face detection confidence',
                        default=0.7,
                        type=float)
    args = parser.parse_args()
    print(args)
    # if input image path is invalid then stop
    assert os.path.isfile(args.input_image), 'Invalid input file'

    # if output directory is invalid then stop
    if args.output_image:
        assert os.path.isdir(os.path.dirname(
            args.output_image)), 'No such directory'

    main(args)


