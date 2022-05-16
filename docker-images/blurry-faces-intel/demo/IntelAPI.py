#!/usr/bin/env python3
import argparse
import logging as log
import os
import sys

import cv2
import numpy as np
from openvino.inference_engine import IECore

from datetime import datetime


class IntelDetectorAPI:
    def __init__(self, model_path):
        self.model_path = model_path

    def processImage(self, image, threshold):
        log.basicConfig(format='[ %(levelname)s ] %(message)s', level=log.INFO, stream=sys.stdout)
        print("#load_model: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))

        # ---------------------------Step 1. Initialize inference engine core--------------------------------------------------
        log.info('Creating Inference Engine')
        ie = IECore()

        # ---------------------------Step 2. Read a model in OpenVINO Intermediate Representation or ONNX format---------------
        log.info(f'Reading the network: {self.model_path}')
        net = ie.read_network(model=self.model_path)

        # ---------------------------Step 3. Configure input & output----------------------------------------------------------
        log.info('Configuring input and output blobs')
        # Get name of input blob
        input_blob = next(iter(net.input_info))

        # Set input and output precision manually
        net.input_info[input_blob].precision = 'U8'

        if len(net.outputs) == 1:
            output_blob = next(iter(net.outputs))
            net.outputs[output_blob].precision = 'FP32'
        else:
            net.outputs['boxes'].precision = 'FP32'
            net.outputs['labels'].precision = 'U16'

        # ---------------------------Step 4. Loading model to the device-------------------------------------------------------
        log.info('Loading the model to the plugin')
        exec_net = ie.load_network(network=net, device_name="MYRIAD")

        # ---------------------------Step 5. Create infer request--------------------------------------------------------------
        # load_network() method of the IECore class with a specified number of requests (default 1) returns an ExecutableNetwork
        # instance which stores infer requests. So you already created Infer requests in the previous step.

        # ---------------------------Step 6. Prepare input---------------------------------------------------------------------
        print("#face_detect_blur: " + str(datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")))
        # image = cv2.imread(image_path)
        h, w, _ = image.shape
        _, _, net_h, net_w = net.input_info[input_blob].input_data.shape

        if image.shape[:-1] != (net_h, net_w):
            log.warning(f'Image is resized from {image.shape[:-1]} to {(net_h, net_w)}')
            image = cv2.resize(image, (net_w, net_h))

        # Change data layout from HWC to CHW
        image = image.transpose((2, 0, 1))
        # Add N dimension to transform to NCHW
        image = np.expand_dims(image, axis=0)

        # ---------------------------Step 7. Do inference----------------------------------------------------------------------
        log.info('Starting inference in synchronous mode')
        res = exec_net.infer(inputs={input_blob: image})

        # ---------------------------Step 8. Process output--------------------------------------------------------------------
        res = res[output_blob]
        # Change a shape of a numpy.ndarray with results ([1, 1, N, 7]) to get another one ([N, 7]),
        # where N is the number of detected bounding boxes
        detections = res.reshape(-1, 7)

        boxes = []
        for i, detection in enumerate(detections):
            _, class_id, confidence, xmin, ymin, xmax, ymax = detection

            if confidence > threshold:
                xmin = int(xmin * w)
                ymin = int(ymin * h)
                xmax = int(xmax * w)
                ymax = int(ymax * h)

                box = (xmin, ymin, xmax, ymax)
                boxes.append(box)
                log.info(f'Valid box, ' f'coords = ({xmin}, {ymin}), ({xmax}, {ymax}), confidence = {confidence:.2f}')
            elif confidence > 0.5:
                log.info(f'Discarded box, ' f'coords = ({xmin}, {ymin}), ({xmax}, {ymax}), confidence = {confidence:.2f}')

        return boxes
