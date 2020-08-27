from pbprocesstools.pbpt_q_process import PBPTQProcessTool
import logging
import os
import shutil
import time
import rsgislib
import rsgislib.vectorutils
import rsgislib.imagecalc
import rsgislib.imageutils
import rsgislib.classification.classxgboost

logger = logging.getLogger(__name__)

def apply_xgboost_binary_classifier(model_file, imgMask, imgMaskVal, imgFileInfo, outProbImg, gdalformat,
                                    outClassImg=None, class_thres=5000, nthread=1):
    """
This function applies a trained binary (i.e., two classes) xgboost model. The function train_xgboost_binary_classifer
can be used to train such as model. The output image will contain the probability of membership to the class of
interest. You will need to threshold this image to get a final hard classification. Alternative, a hard class output
image and threshold can be applied to this image.

:param model_file: a trained xgboost binary model which can be loaded with lgb.Booster(model_file=model_file).
:param imgMask: is an image file providing a mask to specify where should be classified. Simplest mask is all the
                valid data regions (rsgislib.imageutils.genValidMask)
:param imgMaskVal: the pixel value within the imgMask to limit the region to which the classification is applied.
                   Can be used to create a heirachical classification.
:param imgFileInfo: a list of rsgislib.imageutils.ImageBandInfo objects (also used within
                    rsgislib.imageutils.extractZoneImageBandValues2HDF) to identify which images and bands are to
                    be used for the classification so it adheres to the training data.
:param outProbImg: output image file with the classification probabilities - this image is scaled by
                   multiplying by 10000.
:param gdalformat: is the output image format - all GDAL supported formats are supported.
:param outClassImg: Optional output image which will contain the hard classification, defined with a threshold on the
                    probability image.
:param class_thres: The threshold used to define the hard classification. Default is 5000 (i.e., probability of 0.5).
:param nthread: The number of threads to use for the classifier.

    """
    import numpy
    import xgboost as xgb

    from rios import applier
    from rios import cuiprogress

    import rsgislib.imagecalc
    import rsgislib.rastergis

    def _applyXGBClassifier(info, inputs, outputs, otherargs):
        outClassVals = numpy.zeros_like(inputs.imageMask, dtype=numpy.uint16)
        if numpy.any(inputs.imageMask == otherargs.mskVal):
            outClassVals = outClassVals.flatten()
            imgMaskVals = inputs.imageMask.flatten()
            classVars = numpy.zeros((outClassVals.shape[0], otherargs.numClassVars), dtype=numpy.float)
            # Array index which can be used to populate the output array following masking etc.
            ID = numpy.arange(imgMaskVals.shape[0])
            classVarsIdx = 0
            for imgFile in otherargs.imgFileInfo:
                imgArr = inputs.__dict__[imgFile.name]
                for band in imgFile.bands:
                    classVars[..., classVarsIdx] = imgArr[(band - 1)].flatten()
                    classVarsIdx = classVarsIdx + 1
            classVars = classVars[imgMaskVals == otherargs.mskVal]
            ID = ID[imgMaskVals == otherargs.mskVal]
            predClass = numpy.around(otherargs.classifier.predict(xgb.DMatrix(classVars)) * 10000)
            outClassVals[ID] = predClass
            outClassVals = numpy.expand_dims(
                    outClassVals.reshape((inputs.imageMask.shape[1], inputs.imageMask.shape[2])), axis=0)
        outputs.outimage = outClassVals

    classifier = xgb.Booster({'nthread': nthread})
    classifier.load_model(model_file)
    
    print(imgMask)
    infiles = applier.FilenameAssociations()
    infiles.imageMask = imgMask
    numClassVars = 0
    for imgFile in imgFileInfo:
        infiles.__dict__[imgFile.name] = imgFile.fileName
        numClassVars = numClassVars + len(imgFile.bands)

    outfiles = applier.FilenameAssociations()
    outfiles.outimage = outProbImg
    otherargs = applier.OtherInputs()
    otherargs.classifier = classifier
    otherargs.mskVal = imgMaskVal
    otherargs.numClassVars = numClassVars
    otherargs.imgFileInfo = imgFileInfo

    try:
        import tqdm
        progress_bar = rsgislib.TQDMProgressBar()
    except:
        progress_bar = cuiprogress.GDALProgressBar()

    aControls = applier.ApplierControls()
    aControls.progress = progress_bar
    aControls.drivername = gdalformat
    aControls.omitPyramids = True
    aControls.calcStats = False
    print("Applying the Classifier")
    applier.apply(_applyXGBClassifier, infiles, outfiles, otherargs, controls=aControls)
    print("Completed")
    time.sleep(1)
    if os.path.exists(outProbImg):
        print("Calc min/max prob values.")
        min_max_prob_vals = rsgislib.imagecalc.getImageBandMinMax(outProbImg, 1, False, -1)
        print("Min: {}, Max: {}".format(min_max_prob_vals[0], min_max_prob_vals[1]))
        if min_max_prob_vals[1] > 0:
            print("Create hard class")

            bandDefns = []
            bandDefns.append(rsgislib.imagecalc.BandDefn('prob', outProbImg, 1))
            bandDefns.append(rsgislib.imagecalc.BandDefn('msk', imgMask, 1))
            rsgislib.imagecalc.bandMath(outClassImg, '(msk!={})?-1:(prob>{})?1:0'.format(imgMaskVal, class_thres), 'KEA', rsgislib.TYPE_16INT, bandDefns)
            print("Finished")
        else:
            print("Create empty out image")
            rsgislib.imageutils.createCopyImage(imgMask, outClassImg, 1, -1, 'KEA', rsgislib.TYPE_16INT)
            print("Created empty image.")


def countPxlsOfVal(inputImg, vals=[0]):
    """
Function which counts the number of pixels of a set of values returning a list in the same order as the list of values provided.

:param inputImg: the input image
:param vals: is a list of pixel values to be counted

"""
    import numpy

    if len(vals) == 0:
        raise Exception('At least 1 value should be provided within the vals input varable.')
    numVals = len(vals)
    outVals = numpy.zeros(numVals, dtype=numpy.int64)

    from rios.imagereader import ImageReader

    reader = ImageReader(inputImg)
    for (info, block) in reader:
        counts = dict(zip(*numpy.unique(block, return_counts=True)))
        for idx in range(numVals):
            if vals[idx] in counts:
                outVals[idx] = outVals[idx] + counts[vals[idx]]

    return outVals




class ApplyXGBClass(PBPTQProcessTool):

    def __init__(self):
        super().__init__(cmd_name='apply_scn_cls.py', descript=None)

    def do_processing(self, **kwargs):
        if not os.path.exists(self.params['tmp_dir']):
            os.mkdir(self.params['tmp_dir'])
            time.sleep(1)

        n_smpls = countPxlsOfVal(self.params['cls_msk_img'], vals=[1])
        print(n_smpls)

        if n_smpls[0] > 0:
            fileInfo = [rsgislib.imageutils.ImageBandInfo(self.params['sref_img'], 'sen2', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])]
            outProbImg = os.path.join(self.params['tmp_dir'], "prob_cls_img.tif")
            if os.path.exists(outProbImg):
                os.remove(outProbImg)
                time.sleep(1)
            print(self.params['cls_msk_img'])
            apply_xgboost_binary_classifier(self.params['cls_mdl_file'],
                                            self.params['cls_msk_img'], 1,
                                            fileInfo, outProbImg, 'GTIFF',
                                            outClassImg=self.params['out_cls_file'],
                                            class_thres=5000, nthread=1)
        else:
            rsgislib.imageutils.createCopyImage(self.params['vld_img'], self.params['out_cls_file'], 1, -1, 'GTIFF', rsgislib.TYPE_8UINT)

        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])

    def required_fields(self, **kwargs):
        return ["scn_id", "vld_img", "clrsky_img", "sref_img", "cls_msk_img", "cls_mdl_file", "out_cls_file", "tmp_dir"]

    def outputs_present(self, **kwargs):
        files_dict = dict()
        files_dict[self.params['out_cls_file']] = 'file'
        return self.check_files(files_dict)

    def remove_outputs(self, **kwargs):
        # Reset the tmp dir
        if os.path.exists(self.params['tmp_dir']):
            shutil.rmtree(self.params['tmp_dir'])
        os.mkdir(self.params['tmp_dir'])

        # Remove the output file.
        if os.path.exists(self.params['out_cls_file']):
            os.remove(self.params['out_cls_file'])

if __name__ == "__main__":
    ApplyXGBClass().std_run()


