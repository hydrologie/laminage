from hec.script import *
from hec.heclib.dss import *
from hec.hecmath import *
from hec.heclib.util import HecTime
from hec.ui import CheckTreeManager
from java.awt import BorderLayout
from java.awt import Font
from java.awt import GridLayout
from java.awt.event import ActionListener
from javax.swing import *
from javax.swing.border import *
from javax.swing.tree import *
import copy, hec, java, os, re, rma, sys, threading, time, traceback

A, B, C, D, E, F = 1, 2, 3, 4, 5, 6
programName = os.path.splitext(os.path.split(sys.argv[0])[1])[0]
watershedDir = os.path.realpath(sys.argv[1])
watershedName = os.path.basename(watershedDir)
outputDir = watershedDir
statusQueue = None
dataQueue = None


# =========================================================================
def openWatershed(watershed, watershedDir=None):
    '''
    Opens a specified watershed in ResSim.
    '''
    # ----------------------------------------------#
    # determine watershed info from the parameters #
    # ----------------------------------------------#
    if watershedDir is not None:
        openWatershed(os.path.join(watershedDir, watershed))
    if os.path.exists(watershed):
        if os.path.isfile(watershed):
            # ---------------------------------------#
            # watershed param is workspace filename #
            # ---------------------------------------#
            watershedName = os.path.basename(os.path.dirname(watershed))
            wkspFileName = watershed
        else:
            # ----------------------------------------#
            # watershed param is watershed directory #
            # ----------------------------------------#
            watershedName = os.path.basename(watershed)
            wkspFileName = os.path.join(watershed, "%s.wksp" % watershedName)
    else:
        newWatershed = os.path.join(
            hec.client.ClientApp.app().getAppStartDir(),
            "watershed",
            "base",
            watershed)
        if os.path.exists(newWatershed):
            return openWatershed(newWatershed)
        else:
            raise Exception("Unable to open watershed %s" % newWatershed)
    # -------------------------------------------------#
    # open the watershed and verify we were sucessful #
    # -------------------------------------------------#
    ResSim.openWatershed(wkspFileName.replace(os.sep, "/"))
    if ResSim.getWatershedName() != watershedName:
        raise Exception("Unable to open watershed %s" % wkspFileName)

# =========================================================================
def setModule(moduleName):
    '''
    Sets ResSim to the specified module, and returns the module.
    '''
    if `ResSim.getCurrentModule()` != moduleName:
        ResSim.selectModule(moduleName)

    currentModule = ResSim.getCurrentModule()

    if `currentModule` != moduleName:
        raise Exception("Unable to switch to %s module." % moduleName)

    return currentModule

# =========================================================================
def openSimulation(simulationName):
    '''
    Opens the specified simulation and returns it.
    '''
    module = ResSim.getCurrentModule()
    if `module` != "Simulation":
        raise Exception("Must be in Simulation module to open a simulation.")
    if module.openSimulation(simulationName):
        return module.getSimulation()
    else:
        raise Exception('Could not open simulation "%s".' % simulationName)

def createSimulation():
    """

    """
    alternativeNames = []
    filename = os.path.join(watershedDir, 'alternatives.txt')
    print(filename)
    fh = open(filename, 'r')
    lines = fh.readlines()
    # print '%s' % (lines)
    fh.close()

    for i, line in enumerate(lines):
        if i == 0:
            lookback_date = line.split('\n')[0]
        elif i == 1:
            start_date = line.split('\n')[0]
        elif i ==2:
            end_date = line.split('\n')[0]
        else:
            alternativeNames.append(line.split('\n')[0])

    module = ResSim.getCurrentModule()
    a = module.createSimulation("sim_batch", "test_desc", lookback_date, start_date, end_date, 1, 2,
                                alternativeNames)
def runSimulations():

    alternativeNames = []
    filename = os.path.join(watershedDir, 'alternatives.txt')
    print(filename)
    fh = open(filename, 'r')
    lines = fh.readlines()
    # print '%s' % (lines)
    fh.close()

    for i, line in enumerate(lines):
        if i == 0:
            lookback_date = line.split('\n')[0]
        elif i == 1:
            start_date = line.split('\n')[0]
        elif i ==2:
            end_date = line.split('\n')[0]
        else:
            alternativeNames.append(line.split('\n')[0])


    simulationName='sim_batch'

    # alternativeNames = ['M' + "%09d" % (i,) for i in range(1,101)]
    # alternativeNames = ['M000052145']
    createSimulation()
    simulation = openSimulation(simulationName)
    for alternativeName in alternativeNames:
        simRun = simulation.getSimulationRun(alternativeName)
        simulation.computeRun(simRun, -1)
        ResSim.getCurrentModule().saveSimulation()

# =========================================================================
def main():

    simulationName = 'sim_batch'
    simModule = setModule("Simulation")
    openWatershed(watershedDir)
    watershed = ResSim.getWatershed()
    watershedName = watershed.getName()
    t = threading.Thread(target=runSimulations)
    t.start()

# =========================================================================
main()