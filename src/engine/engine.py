"""
Engine definition

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""
import os
import sys

# Adding path to the datamodel package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import SQLalchemy exceptions
from sqlalchemy.exc import IntegrityError

from datamodel.base import Session, engine, Base
from datamodel.dim_signatures import DimSignature
from datamodel.events import Event, EventLink, EventText, EventDouble, EventObject, EventGeometry
from datamodel.gauges import Gauge
from datamodel.dim_processings import DimProcessing, DimProcessingStatus
from datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry
import datetime
import uuid
import pprint
from math import pi
import random
from lxml import etree
import datetime

class Engine():

    session = Session()

    def insertData (self, xml):
        """
        
        """
        # Parse data from the xml file
        parsedXml = etree.parse(xml)
        data = etree.XPathEvaluator(parsedXml)
        
        # Insert the DIM signature
        dimSignature = self.insertDimSignature (data)

        # Insert the source
        try:
            source = self.insertSource (data, dimSignature)
        except:
            # Log that the source file has been already been processed
            print ("The DIM Processing was already ingested")
            self.session.rollback()
            self.session.close()
            return


        self.session.commit()
        return

    def insertDimSignature(self, data):
        """
        """
        self.session.begin_nested()
        dimSignature = data("/gsd/dim_signature").pop()
        dimName = dimSignature.get("name")
        execName = dimSignature.get("exec")
        self.session.add (DimSignature(dimName, execName))
        try:
            self.session.commit()
        except IntegrityError:
            print ("The DIM Signature was already ingested")
            # The DIM signature was already available. Roll back
            # transaction for re-using the session
            self.session.rollback()
            pass

        # Get the stored DIM signature
        dimSignature = self.session.query(DimSignature).filter(DimSignature.dim_signature == dimName and DimSignature.dim_exec_name == execName).first()
        return dimSignature
        
    def insertSource(self, data, dimSignature):
        """
        """
        self.session.begin_nested()
        execVersion = data("/gsd/dim_signature/@version").pop()
        source = data("/gsd/source").pop()
        name = source.get("name")
        generationTime = source.get("generationTime")
        validityStart = source.get("validityStart")
        validityStop = source.get("validityStop")
        id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
        dimProcessing = DimProcessing(id, name, validityStart,
                                      validityStop, generationTime, datetime.datetime.now(),
                                      execVersion, dimSignature)
        self.session.add (dimProcessing)
        try:
            self.session.commit()
        except IntegrityError:
            # The DIM processing was already 
            self.session.rollback()
            raise Exception("The data associated to the source with name {} associated to the DIM signature {} and DIM processing {} with version {} has been already ingested".format (name, dimSignature.dim_signature, dimSignature.dim_exec_name, execVersion))

        # Get the stored DIM signature
        return dimProcessing
        

    # def insertGauges(self, data):
        
    # def insertAnnotationConfs(annotationCnfs):

