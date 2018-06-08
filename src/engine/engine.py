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

    def insertData(self, xml):
        """
        
        """
        # Parse data from the xml file
        parsedXml = etree.parse(xml)
        data = etree.XPathEvaluator(parsedXml)
        
        # Insert the DIM signature
        dimSignature = self.insertDimSignature(data)

        # Insert the source
        try:
            source = self.insertSource(data, dimSignature)
        except:
            # Log that the source file has been already been processed
            print("The DIM Processing was already ingested")
            self.session.rollback()
            self.session.close()
            return
        # end try

        # Insert gauges
        gauges = self.insertGauges(data,dimSignature)

        # Insert annotation configuration
        annotationCnfs = self.insertAnnotationConfs(data,dimSignature)

        # Insert explicit reference groups
        explicitRefGroups = self.insertExplGroups(data)

        # Insert explicit reference groups
        explicitRefs = self.insertExplicitRefs(data, explicitRefGroups)

        # Insert events
        self.insertEvents(data, gauges, explicitRefs, source)

        self.session.commit()
        self.session.close()
        print("All information has been committed")
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
            # The DIM signature was already available. Roll back
            # transaction for re-using the session
            self.session.rollback()
            pass
        # end try

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
            # The DIM processing was already ingested
            self.session.rollback()
            raise Exception("The data associated to the source with name {} associated to the DIM signature {} and DIM processing {} with version {} has been already ingested".format (name, dimSignature.dim_signature, dimSignature.dim_exec_name, execVersion))
        # end try

        return dimProcessing
        
    def insertGauges(self, data, dimSignature):
        """
        """
        dictGauges = {}
        gauges = data("/gsd/data/event/gauge")
        for gauge in gauges:
            self.session.begin_nested()
            name = gauge.get("name")
            system = gauge.get("system")
            gaugeDdbb = self.session.query(Gauge).filter(Gauge.name == name, Gauge.system == system, Gauge.dim_signature_id == dimSignature.dim_signature_id).first()
            if gaugeDdbb == None:
                gaugeDdbb = Gauge(name, dimSignature, system)
                self.session.add (gaugeDdbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The gauge has been inserted already by other process while checking
                    self.session.rollback()
                    pass
                # end try
            else:
                # Nothing done. Close transaction
                self.session.rollback()
            # end if
            dictGauges[(name,system)] = gaugeDdbb
        # end for

        return dictGauges
        
    def insertAnnotationConfs(self, data, dimSignature):
        """
        """
        dictAnnotationConfs = {}
        annotationCnfs = data("/gsd/data/annotation/annotationCnf")
        for annotationCnf in annotationCnfs:
            self.session.begin_nested()
            name = annotationCnf.get("name")
            system = annotationCnf.get("system")
            annotationCnfDdbb = self.session.query(AnnotationCnf).filter(AnnotationCnf.name == name, AnnotationCnf.system == system, AnnotationCnf.dim_signature_id == dimSignature.dim_signature_id).first()
            if annotationCnfDdbb == None:
                annotationCnfDdbb = AnnotationCnf(name, dimSignature, system)
                self.session.add (annotationCnfDdbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The annotation configuration has been inserted already by other process while checking
                    self.session.rollback()
                    pass
                # end try
            else:
                # Nothing done. Close transaction
                self.session.rollback()
            # end if
            dictAnnotationConfs[(name,system)] = annotationCnfDdbb
        # end for

        return dictAnnotationConfs
        
    def insertExplGroups(self, data):
        """
        """
        dictExplGroups = {}
        explGroups = data("/gsd/data/event/explicitRef/@group")
        for explGroup in explGroups:
            print (explGroup)
            self.session.begin_nested()
            explGroupDdbb = self.session.query(ExplicitRefGrp).filter(ExplicitRefGrp.name == explGroup).first()
            if explGroupDdbb == None:
                explGroupDdbb = ExplicitRefGrp(explGroup)
                self.session.add (explGroupDdbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The explicit reference group has been inserted already by other process while checking
                    self.session.rollback()
                    pass
                # end try
            else:
                # Nothing done. Close transaction
                self.session.rollback()
            # end if
            dictExplGroups[explGroup] = explGroupDdbb
        # end for

        return dictExplGroups
        
    def insertExplicitRefs(self, data, explicitRefGroups):
        """
        """
        dictExplicitRefs = {}
        explicitRefs = data("/gsd/data/event/explicitRef")
        for explicitRef in explicitRefs:
            self.session.begin_nested()
            name = explicitRef.get('id')
            explicitRefDdbb = self.session.query(ExplicitRef).filter(ExplicitRef.explicit_ref == name).first()
            if explicitRefDdbb == None:
                group = explicitRef.get('group')
                explicitRefGrp = explicitRefGroups[group]
                explicitRefDdbb = ExplicitRef(datetime.datetime.now(), name, explicitRefGrp)
                self.session.add (explicitRefDdbb)
                try:
                    self.session.commit()
                except IntegrityError:
                    # The gauge has been inserted already by other process while checking
                    self.session.rollback()
                    pass
                # end try
            else:
                # Nothing done. Close transaction
                self.session.rollback()
            # end if
            dictExplicitRefs[name] = explicitRefDdbb
        # end for

        return dictExplicitRefs

    def insertEvents(self, data, gauges, explicitRefs, source):
        """
        """
        listEvents = []
        events = data('/gsd/data/event')
        for event in events:
            id = uuid.uuid1(node = os.getpid(), clock_seq = random.getrandbits(14))
            start = event.get('start')
            stop = event.get('stop')
            generationTime = event.get('generationTime')
            gaugeInfo = event.xpath('gauge').pop()
            gauge = gauges[(gaugeInfo.get('name'), gaugeInfo.get('system'))]
            explicitRef = explicitRefs[event.xpath('explicitRef').pop().get('id')]

            # Insert the event into the list for bulk ingestion
            listEvents.append (dict (event_uuid = id, start = start,
                                     stop = stop, generation_time = generationTime,
                                     ingestion_time = datetime.datetime.now(),
                                     gauge_id = gauge.gauge_id,
                                     explicit_ref_id = explicitRef.explicit_ref_id,
                                     processing_uuid = source.processing_uuid))
        # end for
            
        # Bulk insert events
        self.session.bulk_insert_mappings(Event, listEvents)
        self.session.commit()

        return
