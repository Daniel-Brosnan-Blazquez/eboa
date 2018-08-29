"""
Test: remove tables

Written by DEIMOS Space S.L. (dibb)

module gsdm
"""

# Import engine of the DDBB
from gsdm.datamodel.base import Session, engine, Base
# Import datamodel
from gsdm.datamodel.dim_signatures import DimSignature
from gsdm.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry
from gsdm.datamodel.gauges import Gauge
from gsdm.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from gsdm.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from gsdm.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry


# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())
# end for
