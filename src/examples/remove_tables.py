"""
Test: remove tables

Written by DEIMOS Space S.L. (dibb)

module eboa
"""

# Import engine of the DDBB
from eboa.datamodel.base import Session, engine, Base
# Import datamodel
from eboa.datamodel.dim_signatures import DimSignature
from eboa.datamodel.events import Event, EventLink, EventKey, EventText, EventDouble, EventObject, EventGeometry
from eboa.datamodel.gauges import Gauge
from eboa.datamodel.dim_processings import DimProcessing, DimProcessingStatus
from eboa.datamodel.explicit_refs import ExplicitRef, ExplicitRefGrp, ExplicitRefLink
from eboa.datamodel.annotations import Annotation, AnnotationCnf, AnnotationText, AnnotationDouble, AnnotationObject, AnnotationGeometry


# Clear all tables before executing the test
for table in reversed(Base.metadata.sorted_tables):
    engine.execute(table.delete())
# end for
