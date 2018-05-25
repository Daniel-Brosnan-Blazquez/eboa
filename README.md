##GSDM: Ground Segment Data Management##

This component is the data storage management tool for ground segment data.
The data management will make use of a data model containing the following main entities:

1. Events

2. Annotations

3. Explicit references

4. Data Ingestion Modules

** Purpose **

This component will manage the storage of the ground segment data following the next requirements:

1. Traceability of all the data to the source of information (even information created within the infrastructure)

2. No pre-configuration needed for inserting data

3. Include modern datatypes for storing the data

4. Flexible structure for linking information to events and annotations

5. Flexible structure for linking events between them

6. Flexible structure for linking explicit references between them

7. Continuous developtment/Continuous integration approach

8. Include geo-query functionalities

9. Parallel insertion of data using the RDBMS parallelism mechanisms

10. Quick access to the information by which the information can be managed dynamically depending on the needs of the users

** Scope **

This component will be targetted to systems/tools with the need of storing time-tagged information.
The data model used will allow quick access to the infomration in a structured way.