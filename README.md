# EBOA: Engine for Business Operation Analysis #

This component is the data storage management tool for the Business Operation Analysis.
The data management makes use of a data model containing the following main entities:

1. **Events**: periods of time associated to a gauge of a certain aspect business-related

2. **Annotations**: particular aspects associated to an explicit reference

3. **Explicit references**: identifiers referring to an entity business-related

4. **Sources**: blocks of information received from external interfaces to process; after processing, interesting information is extracted and stored

5. **Alerts**: notifications to users regarding anomalies identified by the system related to previous entities

6. **Reports**: containers of analysis

## Purpose ##

EBOA (Engine for Business Operation Analysis) serves as a comprehensive data storage and management platform designed to handle complex business operation data with maximum flexibility and efficiency. It provides a robust foundation for storing, retrieving, and analyzing time-tagged information without requiring predefined schemas or extensive configuration.

The system is built around the following core requirements and capabilities:

### Data Integrity and Traceability ###
- **Complete traceability**: All data maintains full traceability to its source of information, including data generated within the infrastructure itself
- **Source validation**: Ensures data provenance and reliability through comprehensive source tracking

### Flexibility and Adaptability ###
- **Schema-less insertion**: No pre-configuration required for inserting new types of data
- **Dynamic data structures**: Supports flexible linking between events, annotations, and explicit references
- **Modern data types**: Includes contemporary data types for comprehensive data storage capabilities

### Performance and Scalability ###
- **Parallel processing**: Leverages RDBMS parallelism mechanisms for efficient data insertion
- **Geo-query support**: Includes geospatial query functionalities for location-based analysis
- **Dynamic access**: Enables quick, on-demand access to information based on user requirements

### Development and Operations ###
- **Continuous development**: Follows continuous development and continuous integration practices
- **Structured data access**: Provides organized, structured access to stored information for effective management

## Scope ##

EBOA is designed for enterprise and analytical systems that require robust storage and management of time-series and time-tagged business data. It excels in environments where data complexity, volume, and dynamic requirements demand a flexible, scalable solution.

### Target Systems ###
- Real-time business operation monitoring and analysis platforms
- Event-driven architectures requiring centralized data storage
- Multi-source data aggregation systems
- Complex business intelligence and reporting platforms
- Systems requiring dynamic schema adaptation without downtime

### Supported Operations ###
- Insertion, update, and deletion of events, annotations, and related business entities
- Complex queries across multi-dimensional business data
- Flexible linking and relationship management between different data types
- Annotation and enrichment of business operations data
- Alert generation and reporting on business events

### Data Model Characteristics ###
- Optimized for time-tagged information with comprehensive temporal tracking
- Hierarchical data organization enabling quick structured access
- Support for complex data types including geometric/location data
- Dynamic value structures supporting unlimited data dimensionality
- Maintains referential integrity while providing maximum flexibility

### Integration Capabilities ###
- Multi-format data input support (XML, JSON, Python dictionaries)
- Schema-based validation ensuring data quality
- Parallel data processing leveraging modern database capabilities
- Priority-based data conflict resolution
- Scalable for high-volume, concurrent data operations

## Full Stack Application: EBOA + VBOA ##

While EBOA serves as a powerful backend data management engine, it is designed to work seamlessly with **VBOA** (Visualization for Business Operation Analysis), a modern frontend component that completes the full-stack application architecture.

### Architecture Overview ###

The EBOA + VBOA stack provides an enterprise-grade solution for business operation analysis:

- **EBOA (Backend)**: Data storage, management, and query engine built on PostgreSQL with Python API
- **VBOA (Frontend)**: Interactive web application for visualization, analysis, and real-time monitoring

Together, these components form a complete, production-ready platform for complex business operation management without requiring extensive custom development.

### Easy Tailoring Structure ###

One of the key advantages of the EBOA + VBOA stack is its easily customizable architecture:

- **Modular design**: Both components are designed with modularity in mind, allowing you to extend and customize functionality
- **Configuration-driven**: Most features are driven by configuration files rather than code changes
- **Data model adaptation**: Define your own entities and relationships through EBOA's flexible schema
- **UI customization**: VBOA's interface can be tailored to your specific business needs
- **Integration points**: Well-defined APIs and integration points for connecting custom processors and analyzers

### Getting Started ###

To get started with the full EBOA + VBOA stack:

1. **EBOA Documentation**: This repository contains comprehensive documentation for the backend component
2. **VBOA Repository**: Visit the GitHub repository for VBOA to access the frontend component and additional examples:
https://github.com/Daniel-Brosnan-Blazquez/vboa
3. **Examples**: Complete reference implementations and example configurations demonstrating typical use cases (e.g. monitoring of observation satellites, financial analysis, etc) are available in the GitHub repositories:
https://github.com/Daniel-Brosnan-Blazquez/s1boa
https://github.com/Daniel-Brosnan-Blazquez/s2boa
https://github.com/Daniel-Brosnan-Blazquez/s3boa
https://github.com/Daniel-Brosnan-Blazquez/bankboa

## Insertion Methods ##

EBOA supports various insertion methods at different hierarchical levels, allowing flexible data management based on different update strategies:

### Operation Level (DIM Signature) ###

These methods apply to all events and annotations linked to gauges associated with a specific DIM signature:

- **insert**: Basic insertion without any filtering or removal of existing data.
- **insert_and_erase**: Applies INSERT_and_ERASE logic to all events linked to all gauges associated with the DIM signature.
- **insert_and_erase_with_priority**: Same as insert_and_erase but using priority values to determine data relevance.
- **insert_and_erase_with_equal_or_lower_priority**: Similar to insert_and_erase_with_priority with specific priority handling.

### Event Level (Gauge) ###

These methods apply to individual events based on their gauge configuration:

- **SIMPLE_UPDATE**: All events are inserted without any filtering or removal.
- **INSERT_and_ERASE**: Events are inserted but initially flagged as not visible. The system keeps only events with the greatest generation time within the source's validity period, removing or splitting others as needed.
- **INSERT_and_ERASE_per_EVENT**: Similar to INSERT_and_ERASE, but the validity period is determined per individual event rather than the source.
- **INSERT_and_ERASE_with_PRIORITY**: Same as INSERT_and_ERASE, but uses priority values for source relevance.
- **INSERT_and_ERASE_per_EVENT_with_PRIORITY**: Combines per-event validity checking with priority-based selection.
- **INSERT_and_ERASE_with_EQUAL_or_LOWER_PRIORITY**: INSERT_and_ERASE with specific priority comparison logic.
- **INSERT_and_ERASE_INTERSECTED_EVENTS_with_PRIORITY**: Handles intersecting events with priority considerations.
- **EVENT_KEYS**: Events are inserted but flagged as not visible. Keeps events with the same event key and DIM signature that have the greatest generation time.
- **EVENT_KEYS_with_PRIORITY**: Same as EVENT_KEYS, but incorporates priority values.
- **SET_COUNTER**: For counter-type gauges, sets the counter value.
- **UPDATE_COUNTER**: For counter-type gauges, updates the existing counter value.

### Annotation Level (Annotation Configuration) ###

These methods apply to individual annotations:

- **SIMPLE_UPDATE**: All annotations are inserted without any filtering.
- **INSERT_and_ERASE**: Annotations are inserted with filtering based on validity periods and generation times.
- **INSERT_and_ERASE_with_PRIORITY**: Same as INSERT_and_ERASE for annotations, using priority values.

## Data Model ##

### Overview ###

EBOA implements a comprehensive relational data model designed to handle complex business operation data with maximum flexibility and temporal awareness. The data model is based on a solid relational foundation while supporting dynamic, schema-less value structures for unlimited data dimensionality.

The main entities managed by EBOA are:

- **Events**: Time-bounded measurements or observations of business operations
- **Annotations**: Descriptive information or properties associated with explicit references
- **Explicit References**: Unique identifiers for business entities (objects, systems, locations, etc.)
- **Sources**: Raw data ingestion sources and their metadata
- **Alerts**: Notifications triggered by system conditions or business rules
- **Reports**: Containers for analysis results and business intelligence outputs

### Entity Relationships ###

The EBOA data model defines complex relationships between entities:

![EBOA Data Model Diagram](doc/fig/eboadb.png)

Key relationships include:

- **Events to Explicit References**: Events can be associated with specific business entities
- **Events to Events**: Events can be linked to other events to represent causal or temporal relationships
- **Events to Gauges**: Each event is measured through a gauge configuration defining insertion behavior
- **Annotations to Explicit References**: Annotations enrich business entities with descriptive information
- **All Entities to Sources**: Complete traceability tracking which source generated each entity
- **All Entities to Alerts**: Alert generation and tracking at entity level

### Dynamic Value Structures ###

One of EBOA's most powerful features is the support for dynamic, hierarchical value structures. Events and annotations can contain arbitrarily complex nested data without schema modification.

![Values Hierarchical Structure](doc/fig/values_structure.jpg)

#### Supported Data Types ####

Values can be of the following types:

- **Text**: String values of any length
- **Boolean**: True/False values
- **Double**: Floating-point numeric values
- **Timestamp**: Date and time values
- **Geometry**: Geographic/spatial data (points, polygons, etc.)
- **Object**: Containers for nested hierarchical structures

#### Value Structure Organization ####

The dynamic value tree is organized hierarchically with the following characteristics:

- **Level-based hierarchy**: Each value has a `level_position` indicating its depth in the tree
- **Parent references**: Values maintain references to their parent through `parent_level` and `parent_position`
- **Unique addressing**: The combination of level and position enables unique identification of any value in the structure
- **Tree constraints**: Parent nodes must be of type "Object", while leaf nodes must be non-Object types
- **Unlimited nesting**: Values can contain other values, creating arbitrarily deep hierarchical structures

This design allows EBOA to store complex, semi-structured business data while maintaining full relational integrity and queryability.

### Temporal Tracking ###

EBOA maintains comprehensive temporal metadata for all entities:

- **Generation time**: When data was originally created
- **Reception time**: When data was received by EBOA
- **Processing time**: When data was processed
- **Validity period**: Start and stop times for which data is valid
- **Reported times**: Original times as reported by source systems

This enables sophisticated temporal queries and analysis of business operations over time.
