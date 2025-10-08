/********************************************************************************
 * Copyright (c) 2014-2019 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

#include <tahu.pb.h>

#include <time.h>
#include <sys/time.h>

#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#endif

#ifndef _SPARKPLUGLIB_H_
#define _SPARKPLUGLIB_H_

#ifdef __cplusplus
extern "C" {
#endif

// Enable/disable debug messages
#ifndef SPARKPLUG_DEBUG
#define SPARKPLUG_DEBUG 1
#endif

#if SPARKPLUG_DEBUG
#define DEBUG_PRINT(...) printf(__VA_ARGS__)
#else
#define DEBUG_PRINT(...) do {} while (0)
#endif

// Inttype format specifiers
#if defined(PB_FIELD_32BIT)
#define PRI_b_PB_SIZE   PRIb32
#define PRI_B_PB_SIZE   PRIB32
#define PRI_d_PB_SIZE   PRId32
#define PRI_i_PB_SIZE   PRIi32
#define PRI_o_PB_SIZE   PRIo32
#define PRI_u_PB_SIZE   PRIu32
#define PRI_x_PB_SIZE   PRIx32
#define PRI_X_PB_SIZE   PRIX32
#define SCN_b_PB_SIZE   SCNb32
#define SCN_d_PB_SIZE   SCNd32
#define SCN_i_PB_SIZE   SCNi32
#define SCN_o_PB_SIZE   SCNo32
#define SCN_u_PB_SIZE   SCNu32
#define SCN_x_PB_SIZE   SCNx32
#else
#define PRI_b_PB_SIZE   PRIbLEAST16
#define PRI_B_PB_SIZE   PRIBLEAST16
#define PRI_d_PB_SIZE   PRIdLEAST16
#define PRI_i_PB_SIZE   PRIiLEAST16
#define PRI_o_PB_SIZE   PRIoLEAST16
#define PRI_u_PB_SIZE   PRIuLEAST16
#define PRI_x_PB_SIZE   PRIxLEAST16
#define PRI_X_PB_SIZE   PRIXLEAST16
#define SCN_b_PB_SIZE   SCNbLEAST16
#define SCN_d_PB_SIZE   SCNdLEAST16
#define SCN_i_PB_SIZE   SCNiLEAST16
#define SCN_o_PB_SIZE   SCNoLEAST16
#define SCN_u_PB_SIZE   SCNuLEAST16
#define SCN_x_PB_SIZE   SCNxLEAST16
#endif

// Constants
#define DATA_SET_DATA_TYPE_UNKNOWN 0
#define DATA_SET_DATA_TYPE_INT8 1
#define DATA_SET_DATA_TYPE_INT16 2
#define DATA_SET_DATA_TYPE_INT32 3
#define DATA_SET_DATA_TYPE_INT64 4
#define DATA_SET_DATA_TYPE_UINT8 5
#define DATA_SET_DATA_TYPE_UINT16 6
#define DATA_SET_DATA_TYPE_UINT32 7
#define DATA_SET_DATA_TYPE_UINT64 8
#define DATA_SET_DATA_TYPE_FLOAT 9
#define DATA_SET_DATA_TYPE_DOUBLE 10
#define DATA_SET_DATA_TYPE_BOOLEAN 11
#define DATA_SET_DATA_TYPE_STRING 12
#define DATA_SET_DATA_TYPE_DATETIME 13
#define DATA_SET_DATA_TYPE_TEXT 14

#define METRIC_DATA_TYPE_UNKNOWN 0
#define METRIC_DATA_TYPE_INT8 1
#define METRIC_DATA_TYPE_INT16 2
#define METRIC_DATA_TYPE_INT32 3
#define METRIC_DATA_TYPE_INT64 4
#define METRIC_DATA_TYPE_UINT8 5
#define METRIC_DATA_TYPE_UINT16 6
#define METRIC_DATA_TYPE_UINT32 7
#define METRIC_DATA_TYPE_UINT64 8
#define METRIC_DATA_TYPE_FLOAT 9
#define METRIC_DATA_TYPE_DOUBLE 10
#define METRIC_DATA_TYPE_BOOLEAN 11
#define METRIC_DATA_TYPE_STRING 12
#define METRIC_DATA_TYPE_DATETIME 13
#define METRIC_DATA_TYPE_TEXT 14
#define METRIC_DATA_TYPE_UUID 15
#define METRIC_DATA_TYPE_DATASET 16
#define METRIC_DATA_TYPE_BYTES 17
#define METRIC_DATA_TYPE_FILE 18
#define METRIC_DATA_TYPE_TEMPLATE 19

#define PARAMETER_DATA_TYPE_UNKNOWN 0
#define PARAMETER_DATA_TYPE_INT8 1
#define PARAMETER_DATA_TYPE_INT16 2
#define PARAMETER_DATA_TYPE_INT32 3
#define PARAMETER_DATA_TYPE_INT64 4
#define PARAMETER_DATA_TYPE_UINT8 5
#define PARAMETER_DATA_TYPE_UINT16 6
#define PARAMETER_DATA_TYPE_UINT32 7
#define PARAMETER_DATA_TYPE_UINT64 8
#define PARAMETER_DATA_TYPE_FLOAT 9
#define PARAMETER_DATA_TYPE_DOUBLE 10
#define PARAMETER_DATA_TYPE_BOOLEAN 11
#define PARAMETER_DATA_TYPE_STRING 12
#define PARAMETER_DATA_TYPE_DATETIME 13
#define PARAMETER_DATA_TYPE_TEXT 14

#define PROPERTY_DATA_TYPE_UNKNOWN 0
#define PROPERTY_DATA_TYPE_INT8 1
#define PROPERTY_DATA_TYPE_INT16 2
#define PROPERTY_DATA_TYPE_INT32 3
#define PROPERTY_DATA_TYPE_INT64 4
#define PROPERTY_DATA_TYPE_UINT8 5
#define PROPERTY_DATA_TYPE_UINT16 6
#define PROPERTY_DATA_TYPE_UINT32 7
#define PROPERTY_DATA_TYPE_UINT64 8
#define PROPERTY_DATA_TYPE_FLOAT 9
#define PROPERTY_DATA_TYPE_DOUBLE 10
#define PROPERTY_DATA_TYPE_BOOLEAN 11
#define PROPERTY_DATA_TYPE_STRING 12
#define PROPERTY_DATA_TYPE_DATETIME 13
#define PROPERTY_DATA_TYPE_TEXT 14

/**
 * Attach Metadata to an existing Metric.
 *
 * <p>Caution: The metadata structure is duplicated via shallow copy, and
 * it is expected that any pointers within it are safe to pass to free().
 * This will happen if pb_release() is called on this structure or any
 * structure referencing it, for example via a call to free_payload().
 *
 * @param metric   Pointer to destination metric that metadata will be added to
 * @param metadata Pointer to a source metadata structure that will be copied onto metric
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int add_metadata_to_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                           org_eclipse_tahu_protobuf_Payload_MetaData *metadata);

/**
 * Attach a Metric to an existing Payload.
 *
 * <p>Caution: The metric structure is duplicated via shallow
 * copy, and it is expected that any pointers within it are safe
 * to pass to free(). This will happen if pb_release() is called
 * on this structure or any structure referencing it, for
 * example via a call to free_payload().
 *
 * @param payload Pointer to the destination payload that metric will be added to
 * @param metric  Pointer to source metric structure that will be copied onto payload
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int add_metric_to_payload(org_eclipse_tahu_protobuf_Payload *payload,
                          org_eclipse_tahu_protobuf_Payload_Metric *metric);

/**
 * Helper function to properly cast and push a value into the propertyvalue data structure.
 *
 * <p>Mostly useful when directly building property structures.
 *
 * (No pointers passed into this function are retained by the target structure)
 *
 * @param propertyvalue
 *                 Pointer to propertyvalue structure to receive the value
 * @param datatype Datatype of the value being received (e.g. PROPERTY_DATA_TYPE_INT8)
 * @param value    Pointer to the value to use (cannot be NULL)
 * @param size     Size of the memory pointed to by value
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int set_propertyvalue(org_eclipse_tahu_protobuf_Payload_PropertyValue *propertyvalue,
                      uint32_t datatype,
                      const void *value,
                      size_t size);

/**
 * Add a simple Property to an existing PropertySet
 *
 * (No pointers passed into this function are retained by the target structure)
 *
 * @param propertyset
 *               Pointer to destination PropertySet that property will be added to
 * @param key    Pointer to null-terminated string giving name of new property
 * @param type   Datatype of new property value (e.g. PROPERTY_DATA_TYPE_INT8)
 * @param value  Pointer to value to use for new property, or NULL if reported property value should be NULL.
 * @param size_of_value
 *               Size of data pointed to by value
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int add_property_to_set(org_eclipse_tahu_protobuf_Payload_PropertySet *propertyset,
                        const char *key,
                        uint32_t type,
                        const void *value,
                        size_t size_of_value);

/**
 * Add a PropertySet to an existing Metric
 *
 * <p>Caution: The propertyset structure is duplicated via shallow
 * copy, and it is expected that any pointers within it are safe
 * to pass to free(). This will happen if pb_release() is called
 * on this structure or any structure referencing it, for
 * example via a call to free_payload().
 *
 * @param metric     Pointer to the destination metric that propertyset will be added to
 * @param properties Pointer to source propertyset structure that will be copied onto metric
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int add_propertyset_to_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                              org_eclipse_tahu_protobuf_Payload_PropertySet *properties);

/**
 * Helper function to properly cast and push a value into the
 * metric data structure.
 *
 * <p>Mostly useful when directly building metric structures.
 *
 * <p>Caution: When using datatype METRIC_DATA_TYPE_DATASET or
 * METRIC_DATA_TYPE_TEMPLATE, the structure passed in via value
 * is duplicated using a shallow copy, and it is expected that
 * any pointers within it are safe to pass to free(). This will
 * happen if pb_release() is called on the metric or any
 * structure referencing it, for example via a call to
 * free_payload().
 *
 * When using other datatype values, no pointers are retained by the metric.
 *
 * @param metric
 *                 Pointer to metric structure to receive the
 *                 value
 * @param datatype Datatype of the value being received (e.g. PROPERTY_DATA_TYPE_INT8)
 * @param value    Pointer to the value to use (cannot be NULL)
 * @param size     Size of the memory pointed to by value
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int set_metric_value(org_eclipse_tahu_protobuf_Payload_Metric *metric, uint32_t datatype, const void *value, size_t size);

/**
 * Add a simple Metric to an existing Payload
 *
 * <p>Caution: When using datatype METRIC_DATA_TYPE_DATASET or
 * METRIC_DATA_TYPE_TEMPLATE, the structure passed in via value
 * is duplicated using a shallow copy, and it is expected that
 * any pointers within it are safe to pass to free(). This will
 * happen if pb_release() is called on the metric or any
 * structure referencing it, for example via a call to
 * free_payload().
 *
 * When using other datatype values, no pointers are retained by the metric.
 *
 * CAUTION: The underlying library will allocate memory as
 * needed when building the structure.  On success, it will be
 * necessary to call free_payload() on the structure to release
 * those allocations.
 *
 * @param payload   Pointer to the destination payload that metric will be added to
 * @param name      Pointer to null-terminated string giving name of metric; may be NULL if not using name field on this metric
 * @param has_alias Boolean indicating if the alias number should be included on the metric
 * @param alias     Alias number to use if has_alias is true
 * @param datatype  Datatype of the value (e.g. METRIC_DATA_TYPE_BOOLEAN)
 * @param is_historical
 *                  Boolean indicating if is_historical falg should be set on this metric
 * @param is_transient
 *                  Boolean if is_transient flag should be set on this metric
 * @param value     Pointer to value to use for metric; may be NULL if desired to set is_null flag and not include a value
 * @param size_of_value
 *                  Size of data pointed to by value
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int add_simple_metric(org_eclipse_tahu_protobuf_Payload *payload,
                      const char *name,
                      bool has_alias,
                      uint64_t alias,
                      uint64_t datatype,
                      bool is_historical,
                      bool is_transient,
                      const void *value,
                      size_t size_of_value);

/**
 * Encode a Payload into an array of bytes
 *
 * @param out_buffer Pointer to destination buffer to receive
 *                   the encoded payload, or NULL if you just
 *                   want to calculate the size of the encoded
 *                   payload
 * @param buffer_length
 *                   Size of the destination buffer in bytes
 * @param payload    Pointer to the source payload structure
 *
 * @return Returns the size of the encoded payload in bytes on
 *         success, or -1 on failure
 */
ssize_t encode_payload(uint8_t *out_buffer,
                       size_t buffer_length,
                       const org_eclipse_tahu_protobuf_Payload *payload);

/**
 * Build a payload structure from an encoded buffer
 *
 * <p>CAUTION: The underlying library will allocate memory as
 * needed when building the structure.  On success, it will be
 * necessary to call free_payload() on the structure to release
 * those allocations when done using it.
 *
 * @param payload   Pointer to the destination structure to receive the payload;
 *                  WARNING: any memory allocations referenced
 *                  by the payload structure before it is passed
 *                  into this function will be lost.  They
 *                  should be explicitly freed first if
 *                  necessary.
 * @param in_buffer Pointer to the buffer holding the encoded payload
 * @param buffer_length
 *                  Size of the incoming buffer
 *
 * @return Returns negative on failure, or number of bytes
 *         unused from buffer_length on success
 */
ssize_t decode_payload(org_eclipse_tahu_protobuf_Payload *payload,
                       const uint8_t *in_buffer,
                       size_t buffer_length);

/**
 * Free memory from an existing Payload
 *
 * <p>This walks through the payload structure and any sub-structures it references, and frees all pointers as dynamic allocations.
 *
 * <p>This does NOT release the payload structure itself.  It is up to the calling application to do that if necessary.
 *
 * @param payload Pointer to the Payload structure to release.
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int free_payload(org_eclipse_tahu_protobuf_Payload *payload);

/**
 * Get the current timestamp in milliseconds (format used inside SparkPlug payloads)
 *
 * @return The current timestamp in milliseconds since Jan 1, 1970 UTC.
 */
uint64_t get_current_timestamp(void);

/**
 * Reset the sequence number to 0.
 *
 * This should be used just before starting a new NBIRTH message.
 */
void reset_sparkplug_sequence(void);

/**
 * Get the next empty Payload.
 *
 * <p>This does the initial payload setup including the timestamp and sequence number.
 *
 * @param payload Pointer to the destination payload structure to setup;
 *                WARNING: any memory allocations referenced
 *                by the payload structure before it is passed
 *                into this function will be lost.  They
 *                should be explicitly freed first if
 *                necessary.
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int get_next_payload(org_eclipse_tahu_protobuf_Payload *payload);

/**
 * Initialize a Dataset with the values passed in
 *
 * <p>Caution: The row value structures are duplicated via
 * shallow copy, and it is expected that any pointers within
 * them are safe to pass to free(). This will happen if
 * pb_release() is called on this structure or any structure
 * referencing it, for example via a call to free_payload().
 *
 * @param dataset   Pointer to dataset to initialize
 *                  WARNING: any memory allocations referenced
 *                  by the dataset structure before it is passed
 *                  into this function will be lost.  They
 *                  should be explicitly freed first if
 *                  necessary.
 * @param num_of_rows
 *                  Number of rows in the dataset
 * @param num_of_columns
 *                  Number of columns in the dataset
 * @param datatypes Array of datatypes, one per column (e.g. DATA_SET_DATA_TYPE_INT8)
 * @param column_keys
 *                  Array of pointers to null-terminated strings
 *                  giving names for each column (these strings
 *                  are copied into new allocations)
 * @param row_data  Array of row value structures
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int init_dataset(org_eclipse_tahu_protobuf_Payload_DataSet *dataset,
                 uint64_t num_of_rows,
                 uint64_t num_of_columns,
                 const uint32_t datatypes[],
                 const char *column_keys[],
                 const org_eclipse_tahu_protobuf_Payload_DataSet_Row row_data[]);

/**
 * Initialize a Metric with the values of the arguments passed in
 *
 * <p>Caution: When using datatype METRIC_DATA_TYPE_DATASET or
 * METRIC_DATA_TYPE_TEMPLATE, the structure passed in via value
 * is duplicated using a shallow copy, and it is expected that
 * any pointers within it are safe to pass to free(). This will
 * happen if pb_release() is called on the metric or any
 * structure referencing it, for example via a call to
 * free_payload().
 *
 * <p>When using other datatype values, no pointers are retained by the metric.
 *
 * <p>CAUTION: The underlying library will allocate memory as
 * needed when building the structure.  On success, it will be
 * necessary to call free_payload() on the structure to release
 * those allocations.
 *
 * @param metric    Pointer to the metric data structure to initialize;
 *                  WARNING: any memory allocations referenced
 *                  by the metric structure before it is passed
 *                  into this function will be lost.  They
 *                  should be explicitly freed first if
 *                  necessary.
 * @param name      Pointer to null-terminated string giving name of metric; may be NULL if not using name field on this metric
 * @param has_alias Boolean indicating if the alias number should be included on the metric
 * @param alias     Alias number to use if has_alias is true
 * @param datatype  Datatype of the value (e.g. METRIC_DATA_TYPE_BOOLEAN)
 * @param is_historical
 *                  Boolean indicating if is_historical falg should be set on this metric
 * @param is_transient
 *                  Boolean if is_transient flag should be set on this metric
 * @param value     Pointer to value to use for metric; may be NULL if desired to set is_null flag and not include a value
 * @param size_of_value
 *                  Size of data pointed to by value
 *
 * @return Returns >= 0 on success, or negative on failure
 */
int init_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                const char *name,
                bool has_alias,
                uint64_t alias,
                uint64_t datatype,
                bool is_historical,
                bool is_transient,
                const void *value,
                size_t size_of_value);

/**
 * Display a full Sparkplug Payload
 *
 * @param payload Pointer to the payload structure to display
 */
void print_payload(org_eclipse_tahu_protobuf_Payload *payload);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif

