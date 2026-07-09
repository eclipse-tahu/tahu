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

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <pb_decode.h>
#include <pb_encode.h>
#include <tahu.h>
#include <tahu.pb.h>

static uint8_t payload_sequence;

int add_metadata_to_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                           org_eclipse_tahu_protobuf_Payload_MetaData *metadata) {
    DEBUG_PRINT("Adding metadata...\n");
    metric->has_metadata = true;
    memcpy(&metric->metadata, metadata, sizeof(metric->metadata));
    return 0;
}

int add_metric_to_payload(org_eclipse_tahu_protobuf_Payload *payload,
                          org_eclipse_tahu_protobuf_Payload_Metric *metric) {
    DEBUG_PRINT("Adding metric to payload...\n");
    const int old_count = payload->metrics_count;
    const int new_count = (old_count + 1);
    const size_t new_allocation_size = sizeof(org_eclipse_tahu_protobuf_Payload_Metric) * new_count;
    void *realloc_result = realloc(payload->metrics, new_allocation_size);
    //DEBUG_PRINT("realloc_result=%p\n", realloc_result);
    if (realloc_result == NULL) {
        fprintf(stderr, "realloc failed in add_metric_to_payload\n");
        return -1;
    }
    payload->metrics = realloc_result;
    payload->metrics_count = new_count;
    memcpy(&payload->metrics[old_count], metric, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    return 0;
}

int set_propertyvalue(org_eclipse_tahu_protobuf_Payload_PropertyValue *propertyvalue,
                      uint32_t datatype,
                      const void *value,
                      size_t size) {
    DEBUG_PRINT("Set property value...\n");
    switch (datatype) {
    case PROPERTY_DATA_TYPE_INT8:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag;
        propertyvalue->value.int_value = *(int8_t *)value;
        break;
    case PROPERTY_DATA_TYPE_INT16:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag;
        propertyvalue->value.int_value = *(int16_t *)value;
        break;
    case PROPERTY_DATA_TYPE_INT32:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag;
        propertyvalue->value.int_value = *(int32_t *)value;
        break;
    case PROPERTY_DATA_TYPE_INT64:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_long_value_tag;
        propertyvalue->value.long_value = *(int64_t *)value;
        break;
    case PROPERTY_DATA_TYPE_UINT8:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag;
        propertyvalue->value.int_value = *(uint8_t *)value;
        break;
    case PROPERTY_DATA_TYPE_UINT16:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag;
        propertyvalue->value.int_value = *(uint16_t *)value;
        break;
    case PROPERTY_DATA_TYPE_UINT32:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_long_value_tag;
        propertyvalue->value.long_value = *(uint32_t *)value;
        break;
    case PROPERTY_DATA_TYPE_UINT64:
    case PROPERTY_DATA_TYPE_DATETIME:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_long_value_tag;
        propertyvalue->value.long_value = *(uint64_t *)value;
        break;
    case PROPERTY_DATA_TYPE_FLOAT:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_float_value_tag;
        propertyvalue->value.float_value = *(float *)value;
        break;
    case PROPERTY_DATA_TYPE_DOUBLE:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_double_value_tag;
        propertyvalue->value.double_value = *(double *)value;
        break;
    case PROPERTY_DATA_TYPE_BOOLEAN:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_boolean_value_tag;
        propertyvalue->value.boolean_value = *(bool *)value;
        break;
    case PROPERTY_DATA_TYPE_STRING:
    case PROPERTY_DATA_TYPE_TEXT:
        propertyvalue->which_value = org_eclipse_tahu_protobuf_Payload_PropertyValue_string_value_tag;
        propertyvalue->value.string_value = strndup(value, size);
        break;
    default:
        fprintf(stderr, "Invalid datatype(%"PRIu32") in set_propertyvalue\n", datatype);
        return -1;
    }
    return 0;
}

int add_property_to_set(org_eclipse_tahu_protobuf_Payload_PropertySet *propertyset,
                        const char *key,
                        uint32_t datatype,
                        const void *value,
                        size_t size_of_value) {
    DEBUG_PRINT("Add property to set...\n");
    if (propertyset->keys_count != propertyset->values_count) {
        fprintf(stderr, "Mismatched key/value counts in add_property_to_set\n");
        return -1;
    }
    const int old_count = propertyset->keys_count;
    const int new_count = (old_count + 1);
    const size_t key_allocation_size = sizeof(char *) * new_count;
    const size_t value_allocation_size = sizeof(org_eclipse_tahu_protobuf_Payload_PropertyValue) * new_count;
    void *key_allocation_result = realloc(propertyset->keys, key_allocation_size);
    void *value_allocation_result = realloc(propertyset->values, value_allocation_size);
    //DEBUG_PRINT("key=%p value=%p\n", key_allocation_result, value_allocation_result);
    if (key_allocation_result != NULL) {
        propertyset->keys = key_allocation_result;
    }
    if (value_allocation_result != NULL) {
        propertyset->values = value_allocation_result;
    }
    if ((key_allocation_result == NULL) || (value_allocation_result == NULL)) {
        fprintf(stderr, "realloc failed in add_metric_to_payload\n");
        return -1;
    }
    propertyset->keys_count = new_count;
    propertyset->values_count = new_count;
    propertyset->keys[old_count] = strdup(key);
    if (propertyset->keys[old_count] == NULL) {
        fprintf(stderr, "strdup failed in add_metric_to_payload\n");
        return -1;
    }
    memset(&propertyset->values[old_count], 0, sizeof(org_eclipse_tahu_protobuf_Payload_PropertyValue));
    propertyset->values[old_count].has_type = true;
    propertyset->values[old_count].type = datatype;
    if (value == NULL) {
        propertyset->values[old_count].has_is_null = true;
        propertyset->values[old_count].is_null = true;
    } else {
        set_propertyvalue(&propertyset->values[old_count], datatype, value, size_of_value);
    }
    return 0;
}

int add_propertyset_to_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                              org_eclipse_tahu_protobuf_Payload_PropertySet *properties) {
    DEBUG_PRINT("Add propertyset to metric...\n");
    metric->has_properties = true;
    memcpy(&metric->properties, properties, sizeof(metric->properties));
    return 0;
}

int set_metric_value(org_eclipse_tahu_protobuf_Payload_Metric *metric, uint32_t datatype, const void *value, size_t size) {
    DEBUG_PRINT("Set metric value...\n");
    switch (datatype) {
    case METRIC_DATA_TYPE_INT8:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag;
        metric->value.int_value = *(int8_t *)value;
        break;
    case METRIC_DATA_TYPE_INT16:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag;
        metric->value.int_value = *(int16_t *)value;
        break;
    case METRIC_DATA_TYPE_INT32:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag;
        metric->value.int_value = *(int32_t *)value;
        break;
    case METRIC_DATA_TYPE_INT64:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_long_value_tag;
        metric->value.long_value = *(int64_t *)value;
        break;
    case METRIC_DATA_TYPE_UINT8:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag;
        metric->value.int_value = *(uint8_t *)value;
        break;
    case METRIC_DATA_TYPE_UINT16:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag;
        metric->value.int_value = *(uint16_t *)value;
        break;
    case METRIC_DATA_TYPE_UINT32:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_long_value_tag;
        metric->value.long_value = *(uint32_t *)value;
        break;
    case METRIC_DATA_TYPE_UINT64:
    case METRIC_DATA_TYPE_DATETIME:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_long_value_tag;
        metric->value.long_value = *(uint64_t *)value;
        break;
    case METRIC_DATA_TYPE_FLOAT:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_float_value_tag;
        metric->value.float_value = *(float *)value;
        break;
    case METRIC_DATA_TYPE_DOUBLE:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_double_value_tag;
        metric->value.double_value = *(double *)value;
        break;
    case METRIC_DATA_TYPE_BOOLEAN:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_boolean_value_tag;
        metric->value.boolean_value = *(bool *)value;
        break;
    case METRIC_DATA_TYPE_STRING:
    case METRIC_DATA_TYPE_TEXT:
    case METRIC_DATA_TYPE_UUID:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_string_value_tag;
        metric->value.string_value = strndup(value, size);
        break;
    case METRIC_DATA_TYPE_DATASET:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_dataset_value_tag;
        memcpy(&metric->value.dataset_value, value, sizeof(metric->value.dataset_value));
        break;
    case METRIC_DATA_TYPE_TEMPLATE:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_template_value_tag;
        memcpy(&metric->value.template_value, value, sizeof(metric->value.template_value));
        break;
    case METRIC_DATA_TYPE_BYTES:
        metric->which_value = org_eclipse_tahu_protobuf_Payload_Metric_bytes_value_tag;
        metric->value.bytes_value = (pb_bytes_array_t *)value;
        break;
    case METRIC_DATA_TYPE_FILE:
    case METRIC_DATA_TYPE_UNKNOWN:
    default:
        fprintf(stderr, "Unhandled datatype(%"PRIu32") in set_metric_value\n", datatype);
        return -1;
    }
    return 0;
}

int add_simple_metric(org_eclipse_tahu_protobuf_Payload *payload,
                      const char *name,
                      bool has_alias,
                      uint64_t alias,
                      uint64_t datatype,
                      bool is_historical,
                      bool is_transient,
                      const void *value,
                      size_t size_of_value) {
    DEBUG_PRINT("Add simple metric...\n");
    org_eclipse_tahu_protobuf_Payload_Metric new_metric;
    memset(&new_metric, 0, sizeof(new_metric));
    if (name != NULL) {
        new_metric.name = strdup(name);
        if (new_metric.name == NULL) {
            fprintf(stderr, "strdup name failed in add_simple_metric\n");
            return -1;
        }
    }
    new_metric.has_alias = has_alias;
    new_metric.alias = alias;
    new_metric.has_timestamp = true;
    new_metric.timestamp = get_current_timestamp();
    new_metric.has_datatype = true;
    new_metric.datatype = datatype;
    if (is_historical) {
        new_metric.has_is_historical = true;
        new_metric.is_historical = true;
    }
    if (is_transient) {
        new_metric.has_is_transient = true;
        new_metric.is_transient = true;
    }
    if (value == NULL) {
        new_metric.has_is_null = true;
        new_metric.is_null = true;
    } else {
        set_metric_value(&new_metric, datatype, value, size_of_value);
    }
    add_metric_to_payload(payload, &new_metric);
    return 0;
}

ssize_t encode_payload(uint8_t *out_buffer,
                       size_t buffer_length,
                       const org_eclipse_tahu_protobuf_Payload *payload) {
    // Use a different stream if the user wants a normal encode or just a size check
    pb_ostream_t sizing_stream = PB_OSTREAM_SIZING;
    pb_ostream_t buffer_stream = pb_ostream_from_buffer(out_buffer, buffer_length);
    pb_ostream_t *node_stream = ((out_buffer == NULL) ? &sizing_stream : &buffer_stream);

    // Encode the payload
    DEBUG_PRINT("Encoding payload...\n");
    const bool encode_result = pb_encode(node_stream, org_eclipse_tahu_protobuf_Payload_fields, payload);
    const size_t message_length = node_stream->bytes_written;
    DEBUG_PRINT("Message length: %zu\n", message_length);

    // Error Check
    if (!encode_result) {
        fprintf(stderr, "Encoding failed: %s\n", PB_GET_ERROR(node_stream));
        return -1;
    }

    DEBUG_PRINT("Encoding succeeded\n");
    return message_length;
}

ssize_t decode_payload(org_eclipse_tahu_protobuf_Payload *payload,
                       const uint8_t *in_buffer,
                       size_t buffer_length) {
    DEBUG_PRINT("Decoding payload...\n");
    pb_istream_t node_stream = pb_istream_from_buffer(in_buffer, buffer_length);
    memset(payload, 0, sizeof(org_eclipse_tahu_protobuf_Payload));
    const bool decode_result = pb_decode(&node_stream, org_eclipse_tahu_protobuf_Payload_fields, payload);

    if (!decode_result) {
        fprintf(stderr, "Decoding failed: %s\n", PB_GET_ERROR(&node_stream));
        return -1;
    }

#ifdef SPARKPLUG_DEBUG
    // Print the message data
    print_payload(payload);
#endif

    return node_stream.bytes_left;
}

int free_payload(org_eclipse_tahu_protobuf_Payload *payload) {
    DEBUG_PRINT("Free payload memory...\n");
    pb_release(org_eclipse_tahu_protobuf_Payload_fields, payload);
    return 0;
}

uint64_t get_current_timestamp() {
    // Set the timestamp
    struct timespec ts;
#ifdef __MACH__ // OS X does not have clock_gettime, use clock_get_time
    clock_serv_t cclock;
    mach_timespec_t mts;
    host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
    clock_get_time(cclock, &mts);
    mach_port_deallocate(mach_task_self(), cclock);
    ts.tv_sec = mts.tv_sec;
    ts.tv_nsec = mts.tv_nsec;
#else
    clock_gettime(CLOCK_REALTIME, &ts);
#endif
    return ts.tv_sec * UINT64_C(1000) + ts.tv_nsec / 1000000;
}

void reset_sparkplug_sequence(void) {
    payload_sequence = 0;
}

int get_next_payload(org_eclipse_tahu_protobuf_Payload *payload) {

    // Initialize payload
    DEBUG_PRINT("Current Sequence Number: %"PRIu8"\n", payload_sequence);
    memset(payload, 0, sizeof(org_eclipse_tahu_protobuf_Payload));
    payload->has_timestamp = true;
    payload->timestamp = get_current_timestamp();
    payload->has_seq = true;
    payload->seq = payload_sequence;

    // Increment/wrap the sequence number (stored in a U8, so it
    // will wrap 255-to-0 automatically)
    payload_sequence++;
    return 0;
}

int init_dataset(org_eclipse_tahu_protobuf_Payload_DataSet *dataset,
                 uint64_t num_of_rows,
                 uint64_t num_of_columns,
                 const uint32_t datatypes[],
                 const char *column_keys[],
                 const org_eclipse_tahu_protobuf_Payload_DataSet_Row row_data[]) {
    DEBUG_PRINT("Init dataset...\n");
    memset(dataset, 0, sizeof(org_eclipse_tahu_protobuf_Payload_DataSet));
    dataset->has_num_of_columns = true;
    dataset->num_of_columns = num_of_columns;
    dataset->columns_count = num_of_columns;
    if (num_of_columns == 0) {
        dataset->columns = NULL;
    } else {
        const size_t key_size = num_of_columns * sizeof(char *);
        dataset->columns = malloc(key_size);
        if (dataset->columns == NULL) {
            fprintf(stderr, "malloc(%zu) failure in init_dataset\n", key_size);
            return -1;
        }
    }
    for (int i = 0; i < num_of_columns; i++) {
        dataset->columns[i] = strdup(column_keys[i]);
        if (dataset->columns[i] == NULL) {
            // Initialize remaining columns[] pointers.
            while (i++ < num_of_columns) {
                dataset->columns[i] = NULL;
            }
            fprintf(stderr, "strdup failed in init_dataset\n");
            return -1;
        }
    }
    dataset->types_count = num_of_columns;
    if (num_of_columns == 0) {
        dataset->types = NULL;
    } else {
        const size_t datatypes_size = num_of_columns * sizeof(uint32_t);
        dataset->types = malloc(datatypes_size);
        if (dataset->types == NULL) {
            fprintf(stderr, "malloc(%zu) failure in init_dataset\n", datatypes_size);
            return -1;
        }
        memcpy(dataset->types, datatypes, datatypes_size);
    }
    dataset->rows_count = num_of_rows;
    if (num_of_rows == 0) {
        dataset->rows = NULL;
    } else {
        const size_t row_data_size = num_of_rows * sizeof(org_eclipse_tahu_protobuf_Payload_DataSet_Row);
        dataset->rows = malloc(row_data_size);
        if (dataset->rows == NULL) {
            fprintf(stderr, "malloc(%zu) failure in init_dataset\n", row_data_size);
            return -1;
        }
        memcpy(dataset->rows, row_data, row_data_size);
    }
    return 0;
}

int init_metric(org_eclipse_tahu_protobuf_Payload_Metric *metric,
                const char *name,
                bool has_alias,
                uint64_t alias,
                uint64_t datatype,
                bool is_historical,
                bool is_transient,
                const void *value,
                size_t size_of_value) {
    DEBUG_PRINT("Init metric...\n");
    memset(metric, 0, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    if (name != NULL) {
        metric->name = strdup(name);
        if (metric->name == NULL) {
            fprintf(stderr, "strdup failed to copy metric name\n");
            return -1;
        }
    }
    if (has_alias) {
        metric->has_alias = true;
        metric->alias = alias;
    }
    if (is_historical && !is_transient) {
        metric->has_timestamp = true;
        metric->timestamp = get_current_timestamp();
    }
    metric->has_datatype = true;
    metric->datatype = datatype;
    if (is_historical) {
        metric->has_is_historical = true;
        metric->is_historical = true;
    }
    if (is_transient) {
        metric->has_is_transient = true;
        metric->is_transient = true;
    }
    if (value == NULL) {
        metric->has_is_null = true;
        metric->is_null = true;
    } else {
        return set_metric_value(metric, datatype, value, size_of_value);
    }
    // No support for metadata or properties in this function...
    return 0;
}

/*
 * Display a full Sparkplug Payload
 */
#define PP(...) fprintf(stdout,__VA_ARGS__)
#define EMPTY_PREFIX ""
void print_metadata(const char *prefix, org_eclipse_tahu_protobuf_Payload_MetaData *metadata);
void print_propertyvalue(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertyValue *value);
void print_propertyset(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertySet *properties);
void print_propertysetlist(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertySetList *propertysetlist);
void print_dataset_row(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet_Row *row);
void print_datasetvalue(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue *dsvalue);
void print_dataset(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet *dataset_value);
void print_template_parameter(const char *prefix, org_eclipse_tahu_protobuf_Payload_Template_Parameter *template_parameter);
void print_template(const char *prefix, org_eclipse_tahu_protobuf_Payload_Template *template);
void print_metric(const char *prefix, org_eclipse_tahu_protobuf_Payload_Metric *metric);
void print_payload(org_eclipse_tahu_protobuf_Payload *payload);

void print_metadata(const char *prefix, org_eclipse_tahu_protobuf_Payload_MetaData *metadata) {
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    if (metadata->has_is_multi_part) {
        PP("%sis_multi_part=%u\n", prefix, metadata->is_multi_part);
    }
    if (metadata->content_type != NULL) {
        PP("%scontent_type=%s [%p]\n", prefix, metadata->content_type, metadata->content_type);
    }
    if (metadata->has_size) {
        PP("%shas_size=%"PRIu64"\n", prefix, metadata->size);
    }
    if (metadata->has_seq) {
        PP("%sseq=%"PRIu64"\n", prefix, metadata->seq);
    }
    if (metadata->file_name != NULL) {
        PP("%sfile_name=%s [%p]\n", prefix, metadata->file_name, metadata->file_name);
    }
    if (metadata->file_type != NULL) {
        PP("%sfile_type=%s [%p]\n", prefix, metadata->file_type, metadata->file_type);
    }
    if (metadata->md5 != NULL) {
        PP("%smd5=%s [%p]\n", prefix, metadata->md5, metadata->md5);
    }
    if (metadata->description != NULL) {
        PP("%sdescription=%s [%p]\n", prefix, metadata->description, metadata->description);
    }
    if (metadata->extensions != NULL) {
        PP("%sextensions=[%p] (display not supported)\n", prefix, metadata->extensions);
    }
}

void print_propertyvalue(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertyValue *value) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    if (value->has_type) {
        PP("%stype=%"PRIu32"\n", prefix, value->type);
    }
    if (value->has_is_null) {
        PP("%sis_null=%u\n", prefix, value->is_null);
    }
    if (!value->is_null) {
      switch (value->which_value) {
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_int_value_tag:
	  PP("%sint_value=%"PRId32"\n", prefix, value->value.int_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_long_value_tag:
	  PP("%slong_value=%"PRId64"\n", prefix, value->value.long_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_float_value_tag:
	  PP("%sfloat_value=%f\n", prefix, value->value.float_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_double_value_tag:
	  PP("%sdouble_value=%f\n", prefix, value->value.double_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_boolean_value_tag:
	  PP("%sboolean_value=%u\n", prefix, value->value.boolean_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_string_value_tag:
	  PP("%sstring_value=%s [%p]\n", prefix, value->value.string_value, value->value.string_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_propertyset_value_tag:
	  snprintf(temp, sizeof(temp), "%spropertyset.", prefix);
	  print_propertyset(temp, &value->value.propertyset_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_propertysets_value_tag:
	  snprintf(temp, sizeof(temp), "%spropertysets.", prefix);
	  print_propertysetlist(temp, &value->value.propertysets_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_PropertyValue_extension_value_tag:
	  PP("%sextension_value=[%p] (display not supported)\n", prefix, value->value.extension_value.extensions);
	  break;
      default:
	  PP("%sinvalid which_value=%"PRI_u_PB_SIZE"\n", prefix, value->which_value);
      }
    }
}

void print_propertyset(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertySet *properties) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    PP("%skeys=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, properties->keys, properties->keys_count);
    for (pb_size_t i = 0; i < properties->keys_count; i++) {
        PP("%s keys[%"PRI_u_PB_SIZE"]=%s [%p]\n", prefix, i, properties->keys[i], properties->keys[i]);
    }
    PP("%svalues=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, properties->values, properties->values_count);
    for (pb_size_t i = 0; i < properties->values_count; i++) {
        snprintf(temp, sizeof(temp), "%svalues[%"PRI_u_PB_SIZE"].", prefix, i);
        print_propertyvalue(temp, &properties->values[i]);
    }
    if (properties->extensions != NULL) {
        PP("%sextension=[%p] (display not supported)\n", prefix, properties->extensions);
    }
}

void print_propertysetlist(const char *prefix, org_eclipse_tahu_protobuf_Payload_PropertySetList *propertysetlist) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    // pb_size_t propertyset_count;
    // struct _org_eclipse_tahu_protobuf_Payload_PropertySet *propertyset;
    PP("%spropertyset=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, propertysetlist->propertyset, propertysetlist->propertyset_count);
    for (pb_size_t i = 0; i < propertysetlist->propertyset_count; i++) {
        snprintf(temp, sizeof(temp), "%s.propertyset[%"PRI_u_PB_SIZE"].", prefix, i);
        print_propertyset(temp, &propertysetlist->propertyset[i]);
    }
    if (propertysetlist->extensions != NULL) {
        PP("%sextensions=[%p] (display not supported)\n", prefix, propertysetlist->extensions);
    }
}

void print_dataset_row(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet_Row *row) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    PP("%selements=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, row->elements, row->elements_count);
    for (pb_size_t i = 0; i < row->elements_count; i++) {
        snprintf(temp, sizeof(temp), "%selements[%"PRI_u_PB_SIZE"].", prefix, i);
        print_datasetvalue(temp, &row->elements[i]);
    }
    if (row->extensions != NULL) {
        PP("%selements=[%p] (display not supported)\n", prefix, row->extensions);
    }
}

void print_datasetvalue(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue *dsvalue) {
    switch (dsvalue->which_value) {
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag:
        PP("%sint_value=%"PRId32"\n", prefix, dsvalue->value.int_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_long_value_tag:
        PP("%slong_value=%"PRId64"\n", prefix, dsvalue->value.long_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_float_value_tag:
        PP("%sfloat_value=%f\n", prefix, dsvalue->value.float_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_double_value_tag:
        PP("%sdouble_value=%f\n", prefix, dsvalue->value.double_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_boolean_value_tag:
        PP("%sboolean_value=%u\n", prefix, dsvalue->value.boolean_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_string_value_tag:
        PP("%sstring_value=%s [%p]\n", prefix, dsvalue->value.string_value, dsvalue->value.string_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_extension_value_tag:
        PP("%sextension_value=[%p] (display not supported)\n", prefix, dsvalue->value.extension_value.extensions);
        break;
    default:
        PP("%sinvalid which_value=%"PRI_u_PB_SIZE"\n", prefix, dsvalue->which_value);
    }
}

void print_dataset(const char *prefix, org_eclipse_tahu_protobuf_Payload_DataSet *dataset_value) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    if (dataset_value->has_num_of_columns) {
        PP("%snum_of_columns=%"PRIu64"\n", prefix, dataset_value->num_of_columns);
    }
    PP("%scolumns=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, dataset_value->columns, dataset_value->columns_count);
    for (pb_size_t i = 0; i < dataset_value->columns_count; i++) {
        PP("%scolumn[%"PRI_u_PB_SIZE"]=%s [%p]\n", prefix, i, dataset_value->columns[i], dataset_value->columns[i]);
    }
    PP("%stypes=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, dataset_value->types, dataset_value->types_count);
    for (pb_size_t i = 0; i < dataset_value->types_count; i++) {
        PP("%stype[%"PRI_u_PB_SIZE"]=%"PRIu32"\n", prefix, i, dataset_value->types[i]);
    }
    PP("%srows=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, dataset_value->rows, dataset_value->rows_count);
    for (pb_size_t i = 0; i < dataset_value->rows_count; i++) {
        snprintf(temp, sizeof(temp), "%srow[%"PRI_u_PB_SIZE"].", prefix, i);
        print_dataset_row(temp, &dataset_value->rows[i]);
    }
    if (dataset_value->extensions != NULL) {
        PP("%sextensions=[%p]\n", prefix, dataset_value->extensions);
    }
}

void print_template_parameter(const char *prefix, org_eclipse_tahu_protobuf_Payload_Template_Parameter *template_parameter) {
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    // char *name;
    if (template_parameter->name != NULL) {
        PP("%sname=%s [%p]\n", prefix, template_parameter->name, template_parameter->name);
    }
    if (template_parameter->has_type) {
        PP("%stype=%"PRIu32"\n", prefix, template_parameter->type);
    }
    // pb_size_t which_value;
    switch (template_parameter->which_value) {
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_int_value_tag:
        PP("%sint_value=%"PRId32"\n", prefix, template_parameter->value.int_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_long_value_tag:
        PP("%slong_value=%"PRId64"\n", prefix, template_parameter->value.long_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_float_value_tag:
        PP("%sfloat_value=%f\n", prefix, template_parameter->value.float_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_double_value_tag:
        PP("%sdouble_value=%f\n", prefix, template_parameter->value.double_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_boolean_value_tag:
        PP("%sboolean_value=%u\n", prefix, template_parameter->value.boolean_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_string_value_tag:
        PP("%sstring_value=%s [%p]\n", prefix, template_parameter->value.string_value, template_parameter->value.string_value);
        break;
    case org_eclipse_tahu_protobuf_Payload_Template_Parameter_extension_value_tag:
        PP("%sextension_value=[%p] (display not supported)\n", prefix, template_parameter->value.extension_value.extensions);
        break;
    default:
        PP("%sinvalid which_value=%"PRI_u_PB_SIZE"\n", prefix, template_parameter->which_value);
    }
}

void print_template(const char *prefix, org_eclipse_tahu_protobuf_Payload_Template *template) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    if (template->version != NULL) {
        PP("%sversion=%s [%p]\n", prefix, template->version, template->version);
    }
    PP("%smetrics=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, template->metrics, template->metrics_count);
    for (pb_size_t i = 0; i < template->metrics_count; i++) {
        snprintf(temp, sizeof(temp), "%smetric[%"PRI_u_PB_SIZE"].", prefix, i);
        print_metric(temp, &template->metrics[i]);
    }
    PP("%sparameters=[%p] (count=%"PRI_u_PB_SIZE")\n", prefix, template->parameters, template->parameters_count);
    for (pb_size_t i = 0; i < template->parameters_count; i++) {
        snprintf(temp, sizeof(temp), "%sparameter[%"PRI_u_PB_SIZE"].", prefix, i);
        print_template_parameter(temp, &template->parameters[i]);
    }
    if (template->template_ref != NULL) {
        PP("%stemplate_ref=%s [%p]\n", prefix, template->template_ref, template->template_ref);
    }
    if (template->has_is_definition) {
        PP("%sis_definition=%u\n", prefix, template->is_definition);
    }
    if (template->extensions != NULL) {
        PP("%sextensions=[%p] (display not supported)\n", prefix, template->extensions);
    }
}

void print_metric(const char *prefix, org_eclipse_tahu_protobuf_Payload_Metric *metric) {
    char temp[64];
    if (prefix == NULL) {
        prefix = EMPTY_PREFIX;
    }
    if (metric->name != NULL) {
        PP("%sname=%s [%p]\n", prefix, metric->name, metric->name);
    }
    if (metric->has_alias) {
        PP("%salias=%"PRIu64"\n", prefix, metric->alias);
    }
    if (metric->has_timestamp) {
        PP("%stimestamp=%"PRIu64"\n", prefix, metric->timestamp);
    }
    if (metric->has_datatype) {
        PP("%sdatatype=%"PRIu32"\n", prefix, metric->datatype);
    }
    if (metric->has_is_historical) {
        PP("%sis_historical=%u\n", prefix, metric->is_historical);
    }
    if (metric->has_is_transient) {
        PP("%sis_transient=%u\n", prefix, metric->is_transient);
    }
    if (metric->has_is_null) {
        PP("%sis_null=%u\n", prefix, metric->is_null);
    }
    if (metric->has_metadata) {
        snprintf(temp, sizeof(temp), "%smetadata.", prefix);
        print_metadata(temp, &metric->metadata);
    }
    if (metric->has_properties) {
        snprintf(temp, sizeof(temp), "%sproperties.", prefix);
        print_propertyset(temp, &metric->properties);
    }
    if (!metric->is_null) {
      switch (metric->which_value) {
      case org_eclipse_tahu_protobuf_Payload_Metric_int_value_tag:
	  PP("%sint_value=%"PRId32"\n", prefix, metric->value.int_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_long_value_tag:
	  PP("%slong_value=%"PRId64"\n", prefix, metric->value.long_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_float_value_tag:
	  PP("%sfloat_value=%f\n", prefix, metric->value.float_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_double_value_tag:
	  PP("%sdouble_value=%f\n", prefix, metric->value.double_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_boolean_value_tag:
	  PP("%sboolean_value=%d\n", prefix, metric->value.boolean_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_string_value_tag:
	  PP("%sstring_value=%s [%p]\n", prefix, metric->value.string_value, metric->value.string_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_dataset_value_tag:
	  snprintf(temp, sizeof(temp), "%sdataset.", prefix);
	  print_dataset(temp, &metric->value.dataset_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_bytes_value_tag:
	  PP("%sbytes_value=[%p] (display not supported)\n", prefix, metric->value.bytes_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_template_value_tag:
	  snprintf(temp, sizeof(temp), "%stemplate.", prefix);
	  print_template(temp, &metric->value.template_value);
	  break;
      case org_eclipse_tahu_protobuf_Payload_Metric_extension_value_tag:
	  PP("%sextension_value=[%p] (display not supported)\n", prefix, metric->value.extension_value.extensions);
	  break;
      default:
	  PP("%sinvalid which_type=%"PRI_u_PB_SIZE"\n", prefix, metric->which_value);
	  break;
      }
    }
}

void print_payload(org_eclipse_tahu_protobuf_Payload *payload) {
    char temp[64];
    PP("-----PAYLOAD BEGIN-----\n");
    if (payload->has_timestamp) {
        PP("timestamp=%"PRIu64"\n", payload->timestamp);
    }
    if (payload->has_seq) {
        PP("seq=%"PRIu64"\n", payload->seq);
    }
    if (payload->uuid != NULL) {
        PP("uuid=%s [%p]\n", payload->uuid, payload->uuid);
    }
    if (payload->body != NULL) {
        PP("body=[%p] (display not supported)\n", payload->body);
    }
    if (payload->extensions != NULL) {
        PP("extensions=[%p] (display not supported)\n", payload->extensions);
    }
    PP("metrics=[%p] (count=%"PRI_u_PB_SIZE")\n", payload->metrics, payload->metrics_count);
    for (pb_size_t i = 0; i < payload->metrics_count; i++) {
        snprintf(temp, sizeof(temp), "metric[%"PRI_u_PB_SIZE"].", i);
        print_metric(temp, &payload->metrics[i]);
    }
    PP("-----PAYLOAD END-----\n");
}
