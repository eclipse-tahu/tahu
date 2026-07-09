/********************************************************************************
 * Copyright (c) 2014, 2018 Cirrus Link Solutions and others
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
#include <unistd.h>
#include <tahu.h>
#include <tahu.pb.h>
#include <pb_decode.h>
#include <pb_encode.h>
#include <mosquitto.h>
#include <inttypes.h>

/* Mosquitto Callbacks */
void my_message_callback(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message);
void my_connect_callback(struct mosquitto *mosq, void *userdata, int result);
void my_subscribe_callback(struct mosquitto *mosq, void *userdata, int mid, int qos_count, const int *granted_qos);
void my_log_callback(struct mosquitto *mosq, void *userdata, int level, const char *str);

/* Local Functions */
void publisher(struct mosquitto *mosq, char *topic, void *buf, unsigned len);
void publish_births(struct mosquitto *mosq);
void publish_node_birth(struct mosquitto *mosq);
void publish_device_birth(struct mosquitto *mosq);
void publish_ddata_message(struct mosquitto *mosq);

enum alias_map {
    Next_Server = 0,
    Rebirth = 1,
    Reboot = 2,
    Dataset = 3,
    Node_Metric0 = 4,
    Node_Metric1 = 5,
    Node_Metric2 = 6,
    Device_Metric0 = 7,
    Device_Metric1 = 8,
    Device_Metric2 = 9,
    Device_Metric3 = 10,
    My_Custom_Motor = 11
};

int main(int argc, char *argv[]) {

    // MQTT Parameters
    char *host = "localhost";
    int port = 1883;
    int keepalive = 60;
    bool clean_session = true;
    struct mosquitto *mosq = NULL;

    // MQTT Setup
    srand(time(NULL));
    mosquitto_lib_init();
    mosq = mosquitto_new(NULL, clean_session, NULL);
    if (!mosq) {
        fprintf(stderr, "Error: Out of memory.\n");
        return 1;
    }
    mosquitto_log_callback_set(mosq, my_log_callback);
    mosquitto_connect_callback_set(mosq, my_connect_callback);
    mosquitto_message_callback_set(mosq, my_message_callback);
    mosquitto_subscribe_callback_set(mosq, my_subscribe_callback);
    mosquitto_username_pw_set(mosq, "admin", "changeme");
    mosquitto_will_set(mosq, "spBv1.0/Sparkplug B Devices/NDEATH/C Edge Node 1", 0, NULL, 0, false);

    // Optional SSL parameters for MQTT
    //mosquitto_tls_insecure_set(mosq, true);
    //mosquitto_tls_opts_set(mosq, 0, "tlsv1.2", NULL);               // 0 is DO NOT SSL_VERIFY_PEER

    // MQTT Connect
    if (mosquitto_connect(mosq, host, port, keepalive)) {
        fprintf(stderr, "Unable to connect.\n");
        return 1;
    }

    // Publish the NBIRTH and DBIRTH Sparkplug messages (Birth Certificates)
    publish_births(mosq);

    // Loop and publish more DDATA messages every 5 seconds.  Note this should only be done in real/production
    // scenarios with change events on inputs.  Because Sparkplug ensures state there is no reason to send DDATA
    // messages unless the state of a I/O point has changed.
    int i;
    for (i = 0; i < 100; i++) {
        publish_ddata_message(mosq);
        int j;
        for (j = 0; j < 50; j++) {
            usleep(10000);
            mosquitto_loop(mosq, 0, 1);
        }
    }

    // Close and cleanup
    mosquitto_destroy(mosq);
    mosquitto_lib_cleanup();
    return 0;
}

/*
 * Helper function to publish MQTT messages to the MQTT server
 */
void publisher(struct mosquitto *mosq, char *topic, void *buf, unsigned len) {
    // publish the data
    mosquitto_publish(mosq, NULL, topic, len, buf, 0, false);
}

/*
 * Callback for incoming MQTT messages. Since this is a Sparkplug implementation these will be NCMD and DCMD messages
 */
void my_message_callback(struct mosquitto *mosq, void *userdata, const struct mosquitto_message *message) {

    if (message->payloadlen) {
        fprintf(stdout, "%s :: %d\n", message->topic, message->payloadlen);
    } else {
        fprintf(stdout, "%s (null)\n", message->topic);
    }
    fflush(stdout);

    // Decode the payload
    org_eclipse_tahu_protobuf_Payload inbound_payload = org_eclipse_tahu_protobuf_Payload_init_zero;
    if (decode_payload(&inbound_payload, message->payload, message->payloadlen)) {
    } else {
        fprintf(stderr, "Failed to decode the payload\n");
    }

    // Get the number of metrics in the payload and iterate over them handling them as needed
    int i;
    for (i = 0; i < inbound_payload.metrics_count; i++) {

        if (inbound_payload.metrics[i].name != NULL) {
            // Handle the incoming message as necessary - start with the 'Node Control' metrics
            if (strcmp(inbound_payload.metrics[i].name, "Node Control/Next Server") == 0) {
                // 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                // disconnect from the current MQTT server and connect to the next MQTT server in the
                // list of available servers.  This is used for clients that have a pool of MQTT servers
                // to connect to.
                fprintf(stderr, "'Node Control/Next Server' is not implemented in this example\n");
            } else if (strcmp(inbound_payload.metrics[i].name, "Node Control/Rebirth") == 0) {
                // 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                // its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                // application if it receives an NDATA or DDATA with a metric that was not published in the
                // original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                // its original NBIRTH and DBIRTH messages.
                publish_births(mosq);
            } else if (strcmp(inbound_payload.metrics[i].name, "Node Control/Reboot") == 0) {
                // 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                // This can be used for devices that need a full application reset via a soft reboot.
                // In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                // messages.
                publish_births(mosq);
            } else if (strcmp(inbound_payload.metrics[i].name, "output/Device Metric2") == 0) {
                // This is a metric we declared in our DBIRTH message and we're emulating an output.
                // So, on incoming 'writes' to the output we must publish a DDATA with the new output
                // value.  If this were a real output we'd write to the output and then read it back
                // before publishing a DDATA message.

                // We know this is an Int16 because of how we declated it in the DBIRTH
                uint32_t new_value = inbound_payload.metrics[i].value.int_value;
                fprintf(stdout, "CMD message for output/Device Metric2 - New Value: %d\n", new_value);

                // Create the DDATA payload
                org_eclipse_tahu_protobuf_Payload ddata_payload;
                get_next_payload(&ddata_payload);
                // Note the Metric name 'output/Device Metric2' is not needed because we're using aliases
                add_simple_metric(&ddata_payload, NULL, true, Device_Metric2, METRIC_DATA_TYPE_INT16, false, false, &new_value, sizeof(new_value));

                // Encode the payload into a binary format so it can be published in the MQTT message.
                // The binary_buffer must be large enough to hold the contents of the binary payload
                size_t buffer_length = 128;
                uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
                size_t message_length = encode_payload(binary_buffer, buffer_length, &ddata_payload);

                // Publish the DDATA on the appropriate topic
                mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DDATA/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

                // Free the memory
                free(binary_buffer);
                free_payload(&ddata_payload);
            } else if (strcmp(inbound_payload.metrics[i].name, "output/Device Metric3") == 0) {
                // This is a metric we declared in our DBIRTH message and we're emulating an output.
                // So, on incoming 'writes' to the output we must publish a DDATA with the new output
                // value.  If this were a real output we'd write to the output and then read it back
                // before publishing a DDATA message.

                // We know this is an Boolean because of how we declated it in the DBIRTH
                bool new_value = inbound_payload.metrics[i].value.boolean_value;
                fprintf(stdout, "CMD message for output/Device Metric3 - New Value: %s\n", new_value ? "true" : "false");

                // Create the DDATA payload
                org_eclipse_tahu_protobuf_Payload ddata_payload;
                get_next_payload(&ddata_payload);
                // Note the Metric name 'output/Device Metric3' is not needed because we're using aliases
                add_simple_metric(&ddata_payload, NULL, true, Device_Metric3, METRIC_DATA_TYPE_BOOLEAN, false, false, &new_value, sizeof(new_value));

                // Encode the payload into a binary format so it can be published in the MQTT message.
                // The binary_buffer must be large enough to hold the contents of the binary payload
                size_t buffer_length = 128;
                uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
                size_t message_length = encode_payload(binary_buffer, buffer_length, &ddata_payload);

                // Publish the DDATA on the appropriate topic
                mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DDATA/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

                // Free the memory
                free(binary_buffer);
                free_payload(&ddata_payload);
            } else {
                fprintf(stderr, "Unknown CMD: %s\n", inbound_payload.metrics[i].name);
            }
        } else if (inbound_payload.metrics[i].has_alias) {
            // Handle the incoming message as necessary - start with the 'Node Control' metrics
            if (inbound_payload.metrics[i].alias == Next_Server) {
                // 'Node Control/Next Server' is an NCMD used to tell the device/client application to
                // disconnect from the current MQTT server and connect to the next MQTT server in the
                // list of available servers.  This is used for clients that have a pool of MQTT servers
                // to connect to.
                fprintf(stderr, "'Node Control/Next Server' is not implemented in this example\n");
            } else if (inbound_payload.metrics[i].alias == Rebirth) {
                // 'Node Control/Rebirth' is an NCMD used to tell the device/client application to resend
                // its full NBIRTH and DBIRTH again.  MQTT Engine will send this NCMD to a device/client
                // application if it receives an NDATA or DDATA with a metric that was not published in the
                // original NBIRTH or DBIRTH.  This is why the application must send all known metrics in
                // its original NBIRTH and DBIRTH messages.
                publish_births(mosq);
            } else if (inbound_payload.metrics[i].alias == Reboot) {
                // 'Node Control/Reboot' is an NCMD used to tell a device/client application to reboot
                // This can be used for devices that need a full application reset via a soft reboot.
                // In this case, we fake a full reboot with a republishing of the NBIRTH and DBIRTH
                // messages.
                publish_births(mosq);
            } else if (inbound_payload.metrics[i].alias == Device_Metric2) {
                // This is a metric we declared in our DBIRTH message and we're emulating an output.
                // So, on incoming 'writes' to the output we must publish a DDATA with the new output
                // value.  If this were a real output we'd write to the output and then read it back
                // before publishing a DDATA message.

                // We know this is an Int16 because of how we declated it in the DBIRTH
                uint32_t new_value = inbound_payload.metrics[i].value.int_value;
                fprintf(stdout, "CMD message for output/Device Metric2 - New Value: %d\n", new_value);

                // Create the DDATA payload
                org_eclipse_tahu_protobuf_Payload ddata_payload;
                get_next_payload(&ddata_payload);
                // Note the Metric name 'output/Device Metric2' is not needed because we're using aliases
                add_simple_metric(&ddata_payload, NULL, true, Device_Metric2, METRIC_DATA_TYPE_INT16, false, false, &new_value, sizeof(new_value));

                // Encode the payload into a binary format so it can be published in the MQTT message.
                // The binary_buffer must be large enough to hold the contents of the binary payload
                size_t buffer_length = 128;
                uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
                size_t message_length = encode_payload(binary_buffer, buffer_length, &ddata_payload);

                // Publish the DDATA on the appropriate topic
                mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DDATA/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

                // Free the memory
                free(binary_buffer);
                free_payload(&ddata_payload);
            } else if (inbound_payload.metrics[i].alias == Device_Metric3) {
                // This is a metric we declared in our DBIRTH message and we're emulating an output.
                // So, on incoming 'writes' to the output we must publish a DDATA with the new output
                // value.  If this were a real output we'd write to the output and then read it back
                // before publishing a DDATA message.

                // We know this is an Boolean because of how we declated it in the DBIRTH
                bool new_value = inbound_payload.metrics[i].value.boolean_value;
                fprintf(stdout, "CMD message for output/Device Metric3 - New Value: %s\n", new_value ? "true" : "false");

                // Create the DDATA payload
                org_eclipse_tahu_protobuf_Payload ddata_payload;
                get_next_payload(&ddata_payload);
                // Note the Metric name 'output/Device Metric3' is not needed because we're using aliases
                add_simple_metric(&ddata_payload, NULL, true, Device_Metric3, METRIC_DATA_TYPE_BOOLEAN, false, false, &new_value, sizeof(new_value));

                // Encode the payload into a binary format so it can be published in the MQTT message.
                // The binary_buffer must be large enough to hold the contents of the binary payload
                size_t buffer_length = 128;
                uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
                size_t message_length = encode_payload(binary_buffer, buffer_length, &ddata_payload);

                // Publish the DDATA on the appropriate topic
                mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DDATA/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

                // Free the memory
                free(binary_buffer);
                free_payload(&ddata_payload);
            } else {
                fprintf(stderr, "Unknown CMD: %ld\n", inbound_payload.metrics[i].alias);
            }
        } else {
            fprintf(stdout, "Not a metric name or alias??\n");
        }
    }
}

/*
 * Callback for successful or unsuccessful MQTT connect.  Upon successful connect, subscribe to our Sparkplug NCMD and DCMD messages.
 * A production application should handle MQTT connect failures and reattempt as necessary.
 */
void my_connect_callback(struct mosquitto *mosq, void *userdata, int result) {
    if (!result) {
        // Subscribe to commands
        mosquitto_subscribe(mosq, NULL, "spBv1.0/Sparkplug B Devices/NCMD/C Edge Node 1/#", 0);
        mosquitto_subscribe(mosq, NULL, "spBv1.0/Sparkplug B Devices/DCMD/C Edge Node 1/#", 0);
    } else {
        fprintf(stderr, "MQTT Connect failed\n");
    }
}

/*
 * Callback for successful MQTT subscriptions.
 */
void my_subscribe_callback(struct mosquitto *mosq, void *userdata, int mid, int qos_count, const int *granted_qos) {
    int i;

    fprintf(stdout, "Subscribed (mid: %d): %d", mid, granted_qos[0]);
    for (i = 1; i < qos_count; i++) {
        fprintf(stdout, ", %d", granted_qos[i]);
    }
    fprintf(stdout, "\n");
}

/*
 * MQTT logger callback
 */
void my_log_callback(struct mosquitto *mosq, void *userdata, int level, const char *str) {
    // Print all log messages regardless of level.
    fprintf(stdout, "%s\n", str);
}

/*
 * Helper to publish the Sparkplug NBIRTH and DBIRTH messages after initial MQTT connect.
 * This is also used for Rebirth requests from the backend.
 */
void publish_births(struct mosquitto *mosq) {
    // Initialize the sequence number for Sparkplug MQTT messages
    // This must be zero on every NBIRTH publish

    // Publish the NBIRTH
    publish_node_birth(mosq);

    // Publish the DBIRTH
    publish_device_birth(mosq);
}

/*
 * Helper function to publish a NBIRTH message.  The NBIRTH should include all 'node control' metrics that denote device capability.
 * In addition, it should include every node metric that may ever be published from this edge node.  If any NDATA messages arrive at
 * MQTT Engine that were not included in the NBIRTH, MQTT Engine will request a Rebirth from the device.
 */
void publish_node_birth(struct mosquitto *mosq) {
    // Create the NBIRTH payload
    org_eclipse_tahu_protobuf_Payload nbirth_payload;

    // Initialize the sequence number for Sparkplug MQTT messages
    // This must be zero on every NBIRTH publish
    reset_sparkplug_sequence();
    get_next_payload(&nbirth_payload);
    nbirth_payload.uuid = strdup("MyUUID");

    // Add node control metrics
    fprintf(stdout, "Adding metric: 'Node Control/Next Server'\n");
    bool next_server_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Next Server", true, Next_Server, METRIC_DATA_TYPE_BOOLEAN, false, false, &next_server_value, sizeof(next_server_value));
    fprintf(stdout, "Adding metric: 'Node Control/Rebirth'\n");
    bool rebirth_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Rebirth", true, Rebirth, METRIC_DATA_TYPE_BOOLEAN, false, false, &rebirth_value, sizeof(rebirth_value));
    fprintf(stdout, "Adding metric: 'Node Control/Reboot'\n");
    bool reboot_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Reboot", true, Reboot, METRIC_DATA_TYPE_BOOLEAN, false, false, &reboot_value, sizeof(reboot_value));

    // Add some regular node metrics
    fprintf(stdout, "Adding metric: 'Node Metric0'\n");
    char nbirth_metric_zero_value[] = "hello node";
    add_simple_metric(&nbirth_payload, "Node Metric0", true, Node_Metric0, METRIC_DATA_TYPE_STRING, false, false, &nbirth_metric_zero_value, sizeof(nbirth_metric_zero_value));
    fprintf(stdout, "Adding metric: 'Node Metric1'\n");
    bool nbirth_metric_one_value = true;
    add_simple_metric(&nbirth_payload, "Node Metric1", true, Node_Metric1, METRIC_DATA_TYPE_BOOLEAN, false, false, &nbirth_metric_one_value, sizeof(nbirth_metric_one_value));

    // Create a DataSet
    org_eclipse_tahu_protobuf_Payload_DataSet dataset = org_eclipse_tahu_protobuf_Payload_DataSet_init_default;
    uint32_t datatypes[] = { DATA_SET_DATA_TYPE_INT8,
        DATA_SET_DATA_TYPE_INT16,
        DATA_SET_DATA_TYPE_INT32 };
    const char *column_keys[] = { "Int8s",
        "Int16s",
        "Int32s" };
    org_eclipse_tahu_protobuf_Payload_DataSet_Row *row_data = (org_eclipse_tahu_protobuf_Payload_DataSet_Row *)
        calloc(2, sizeof(org_eclipse_tahu_protobuf_Payload_DataSet_Row));
    row_data[0].elements_count = 3;
    row_data[0].elements = (org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue *)
        calloc(3, sizeof(org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue));
    row_data[0].elements[0].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[0].elements[0].value.int_value = 0;
    row_data[0].elements[1].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[0].elements[1].value.int_value = 1;
    row_data[0].elements[2].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[0].elements[2].value.int_value = 2;
    row_data[1].elements_count = 3;
    row_data[1].elements = (org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue *)
        calloc(3, sizeof(org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue));
    row_data[1].elements[0].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[1].elements[0].value.int_value = 3;
    row_data[1].elements[1].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[1].elements[1].value.int_value = 4;
    row_data[1].elements[2].which_value = org_eclipse_tahu_protobuf_Payload_DataSet_DataSetValue_int_value_tag;
    row_data[1].elements[2].value.int_value = 5;
    init_dataset(&dataset, 2, 3, datatypes, column_keys, row_data);

    // Create the a Metric with the DataSet value and add it to the payload
    fprintf(stdout, "Adding metric: 'DataSet'\n");
    org_eclipse_tahu_protobuf_Payload_Metric dataset_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&dataset_metric, "DataSet", true, Dataset, METRIC_DATA_TYPE_DATASET, false, false, &dataset, sizeof(dataset));
    add_metric_to_payload(&nbirth_payload, &dataset_metric);

    // Add a metric with a custom property
    fprintf(stdout, "Adding metric: 'Node Metric2'\n");
    org_eclipse_tahu_protobuf_Payload_Metric prop_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t nbirth_metric_two_value = 13;
    init_metric(&prop_metric, "Node Metric2", true, Node_Metric2, METRIC_DATA_TYPE_INT16, false, false, &nbirth_metric_two_value, sizeof(nbirth_metric_two_value));
    org_eclipse_tahu_protobuf_Payload_PropertySet properties = org_eclipse_tahu_protobuf_Payload_PropertySet_init_default;
    add_property_to_set(&properties, "engUnit", PROPERTY_DATA_TYPE_STRING, "MyCustomUnits", sizeof("MyCustomUnits"));
    add_propertyset_to_metric(&prop_metric, &properties);
    add_metric_to_payload(&nbirth_payload, &prop_metric);

    // Create a metric called RPMs which is a member of the UDT definition - note aliases do not apply to UDT members
    org_eclipse_tahu_protobuf_Payload_Metric rpms_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t rpms_value = 0;
    init_metric(&rpms_metric, "RPMs", false, 0, METRIC_DATA_TYPE_INT32, false, false, &rpms_value, sizeof(rpms_value));

    // Create a metric called AMPs which is a member of the UDT definition - note aliases do not apply to UDT members
    org_eclipse_tahu_protobuf_Payload_Metric amps_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t amps_value = 0;
    init_metric(&amps_metric, "AMPs", false, 0, METRIC_DATA_TYPE_INT32, false, false, &amps_value, sizeof(amps_value));

    // Create a Template/UDT Parameter - this is purely for example of including parameters and is not actually used by UDT instances
    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter.name = strdup("Index");
    parameter.has_type = true;
    parameter.type = PARAMETER_DATA_TYPE_STRING;
    parameter.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_string_value_tag;
    parameter.value.string_value = strdup("0");

    // Create the UDT definition value which includes the UDT members and parameters
    org_eclipse_tahu_protobuf_Payload_Template udt_template = org_eclipse_tahu_protobuf_Payload_Template_init_default;
    udt_template.metrics_count = 2;
    udt_template.metrics = (org_eclipse_tahu_protobuf_Payload_Metric *)calloc(2, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    udt_template.metrics[0] = rpms_metric;
    udt_template.metrics[1] = amps_metric;
    udt_template.parameters_count = 1;
    udt_template.parameters = (org_eclipse_tahu_protobuf_Payload_Template_Parameter *)calloc(1, sizeof(org_eclipse_tahu_protobuf_Payload_Template_Parameter));
    udt_template.parameters[0] = parameter;
    udt_template.template_ref = NULL;
    udt_template.has_is_definition = true;
    udt_template.is_definition = true;

    // Create the root UDT definition and add the UDT definition value which includes the UDT members and parameters
    org_eclipse_tahu_protobuf_Payload_Metric metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&metric, "_types_/Custom_Motor", false, 0, METRIC_DATA_TYPE_TEMPLATE, false, false, &udt_template, sizeof(udt_template));

    // Add the UDT to the payload
    add_metric_to_payload(&nbirth_payload, &metric);

#if SPARKPLUG_DEBUG
    // Print the payload for debug
    print_payload(&nbirth_payload);
#endif

    // Encode the payload into a binary format so it can be published in the MQTT message.
    // The binary_buffer must be large enough to hold the contents of the binary payload
    size_t buffer_length = 1024;
    uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
    size_t message_length = encode_payload(binary_buffer, buffer_length, &nbirth_payload);

    // Publish the NBIRTH on the appropriate topic
    mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/NBIRTH/C Edge Node 1", message_length, binary_buffer, 0, false);

    // Free the memory
    free(binary_buffer);
    free(row_data);
    free_payload(&nbirth_payload);
}

void publish_device_birth(struct mosquitto *mosq) {
    // Create the DBIRTH payload
    org_eclipse_tahu_protobuf_Payload dbirth_payload;
    get_next_payload(&dbirth_payload);

    // Add some device metrics
    fprintf(stdout, "Adding metric: 'input/Device Metric0'\n");
    char dbirth_metric_zero_value[] = "hello device";
    add_simple_metric(&dbirth_payload, "input/Device Metric0", true, Device_Metric0, METRIC_DATA_TYPE_STRING, false, false, &dbirth_metric_zero_value, sizeof(dbirth_metric_zero_value));
    fprintf(stdout, "Adding metric: 'input/Device Metric1'\n");
    bool dbirth_metric_one_value = true;
    add_simple_metric(&dbirth_payload, "input/Device Metric1", true, Device_Metric1, METRIC_DATA_TYPE_BOOLEAN, false, false, &dbirth_metric_one_value, sizeof(dbirth_metric_one_value));
    fprintf(stdout, "Adding metric: 'output/Device Metric2'\n");
    uint32_t dbirth_metric_two_value = 16;
    add_simple_metric(&dbirth_payload, "output/Device Metric2", true, Device_Metric2, METRIC_DATA_TYPE_INT16, false, false, &dbirth_metric_two_value, sizeof(dbirth_metric_two_value));
    fprintf(stdout, "Adding metric: 'output/Device Metric3'\n");
    bool dbirth_metric_three_value = true;
    add_simple_metric(&dbirth_payload, "output/Device Metric3", true, Device_Metric3, METRIC_DATA_TYPE_BOOLEAN, false, false, &dbirth_metric_three_value, sizeof(dbirth_metric_three_value));

    // Create a metric called RPMs for the UDT instance
    org_eclipse_tahu_protobuf_Payload_Metric rpms_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t rpms_value = 123;
    init_metric(&rpms_metric, "RPMs", false, 0, METRIC_DATA_TYPE_INT32, false, false, &rpms_value, sizeof(rpms_value));

    // Create a metric called AMPs for the UDT instance and create a custom property (milliamps) for it
    org_eclipse_tahu_protobuf_Payload_Metric amps_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t amps_value = 456;
    init_metric(&amps_metric, "AMPs", false, 0, METRIC_DATA_TYPE_INT32, false, false, &amps_value, sizeof(amps_value));
    org_eclipse_tahu_protobuf_Payload_PropertySet properties = org_eclipse_tahu_protobuf_Payload_PropertySet_init_default;
    add_property_to_set(&properties, "engUnit", PROPERTY_DATA_TYPE_STRING, "milliamps", sizeof("milliamps"));
    add_propertyset_to_metric(&amps_metric, &properties);

    // Create a Template/UDT instance Parameter - this is purely for example of including parameters and is not actually used by UDT instances
    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter.name = strdup("Index");
    parameter.has_type = true;
    parameter.type = PARAMETER_DATA_TYPE_STRING;
    parameter.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_string_value_tag;
    parameter.value.string_value = strdup("1");

    // Create the UDT instance value which includes the UDT members and parameters
    org_eclipse_tahu_protobuf_Payload_Template udt_template = org_eclipse_tahu_protobuf_Payload_Template_init_default;
    udt_template.version = NULL;
    udt_template.metrics_count = 2;
    udt_template.metrics = (org_eclipse_tahu_protobuf_Payload_Metric *)calloc(2, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    udt_template.metrics[0] = rpms_metric;
    udt_template.metrics[1] = amps_metric;
    udt_template.parameters_count = 1;
    udt_template.parameters = (org_eclipse_tahu_protobuf_Payload_Template_Parameter *)calloc(1, sizeof(org_eclipse_tahu_protobuf_Payload_Template_Parameter));
    udt_template.parameters[0] = parameter;
    udt_template.template_ref = strdup("Custom_Motor");
    udt_template.has_is_definition = true;
    udt_template.is_definition = false;

    // Create the root UDT instance and add the UDT instance value
    org_eclipse_tahu_protobuf_Payload_Metric metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&metric, "My_Custom_Motor", true, My_Custom_Motor, METRIC_DATA_TYPE_TEMPLATE, false, false, &udt_template, sizeof(udt_template));

    // Add the UDT Instance to the payload
    add_metric_to_payload(&dbirth_payload, &metric);

#if SPARKPLUG_DEBUG
    // Print the payload
    print_payload(&dbirth_payload);
#endif

    // Encode the payload into a binary format so it can be published in the MQTT message.
    // The binary_buffer must be large enough to hold the contents of the binary payload
    size_t buffer_length = 1024;
    uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
    size_t message_length = encode_payload(binary_buffer, buffer_length, &dbirth_payload);

    // Publish the DBIRTH on the appropriate topic
    mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DBIRTH/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

    // Free the memory
    free(binary_buffer);
    free_payload(&dbirth_payload);
}

void publish_ddata_message(struct mosquitto *mosq) {
    // Create the DDATA payload
    org_eclipse_tahu_protobuf_Payload ddata_payload;
    get_next_payload(&ddata_payload);

    // Add some device metrics to denote changed values on inputs
    fprintf(stdout, "Adding metric: 'input/Device Metric0'\n");
    char ddata_metric_zero_value[13];
    int i;
    for (i = 0; i < 12; ++i) {
        ddata_metric_zero_value[i] = '0' + rand() % 72; // starting on '0', ending on '}'
    }
    ddata_metric_zero_value[12] = 0;

    // Note the Metric name 'input/Device Metric0' is not needed because we're using aliases
    add_simple_metric(&ddata_payload, NULL, true, Device_Metric0, METRIC_DATA_TYPE_STRING, false, false, &ddata_metric_zero_value, sizeof(ddata_metric_zero_value));
    fprintf(stdout, "Adding metric: 'input/Device Metric1'\n");
    bool ddata_metric_one_value = rand() % 2;
    // Note the Metric name 'input/Device Metric1' is not needed because we're using aliases
    add_simple_metric(&ddata_payload, NULL, true, Device_Metric1, METRIC_DATA_TYPE_BOOLEAN, false, false, &ddata_metric_one_value, sizeof(ddata_metric_one_value));

#if SPARKPLUG_DEBUG
    // Print the payload
    print_payload(&ddata_payload);
#endif

    // Encode the payload into a binary format so it can be published in the MQTT message.
    // The binary_buffer must be large enough to hold the contents of the binary payload
    size_t buffer_length = 1024;
    uint8_t *binary_buffer = (uint8_t *)malloc(buffer_length * sizeof(uint8_t));
    size_t message_length = encode_payload(binary_buffer, buffer_length, &ddata_payload);

    // Publish the DDATA on the appropriate topic
    mosquitto_publish(mosq, NULL, "spBv1.0/Sparkplug B Devices/DDATA/C Edge Node 1/Emulated Device", message_length, binary_buffer, 0, false);

    // Free the memory
    free(binary_buffer);
    free_payload(&ddata_payload);
}
