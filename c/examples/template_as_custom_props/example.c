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
            usleep(100000);
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
        return;  // JPL 04/06/17...
    }

    // Get the number of metrics in the payload and iterate over them handling them as needed
    int i;
    for (i = 0; i < inbound_payload.metrics_count; i++) {
        // Handle the incoming message as necessary - start with the 'Node Control' metrics
        // JPL 04/06/17... Handle ALIAS metrics versus text-name based metrics
        if (inbound_payload.metrics[i].name == NULL) {  // alias 0 to 2
            switch (inbound_payload.metrics[i].alias) {
            case 0:  // Next Server
                fprintf(stderr, "Using Next Configured MQtt Server\n");
                break;

            case 1:  // Resend Births
                fprintf(stderr, "Resend Birth Certificates\n");
                publish_births(mosq);
                break;

            case 2:  // Next Server
                fprintf(stderr, "REBOOT Operating system\n");
                //system("reboot");
                break;
            }
        } else if (strcmp(inbound_payload.metrics[i].name, "Node Control/Next Server") == 0) {
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
        } else {
            fprintf(stderr, "Unknown CMD: %s\n", inbound_payload.metrics[i].name);
        }
    }
    free_payload(&inbound_payload);  // JPL 04/06/17...
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

    // Add node control metrics
    fprintf(stdout, "Adding metric: 'Node Control/Next Server'\n");
    bool next_server_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Next Server", true, 0, METRIC_DATA_TYPE_BOOLEAN, false, false, &next_server_value, sizeof(next_server_value));
    fprintf(stdout, "Adding metric: 'Node Control/Rebirth'\n");
    bool rebirth_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Rebirth", true, 1, METRIC_DATA_TYPE_BOOLEAN, false, false, &rebirth_value, sizeof(rebirth_value));
    fprintf(stdout, "Adding metric: 'Node Control/Reboot'\n");
    bool reboot_value = false;
    add_simple_metric(&nbirth_payload, "Node Control/Reboot", true, 2, METRIC_DATA_TYPE_BOOLEAN, false, false, &reboot_value, sizeof(reboot_value));

    // Create a metric called 'My Real Metric' which will be a member of the Template definition - note aliases do not apply to Template members
    org_eclipse_tahu_protobuf_Payload_Metric my_real_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t my_real_metric_value = 0;      // Default value
    init_metric(&my_real_metric, "My Real Metric", false, 0, METRIC_DATA_TYPE_INT32, false, false, &my_real_metric_value, sizeof(my_real_metric_value));

    // Create some Template Parameters - In this example we're using them as custom properties of a regular metric via a Template
    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_one = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;

    parameter_one.name = strdup("MyPropKey1");
    parameter_one.has_type = true;
    parameter_one.type = PARAMETER_DATA_TYPE_STRING;
    parameter_one.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_string_value_tag;

    parameter_one.value.string_value = strdup("MyDefaultPropValue1");
    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_two = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter_two.name = strdup("MyPropKey2");
    parameter_two.has_type = true;
    parameter_two.type = PARAMETER_DATA_TYPE_INT32;
    parameter_two.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_int_value_tag;
    parameter_two.value.int_value = 0;      // Default value

    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_three = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter_three.name = strdup("MyPropKey3");
    parameter_three.has_type = true;
    parameter_three.type = PARAMETER_DATA_TYPE_FLOAT;
    parameter_three.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_float_value_tag;
    parameter_three.value.float_value = 0.0;    // Default value

    // Create the Template definition value which includes the single Template members and parameters which are custom properties of the 'real metric'
    org_eclipse_tahu_protobuf_Payload_Template udt_template = org_eclipse_tahu_protobuf_Payload_Template_init_default;
    udt_template.metrics_count = 1;
    udt_template.metrics = (org_eclipse_tahu_protobuf_Payload_Metric *)calloc(1, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    udt_template.metrics[0] = my_real_metric;
    udt_template.parameters_count = 3;
    udt_template.parameters = (org_eclipse_tahu_protobuf_Payload_Template_Parameter *)calloc(3, sizeof(org_eclipse_tahu_protobuf_Payload_Template_Parameter));
    udt_template.parameters[0] = parameter_one;
    udt_template.parameters[1] = parameter_two;
    udt_template.parameters[2] = parameter_three;
    udt_template.template_ref = NULL;
    udt_template.has_is_definition = true;
    udt_template.is_definition = true;

    // Create the root Template definition and add the Template definition value which includes the Template members and parameters
    org_eclipse_tahu_protobuf_Payload_Metric metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&metric, "_types_/My Metric Definition", true, 3, METRIC_DATA_TYPE_TEMPLATE, false, false, &udt_template, sizeof(udt_template));

    // Add the Template to the payload
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
    free_payload(&nbirth_payload);
}

void publish_device_birth(struct mosquitto *mosq) {
    // Create the DBIRTH payload
    org_eclipse_tahu_protobuf_Payload dbirth_payload;
    get_next_payload(&dbirth_payload);

    // Add a metric with a custom property.  For use with Ignition, in order to see this as a Tag Property - it must be a known Ignition Tag Property.
    fprintf(stdout, "Adding metric: 'Device Metric1'\n");
    org_eclipse_tahu_protobuf_Payload_Metric prop_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t nbirth_metric_two_value = 13;
    init_metric(&prop_metric, "Device Metric1", true, 4, METRIC_DATA_TYPE_INT16, false, false, &nbirth_metric_two_value, sizeof(nbirth_metric_two_value));
    org_eclipse_tahu_protobuf_Payload_PropertySet properties = org_eclipse_tahu_protobuf_Payload_PropertySet_init_default;
    add_property_to_set(&properties, "engUnit", PROPERTY_DATA_TYPE_STRING, "MyCustomUnits", sizeof("MyCustomUnits"));
    add_propertyset_to_metric(&prop_metric, &properties);
    add_metric_to_payload(&dbirth_payload, &prop_metric);

    // Create a metric called 'My Real Metric' for the Template instance
    org_eclipse_tahu_protobuf_Payload_Metric my_real_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t my_real_metric_value = 123;    // Not a default - this is the actual value of the instance
    init_metric(&my_real_metric, "My Real Metric", false, 0, METRIC_DATA_TYPE_INT32, false, false, &my_real_metric_value, sizeof(my_real_metric_value));

    // Create some Template/UDT instance Parameters - in this example they represent custom tag properties
    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_one = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter_one.name = strdup("MyPropKey1");
    parameter_one.has_type = true;
    parameter_one.type = PARAMETER_DATA_TYPE_STRING;
    parameter_one.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_string_value_tag;
    parameter_one.value.string_value = strdup("MyInstancePropValue1");

    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_two = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter_two.name = strdup("MyPropKey2");
    parameter_two.has_type = true;
    parameter_two.type = PARAMETER_DATA_TYPE_INT32;
    parameter_two.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_int_value_tag;
    parameter_two.value.int_value = 1089;

    org_eclipse_tahu_protobuf_Payload_Template_Parameter parameter_three = org_eclipse_tahu_protobuf_Payload_Template_Parameter_init_default;
    parameter_three.name = strdup("MyPropKey3");
    parameter_three.has_type = true;
    parameter_three.type = PARAMETER_DATA_TYPE_FLOAT;
    parameter_three.which_value = org_eclipse_tahu_protobuf_Payload_Template_Parameter_float_value_tag;
    parameter_three.value.float_value = 12.34;

    // Create the Template instance value which includes the Template members and parameters
    org_eclipse_tahu_protobuf_Payload_Template udt_template = org_eclipse_tahu_protobuf_Payload_Template_init_default;
    udt_template.version = NULL;
    udt_template.metrics_count = 1;
    udt_template.metrics = (org_eclipse_tahu_protobuf_Payload_Metric *)calloc(1, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    udt_template.metrics[0] = my_real_metric;
    udt_template.parameters_count = 3;
    udt_template.parameters = (org_eclipse_tahu_protobuf_Payload_Template_Parameter *)calloc(3, sizeof(org_eclipse_tahu_protobuf_Payload_Template_Parameter));
    udt_template.parameters[0] = parameter_one;
    udt_template.parameters[1] = parameter_two;
    udt_template.parameters[2] = parameter_three;
    udt_template.template_ref = strdup("My Metric Definition");
    udt_template.has_is_definition = true;
    udt_template.is_definition = false;

    // Create the root Template instance and add the Template instance value
    org_eclipse_tahu_protobuf_Payload_Metric metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&metric, "My Metric Instance 1", true, 5, METRIC_DATA_TYPE_TEMPLATE, false, false, &udt_template, sizeof(udt_template));

    // Add the Template Instance to the payload
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

    // Update the metric called 'My Real Metric' for the Template instance to update the 'real' metric value
    org_eclipse_tahu_protobuf_Payload_Metric my_real_metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    uint32_t my_real_metric_value = rand(); // Not a default - this is the actual value of the metric of instance
    init_metric(&my_real_metric, "My Real Metric", false, 0, METRIC_DATA_TYPE_INT32, false, false, &my_real_metric_value, sizeof(my_real_metric_value));

    // Create the Template instance value which includes the Template members and parameters
    org_eclipse_tahu_protobuf_Payload_Template udt_template = org_eclipse_tahu_protobuf_Payload_Template_init_default;
    udt_template.version = NULL;
    udt_template.metrics_count = 1;
    udt_template.metrics = (org_eclipse_tahu_protobuf_Payload_Metric *)calloc(1, sizeof(org_eclipse_tahu_protobuf_Payload_Metric));
    udt_template.metrics[0] = my_real_metric;
    udt_template.has_is_definition = true;
    udt_template.is_definition = false;

    // Create the root Template instance and add the Template instance value
    org_eclipse_tahu_protobuf_Payload_Metric metric = org_eclipse_tahu_protobuf_Payload_Metric_init_default;
    init_metric(&metric, "My Metric Instance 1", true, 5, METRIC_DATA_TYPE_TEMPLATE, false, false, &udt_template, sizeof(udt_template));

    add_metric_to_payload(&ddata_payload, &metric);

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
