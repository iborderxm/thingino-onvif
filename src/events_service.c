/*
 * Copyright (c) 2024 roleo.
 * Copyright (c) 2025 Thingino project.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "events_service.h"

#include "fault.h"
#include "log.h"
#include "mxml_wrapper.h"
#include "onvif_simple_server.h"
#include "utils.h"
#include "xml_logger.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

extern service_context_t service_ctx;

shm_t *subs_evts;

int events_get_service_capabilities()
{
    char ebasesubscription[8], epullpoint[8];

    if ((service_ctx.events_enable == EVENTS_PULLPOINT) || (service_ctx.events_enable == EVENTS_BOTH)) {
        strcpy(epullpoint, "true");
    } else {
        strcpy(epullpoint, "false");
    }
    if ((service_ctx.events_enable == EVENTS_BASESUBSCRIPTION) || (service_ctx.events_enable == EVENTS_BOTH)) {
        strcpy(ebasesubscription, "true");
    } else {
        strcpy(ebasesubscription, "false");
    }

    long size = cat(NULL,
                    "events_service_files/GetServiceCapabilities.xml",
                    4,
                    "%EVENTS_BASESUBSCRIPTION%",
                    ebasesubscription,
                    "%EVENTS_PULLPOINT%",
                    epullpoint);

    output_http_headers(size);

    return cat("stdout",
               "events_service_files/GetServiceCapabilities.xml",
               4,
               "%EVENTS_BASESUBSCRIPTION%",
               ebasesubscription,
               "%EVENTS_PULLPOINT%",
               epullpoint);
}

int events_create_pull_point_subscription()
{
    const char *element;
    const char *itt;
    const char *te;
    const char *mc;
    time_t now, expire_time;
    char iso_str[21];
    char iso_str_2[21];
    int i, subscription_id, sub_index;

    char my_address[16];
    char my_netmask[16];
    char my_port[8];
    char events_service_address[MAX_LEN];
    topic_expressions_t *topic_expression;

    log_info("CreatePullPointSubscription received");

    get_ip_address(my_address, my_netmask, service_ctx.ifs);
    my_port[0] = '\0';
    if (service_ctx.port != 80)
        sprintf(my_port, ":%d", service_ctx.port);

    // TopicExpression filter is supported
    element = get_element("Filter", "Body");
    if (element == NULL) {
        te = NULL;
        mc = NULL;
    } else {
        te = get_element("TopicExpression", "Body");
        mc = get_element("MessageContent", "Body");
    }

    time(&now);
    itt = get_element("InitialTerminationTime", "Body");
    if (itt == NULL) {
        // Use NotificationProducer termination time = 10 minutes (more interoperable default)
        expire_time = now + 600;
    } else {
        if (strncmp("PT", itt, 2) == 0) {
            expire_time = now + interval2sec(itt);
        } else {
            expire_time = from_iso_date(itt);
        }
    }

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_fault("events_service",
                   "Receiver",
                   "wsntw:UnableToCreatePullPointFault",
                   "wsntw:UnableToCreatePullPointFault",
                   "Unable to create pull point",
                   "");
        return -3;
    }
    sem_memory_wait();
    subscription_id = 0;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subscription_id < subs_evts->subscriptions[i].id) {
            subscription_id = subs_evts->subscriptions[i].id;
        }
    }
    subscription_id = (subscription_id == 65535) ? 1 : subscription_id + 1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].used == SUB_UNUSED) {
            subs_evts->subscriptions[i].id = subscription_id;
            memset(subs_evts->subscriptions[i].reference, '\0', CONSUMER_REFERENCE_MAX_SIZE);
            subs_evts->subscriptions[i].used = SUB_PULL;
            subs_evts->subscriptions[i].expire = expire_time;
            subs_evts->subscriptions[i].topic_expression[0] = '\0';
            if ((te != NULL) && (te[0] != '\0')) {
                if (strlen(te) < sizeof(subs_evts->subscriptions[i].topic_expression) - 1) {
                    strcpy(subs_evts->subscriptions[i].topic_expression, te);
                }
            }
            break;
        }
    }
    sem_memory_post();
    sub_index = i;
    if (sub_index == MAX_SUBSCRIPTIONS) {
        // Log current subscription status to help diagnose the issue
        log_error("Reached the maximum number of subscriptions (%d)", MAX_SUBSCRIPTIONS);
        sem_memory_wait();
        int active_count = 0;
        time_t current_time = time(NULL);
        for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
            if (subs_evts->subscriptions[i].used != SUB_UNUSED) {
                active_count++;
                char expire_str[21];
                to_iso_date(expire_str, sizeof(expire_str), subs_evts->subscriptions[i].expire);
                int expired = (current_time > subs_evts->subscriptions[i].expire) ? 1 : 0;
                log_warn("Subscription slot %d: id=%d, type=%s, expire=%s%s",
                         i,
                         subs_evts->subscriptions[i].id,
                         subs_evts->subscriptions[i].used == SUB_PULL ? "PULL" : "PUSH",
                         expire_str,
                         expired ? " (EXPIRED)" : "");
            }
        }
        sem_memory_post();
        log_error("Active subscriptions: %d/%d (consider increasing MAX_SUBSCRIPTIONS or check for subscription leaks)",
                  active_count,
                  MAX_SUBSCRIPTIONS);
        send_fault("events_service",
                   "Receiver",
                   "wsntw:SubscribeCreationFailedFault",
                   "wsntw:SubscribeCreationFailedFault",
                   "Subscribe creation failed fault",
                   "");
        return -4;
    }

    // Force notification with Property "Initialized"
    sem_memory_wait();
    for (i = 0; i < service_ctx.events_num && i < MAX_EVENTS; i++) {
        if (service_ctx.events[i].topic != NULL
            && is_topic_in_expression(subs_evts->subscriptions[sub_index].topic_expression, service_ctx.events[i].topic)) {
            subs_evts->events[i].pull_send_initialized |= (1 << sub_index);
            subs_evts->events[i].pull_notify |= (1 << sub_index);
            log_debug("Event %d matches topic expression %s", i, subs_evts->subscriptions[sub_index].topic_expression);
        }
    }
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    sprintf(events_service_address, "http://%s%s/onvif/events_service?sub=%d", my_address, my_port, subscription_id);
    log_debug("CreatePullPointSubscription: events_service_address = %s", events_service_address);

    to_iso_date(iso_str, sizeof(iso_str), now);
    to_iso_date(iso_str_2, sizeof(iso_str_2), expire_time);

    log_info("Created PULL subscription: id=%d, slot=%d, expire=%s", subscription_id, sub_index, iso_str_2);
    log_debug("Subscription data: expire %s - subscription id %d", iso_str_2, subscription_id);

    long size = cat(NULL,
                    "events_service_files/CreatePullPointSubscription.xml",
                    6,
                    "%ADDRESS%",
                    events_service_address,
                    "%CURRENT_TIME%",
                    iso_str,
                    "%TERMINATION_TIME%",
                    iso_str_2);
    // Log the generated XML response for debugging
    FILE *fp = fopen("/tmp/CreatePullPointSubscriptionResponse.xml", "r");
    if (fp) {
        char buf[1024];
        size_t n = fread(buf, 1, sizeof(buf) - 1, fp);
        buf[n] = '\0';
        log_debug("CreatePullPointSubscription XML response: %s", buf);
        fclose(fp);
    }

    output_http_headers(size);

    return cat("stdout",
               "events_service_files/CreatePullPointSubscription.xml",
               6,
               "%ADDRESS%",
               events_service_address,
               "%CURRENT_TIME%",
               iso_str,
               "%TERMINATION_TIME%",
               iso_str_2);
}

int events_pull_messages()
{
    const char *timeout;
    const char *message_limit;
    int limit, count;
    time_t now, now_p_timeout, previous_expire_time, expire_time;
    char iso_str[21];
    char iso_str_2[21];
    char iso_str_3[21];

    int qs_size;
    char *qs_string;
    char *sub_id_s;
    int sub_id, sub_index;

    int at_least_one_message;
    int i, c;
    char *endptr;
    char property[32];
    char data_name[32];
    char data_value[32];
    char dest_a[] = "stdout";
    char *dest;
    long size, total_size;

    // Initialize subs_evts to NULL to prevent crashes in error paths
    subs_evts = NULL;

    // Subscription manager replies to address http://%s%s/onvif/events_service?sub=%d
    log_debug("PullMessages request received");

    // Get parameters from query string
    get_from_query_string(&qs_string, &qs_size, "sub");
    if ((qs_size == -1) || (qs_string == NULL)) {
        log_error("No sub parameter in query string for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "missing sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -1;
    }
    sub_id_s = (char *) malloc((qs_size + 1) * sizeof(char));
    memset(sub_id_s, '\0', qs_size + 1);
    strncpy(sub_id_s, qs_string, qs_size);
    sub_id = atoi(sub_id_s);
    free(sub_id_s);
    if ((sub_id <= 0) || (sub_id > 65535)) {
        log_error("sub index out of range for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "sub out of range",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -2;
    }

    // Get parameters from xml content
    timeout = get_element("Timeout", "Body");
    if (timeout == NULL) {
        log_error("No Timeout element for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "missing Timeout",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_action_failed_fault("events_service", -3);
        return -3;
    }

    message_limit = get_element("MessageLimit", "Body");
    if (message_limit == NULL) {
        log_error("No MessageLimit element for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "missing MessageLimit",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_action_failed_fault("events_service", -4);
        return -4;
    }

    limit = strtol(message_limit, &endptr, 10);
    /* Check for various possible errors */
    if ((errno == ERANGE && (limit == LONG_MAX || limit == LONG_MIN)) || (errno != 0 && limit == 0)) {
        log_error("Wrong MessageLimit value for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "invalid MessageLimit",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_action_failed_fault("events_service", -5);
        return -5;
    }

    log_debug("Pull message request with timeout %d seconds and message limit %d", interval2sec(timeout), limit);

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_action_failed_fault("events_service", -6);
        return -6;
    }

    // Find subscription index
    sem_memory_wait();
    sub_index = -1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].id == sub_id) {
            sub_index = i;
            break;
        }
    }
    if ((sub_index < 0) || (sub_index >= MAX_SUBSCRIPTIONS)) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("Sub index out of range for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "subscription not found",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -7;
    }

    if (subs_evts->subscriptions[sub_index].used != SUB_PULL) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("This subscription is not a Real-time Pull-Point Notification Interface");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "PullMessages",
                              "not pull-point subscription",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -8;
    }

    // Set temporary termination time += 10 to avoid termination during this function
    time(&now);
    previous_expire_time = subs_evts->subscriptions[sub_index].expire;
    subs_evts->subscriptions[sub_index].expire = now + interval2sec(timeout) + 10;
    sem_memory_post();
    now_p_timeout = now + interval2sec(timeout);

    // Check if at least 1 message was triggered
    // or SetSynchronizationPoint request is received
    at_least_one_message = 0;
    while ((at_least_one_message == 0) && (now <= now_p_timeout)) {
        for (i = 0; i < service_ctx.events_num && i < MAX_EVENTS; i++) {
            sem_memory_wait();
            if (subs_evts->events[i].pull_notify & (1 << sub_index)) {
                at_least_one_message = 1;
                sem_memory_post();
                break;
            }
            sem_memory_post();
        }
        if (at_least_one_message == 1) {
            break;
        }
        usleep(100000);
        time(&now);
    }

    // Set correct termination time
    time(&now);
    sem_memory_wait();
    if (previous_expire_time > now + interval2sec(timeout)) {
        subs_evts->subscriptions[sub_index].expire = previous_expire_time;
    } else {
        subs_evts->subscriptions[sub_index].expire = now + interval2sec(timeout);
    }
    expire_time = subs_evts->subscriptions[sub_index].expire;
    sem_memory_post();
    to_iso_date(iso_str, sizeof(iso_str), now);
    to_iso_date(iso_str_2, sizeof(iso_str_2), expire_time);

    // We need 1st step to evaluate content length
    sem_memory_wait();
    for (c = 0; c < 2; c++) {
        if (c == 0) {
            dest = NULL;
        } else {
            dest = dest_a;
            // Send HTTP headers once for the full payload size calculated in the first pass
            output_http_headers(total_size);
        }

        size = cat(dest, "events_service_files/PullMessages_1.xml", 4, "%CURRENT_TIME%", iso_str, "%TERMINATION_TIME%", iso_str_2);
        if (c == 0)
            total_size = size;

        count = 0;
        for (i = 0; i < service_ctx.events_num && i < MAX_EVENTS; i++) {
            if (count >= limit)
                break;
            // Skip events with NULL topic to prevent segfaults
            if (service_ctx.events[i].topic == NULL) {
                log_warn("Skipping event %d with NULL topic", i);
                continue;
            }
            if ((subs_evts->events[i].pull_notify & (1 << sub_index))) {
                if ((subs_evts->events[i].pull_send_initialized & (1 << sub_index))) {
                    strcpy(property, "Initialized");
                    to_iso_date(iso_str_3, sizeof(iso_str_3), now);
                } else {
                    strcpy(property, "Changed");
                    to_iso_date(iso_str_3, sizeof(iso_str_3), subs_evts->events[i].e_time);
                }
                if (subs_evts->events[i].is_on) {
                    if (service_ctx.events[i].topic != NULL && strcmp("tns1:Device/Trigger/Relay", service_ctx.events[i].topic) == 0) {
                        strcpy(data_value, "active");
                    } else {
                        strcpy(data_value, "true");
                    }
                } else {
                    if (service_ctx.events[i].topic != NULL && strcmp("tns1:Device/Trigger/Relay", service_ctx.events[i].topic) == 0) {
                        strcpy(data_value, "inactive");
                    } else {
                        strcpy(data_value, "false");
                    }
                }
                if (service_ctx.events[i].topic != NULL && strcmp("tns1:Device/Trigger/Relay", service_ctx.events[i].topic) == 0) {
                    strcpy(data_name, "LogicalState");
                } else if (service_ctx.events[i].topic != NULL
                           && strstr(service_ctx.events[i].topic, "VideoSource/MotionAlarm")) {
                    // ONVIF event catalog: VideoSource/MotionAlarm carries State
                    strcpy(data_name, "State");
                } else if (service_ctx.events[i].topic != NULL
                           && strstr(service_ctx.events[i].topic, "CellMotionDetector/Motion")) {
                    // ONVIF event catalog: CellMotionDetector/Motion carries IsMotion
                    strcpy(data_name, "IsMotion");
                } else if (service_ctx.events[i].topic != NULL
                           && strstr(service_ctx.events[i].topic, "MyRuleDetector/PeopleDetect")) {
                    // RuleEngine/MyRuleDetector/PeopleDetect carries IsPeopleDetected
                    strcpy(data_name, "IsPeopleDetected");
                } else {
                    strcpy(data_name, "State");
                }
                // Ensure we have valid strings to prevent null pointer dereference
                const char *safe_topic = service_ctx.events[i].topic ? service_ctx.events[i].topic : "Unknown";
                char sources_xml[512];
                build_event_sources(sources_xml, sizeof(sources_xml), &service_ctx.events[i]);

                size = cat(dest,
                           "events_service_files/PullMessages_2.xml",
                           12,
                           "%TOPIC%",
                           safe_topic,
                           "%UTC_TIME%",
                           iso_str_3,
                           "%PROPERTY%",
                           property,
                           "%SOURCES%",
                           sources_xml,
                           "%DATA_NAME%",
                           data_name,
                           "%DATA_VALUE%",
                           data_value);
                if (c == 0)
                    total_size += size;

                if (c == 1) {
                    subs_evts->events[i].pull_send_initialized &= ~(1 << sub_index);
                    subs_evts->events[i].pull_notify &= ~(1 << sub_index);
                }

                count++;
            }
        }

        size = cat(dest, "events_service_files/PullMessages_3.xml", 0);
        if (c == 0)
            total_size += size;
    }
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    return total_size;
}

int events_subscribe()
{
    const char *address;
    const char *element;
    const char *itt;
    const char *te;
    const char *mc;
    char msg_uuid[UUID_LEN + 1];
    const char *relates_to_uuid;
    time_t now, expire_time;
    char iso_str[21];
    char iso_str_2[21];
    int i, subsel;
    int subscription_id, sub_index;

    char my_address[16];
    char my_netmask[16];
    char my_port[8];
    char events_service_address[MAX_LEN];

    log_info("Subscribe request received");

    get_ip_address(my_address, my_netmask, service_ctx.ifs);
    my_port[0] = '\0';
    if (service_ctx.port != 80)
        sprintf(my_port, ":%d", service_ctx.port);

    address = get_element("Address", "Body");
    if (address == NULL) {
        log_error("No Address element for Subscribe method");
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -1;
    }

    // TopicExpression filter is supported
    element = get_element("Filter", "Body");
    if (element == NULL) {
        te = NULL;
        mc = NULL;
    } else {
        te = get_element("TopicExpression", "Body");
        mc = get_element("MessageContent", "Body");
    }

    time(&now);
    itt = get_element("InitialTerminationTime", "Body");
    if (itt == NULL) {
        // Use NotificationProducer termination time = 10 minutes
        expire_time = now + 600;
    } else {
        if (strncmp("PT", itt, 2) == 0) {
            expire_time = now + interval2sec(itt);
        } else {
            expire_time = from_iso_date(itt);
        }
    }

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_action_failed_fault("events_service", -4);
        return -4;
    }
    sem_memory_wait();
    subscription_id = 0;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subscription_id < subs_evts->subscriptions[i].id) {
            subscription_id = subs_evts->subscriptions[i].id;
        }
    }
    subscription_id = (subscription_id == 65535) ? 1 : subscription_id + 1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].used == SUB_UNUSED) {
            subs_evts->subscriptions[i].id = subscription_id;
            memset(subs_evts->subscriptions[i].reference, '\0', CONSUMER_REFERENCE_MAX_SIZE);
            strncpy(subs_evts->subscriptions[i].reference, address, CONSUMER_REFERENCE_MAX_SIZE - 1);
            subs_evts->subscriptions[i].used = SUB_PUSH;
            subs_evts->subscriptions[i].expire = expire_time;
            subs_evts->subscriptions[i].topic_expression[0] = '\0';
            if ((te != NULL) && (te[0] != '\0')) {
                if (strlen(te) < sizeof(subs_evts->subscriptions[i].topic_expression) - 1) {
                    strcpy(subs_evts->subscriptions[i].topic_expression, te);
                }
            }
            break;
        }
    }
    sem_memory_post();
    sub_index = i;
    if (sub_index == MAX_SUBSCRIPTIONS) {
        // Log current subscription status to help diagnose the issue
        log_error("Reached the maximum number of subscriptions (%d)", MAX_SUBSCRIPTIONS);
        sem_memory_wait();
        int active_count = 0;
        time_t current_time = time(NULL);
        for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
            if (subs_evts->subscriptions[i].used != SUB_UNUSED) {
                active_count++;
                char expire_str[21];
                to_iso_date(expire_str, sizeof(expire_str), subs_evts->subscriptions[i].expire);
                int expired = (current_time > subs_evts->subscriptions[i].expire) ? 1 : 0;
                log_warn("Subscription slot %d: id=%d, type=%s, expire=%s%s",
                         i,
                         subs_evts->subscriptions[i].id,
                         subs_evts->subscriptions[i].used == SUB_PULL ? "PULL" : "PUSH",
                         expire_str,
                         expired ? " (EXPIRED)" : "");
            }
        }
        sem_memory_post();
        log_error("Active subscriptions: %d/%d (consider increasing MAX_SUBSCRIPTIONS or check for subscription leaks)",
                  active_count,
                  MAX_SUBSCRIPTIONS);
        send_fault("events_service",
                   "Receiver",
                   "wsntw:SubscribeCreationFailedFault",
                   "wsntw:SubscribeCreationFailedFault",
                   "Subscribe creation failed fault",
                   "");
        return -5;
    }

    // Force notification with Property "Initialized"
    sem_memory_wait();
    subs_evts->subscriptions[sub_index].push_need_sync = 1;
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    sprintf(events_service_address, "http://%s%s/onvif/events_service?sub=%d", my_address, my_port, subscription_id);

    gen_uuid(msg_uuid);

    relates_to_uuid = get_element("MessageID", "Header");
    if (relates_to_uuid == NULL) {
        log_error("No MessageID element for Subscribe method");
        send_action_failed_fault("events_service", -6);
        return -6;
    }

    to_iso_date(iso_str, sizeof(iso_str), now);
    to_iso_date(iso_str_2, sizeof(iso_str_2), expire_time);

    log_info("Created PUSH subscription: id=%d, slot=%d, reference=%s, expire=%s", subscription_id, sub_index, address, iso_str_2);
    log_debug("Subscription data: reference %s - expire %s - subscription index %d", address, iso_str_2, subscription_id);

    long size = cat(NULL,
                    "events_service_files/Subscribe.xml",
                    10,
                    "%MSG_UUID%",
                    msg_uuid,
                    "%REL_TO_UUID%",
                    relates_to_uuid,
                    "%REFERENCE%",
                    events_service_address,
                    "%CURRENT_TIME%",
                    iso_str,
                    "%TERMINATION_TIME%",
                    iso_str_2);

    output_http_headers(size);

    return cat("stdout",
               "events_service_files/Subscribe.xml",
               10,
               "%MSG_UUID%",
               msg_uuid,
               "%REL_TO_UUID%",
               relates_to_uuid,
               "%REFERENCE%",
               events_service_address,
               "%CURRENT_TIME%",
               iso_str,
               "%TERMINATION_TIME%",
               iso_str_2);
}

int events_renew()
{
    const char *tt;
    char msg_uuid[UUID_LEN + 1];
    const char *relates_to_uuid;
    time_t now, expire_time;
    char iso_str[21];
    char iso_str_2[21];

    int i;
    int qs_size;
    char *qs_string;
    char *sub_id_s;
    int sub_id, sub_index;

    // Subscription manager replies to address http://%s%s/onvif/events_service?sub=%d
    log_info("Renew request received");

    // Debug: log the QUERY_STRING environment variable
    char *query_env = getenv("QUERY_STRING");
    log_debug("QUERY_STRING environment variable: '%s'", query_env ? query_env : "(null)");

    // Get parameters from query string
    get_from_query_string(&qs_string, &qs_size, "sub");
    if ((qs_size == -1) || (qs_string == NULL)) {
        log_error("No sub parameter in query string for Renew method (QUERY_STRING='%s')", query_env ? query_env : "(null)");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "missing sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -1;
    }

    log_debug("Extracted sub parameter: '%.*s' (size=%d)", qs_size, qs_string, qs_size);
    sub_id_s = (char *) malloc((qs_size + 1) * sizeof(char));
    memset(sub_id_s, '\0', qs_size + 1);
    strncpy(sub_id_s, qs_string, qs_size);

    // Validate that the string contains only digits
    int valid = 1;
    for (i = 0; i < qs_size; i++) {
        if (sub_id_s[i] < '0' || sub_id_s[i] > '9') {
            valid = 0;
            break;
        }
    }

    if (!valid) {
        log_error("Invalid sub parameter (non-numeric) for Renew method: '%s'", sub_id_s);
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "non-numeric sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        free(sub_id_s);
        send_fault("events_service",
                   "Receiver",
                   "wsrf-rw:ResourceUnknownFault",
                   "wsrf-rw:ResourceUnknownFault",
                   "Resource unknown",
                   "Invalid subscription ID format");
        return -2;
    }

    sub_id = atoi(sub_id_s);
    free(sub_id_s);

    if ((sub_id <= 0) || (sub_id > 65535)) {
        log_error("sub index out of range for Renew method: %d (valid range: 1-65535)", sub_id);
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "sub out of range",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service",
                   "Receiver",
                   "wsrf-rw:ResourceUnknownFault",
                   "wsrf-rw:ResourceUnknownFault",
                   "Resource unknown",
                   "Subscription ID out of valid range");
        return -2;
    }

    log_debug("Renew request for subscription ID: %d", sub_id);

    // Get parameters from xml content
    time(&now);
    tt = get_element("TerminationTime", "Body");
    if (tt == NULL) {
        log_error("No TerminationTime element for Renew method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "missing TerminationTime",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service",
                   "Receiver",
                   "wsntw:UnacceptableTerminationTimeFault",
                   "wsntw:UnacceptableTerminationTimeFault",
                   "Unacceptable termination time",
                   "");
        return -3;
    } else {
        if (strncmp("PT", tt, 2) == 0) {
            expire_time = now + interval2sec(tt);
        } else {
            expire_time = from_iso_date(tt);
        }
    }

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_action_failed_fault("events_service", -4);
        return -4;
    }

    // Find subscription
    sem_memory_wait();
    sub_index = -1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].id == sub_id) {
            sub_index = i;
            break;
        }
    }
    if ((sub_index < 0) || (sub_index >= MAX_SUBSCRIPTIONS)) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("Sub index (%d) out of range for Renew method", sub_index);
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "subscription not found",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -5;
    }

    if (subs_evts->subscriptions[sub_index].used == SUB_UNUSED) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("This subscription is empty or expired");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "Renew",
                              "subscription empty or expired",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -6;
    }
    subs_evts->subscriptions[sub_index].expire = expire_time;
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    gen_uuid(msg_uuid);
    relates_to_uuid = get_element("MessageID", "Header");
    if (relates_to_uuid == NULL) {
        // Be tolerant: some clients omit MessageID in Renew
        log_warn("Renew: missing MessageID; generating one for RelatesTo");
        static char rel_uuid_buf[UUID_LEN + 1];
        gen_uuid(rel_uuid_buf);
        relates_to_uuid = rel_uuid_buf;
    }

    to_iso_date(iso_str, sizeof(iso_str), now);
    to_iso_date(iso_str_2, sizeof(iso_str_2), expire_time);

    log_debug("Subscription data: expire %s - subscription index %d", iso_str_2, sub_index);

    long size = cat(NULL,
                    "events_service_files/Renew.xml",
                    8,
                    "%MSG_UUID%",
                    msg_uuid,
                    "%REL_TO_UUID%",
                    relates_to_uuid,
                    "%CURRENT_TIME%",
                    iso_str,
                    "%TERMINATION_TIME%",
                    iso_str_2);

    output_http_headers(size);

    return cat("stdout",
               "events_service_files/Renew.xml",
               8,
               "%MSG_UUID%",
               msg_uuid,
               "%REL_TO_UUID%",
               relates_to_uuid,
               "%CURRENT_TIME%",
               iso_str,
               "%TERMINATION_TIME%",
               iso_str_2);
}

/*
 * Split a concrete ONVIF topic into up to 3 path components.
 * Returns the number of components (0..3), or -1 when the topic has more
 * than 3 levels (not supported by the TopicSet template).
 */
static int split_topic(const char *topic, char comps[][128])
{
    char buf[1024];
    int levels = 0;
    char *token;

    if ((topic == NULL) || (topic[0] == '\0'))
        return 0;

    snprintf(buf, sizeof(buf), "%s", topic);
    token = strtok(buf, "/");
    while ((token != NULL) && (levels < 3)) {
        snprintf(comps[levels], 128, "%s", token);
        levels++;
        token = strtok(NULL, "/");
    }

    if (token != NULL)
        return -1;

    return levels;
}

/*
 * Build the opening tag of one TopicSet level. Topic paths specify the
 * namespace only on the first component (e.g. tns1:VideoSource/MotionAlarm),
 * so every component without a prefix stays in the ONVIF topics namespace.
 */
static void topic_level_open(char *out, size_t outlen, const char *comp, int leaf)
{
    if (strchr(comp, ':') != NULL)
        snprintf(out, outlen, "<%s%s>", comp, leaf ? "  wstop:topic=\"true\"" : "");
    else
        snprintf(out, outlen, "<tns1:%s%s>", comp, leaf ? "  wstop:topic=\"true\"" : "");
}

static void topic_level_close(char *out, size_t outlen, const char *comp)
{
    if (strchr(comp, ':') != NULL)
        snprintf(out, outlen, "</%s>", comp);
    else
        snprintf(out, outlen, "</tns1:%s>", comp);
}

/* Data item carried by a known event topic (State/IsMotion/IsPeopleDetected/...) */
static void event_data_item(const char *topic, char *data_name, char *data_type)
{
    if ((topic != NULL) && (strcmp(topic, "tns1:Device/Trigger/Relay") == 0)) {
        strcpy(data_name, "LogicalState");
        strcpy(data_type, "tt:RelayLogicalState");
    } else if ((topic != NULL) && (strstr(topic, "VideoSource/MotionAlarm") != NULL)) {
        // ONVIF event catalog: VideoSource/MotionAlarm advertises State
        strcpy(data_name, "State");
        strcpy(data_type, "xsd:boolean");
    } else if ((topic != NULL) && (strstr(topic, "CellMotionDetector/Motion") != NULL)) {
        // ONVIF event catalog: CellMotionDetector/Motion advertises IsMotion
        strcpy(data_name, "IsMotion");
        strcpy(data_type, "xsd:boolean");
    } else if ((topic != NULL) && (strstr(topic, "MyRuleDetector/PeopleDetect") != NULL)) {
        // RuleEngine/MyRuleDetector/PeopleDetect advertises IsPeopleDetected
        strcpy(data_name, "IsPeopleDetected");
        strcpy(data_type, "xsd:boolean");
    } else {
        strcpy(data_name, "State");
        strcpy(data_type, "xsd:boolean");
    }
}

static long emit_event_topic_block(char *dest,
                                   const char *l1s, const char *l2s, const char *l3s,
                                   const char *sources_xml,
                                   const char *data_name, const char *data_type,
                                   const char *l3e, const char *l2e, const char *l1e)
{
    return cat(dest,
               "events_service_files/GetEventProperties_2.xml",
               18,
               "%TOPIC_L1_START%", l1s,
               "%TOPIC_L2_START%", l2s,
               "%TOPIC_L3_START%", l3s,
               "%SOURCES%", sources_xml,
               "%DATA_NAME%", data_name,
               "%DATA_TYPE%", data_type,
               "%TOPIC_L3_END%", l3e,
               "%TOPIC_L2_END%", l2e,
               "%TOPIC_L1_END%", l1e);
}

int events_get_event_properties()
{
    int c, i, j, k;
    int event_count;
    int depth[MAX_EVENTS];
    unsigned char valid[MAX_EVENTS];
    unsigned char skip[MAX_EVENTS];
    unsigned char emitted[MAX_EVENTS];
    char comps[MAX_EVENTS][3][128];
    char l1s[256], l1e[256];
    char l2s[256], l2e[256];
    char l3s[256], l3e[256];
    char sources_xml[512];
    char data_name[32];
    char data_type[32];
    long size, total_size;
    char dest_a[] = "stdout";
    char *dest;

    /*
     * Parse all configured topics once: the TopicSet has to be emitted as a
     * real tree, so events sharing a topic path must be grouped together
     * instead of repeating the whole L1/L2 branch for each event.
     */
    event_count = service_ctx.events_num;
    if (event_count > MAX_EVENTS)
        event_count = MAX_EVENTS;

    memset(valid, 0, sizeof(valid));
    memset(skip, 0, sizeof(skip));
    // Components are compared as fixed-size blocks below, so clear them
    // to avoid comparing uninitialized tail bytes.
    memset(comps, 0, sizeof(comps));

    for (i = 0; i < event_count; i++) {
        if (service_ctx.events[i].topic == NULL) {
            log_warn("Skipping event %d with NULL topic in GetEventProperties", i);
            continue;
        }
        depth[i] = split_topic(service_ctx.events[i].topic, comps[i]);
        if (depth[i] < 0) {
            log_error("The topic has too many levels");
            send_action_failed_fault("events_service", -1);
            return -1;
        }
        if (depth[i] == 0) {
            log_warn("Skipping event %d with empty topic in GetEventProperties", i);
            continue;
        }
        valid[i] = 1;
    }

    /*
     * Collapse duplicate topic paths (e.g. two relays both advertising
     * tns1:Device/Trigger/Relay) so each topic is advertised only once.
     */
    for (i = 0; i < event_count; i++) {
        if (!valid[i])
            continue;
        for (j = 0; j < i; j++) {
            if (!valid[j] || (depth[j] != depth[i]))
                continue;
            if (memcmp(comps[j], comps[i], depth[i] * 128) == 0) {
                skip[i] = 1;
                break;
            }
        }
    }

    // We need 1st step to evaluate content length
    for (c = 0; c < 2; c++) {
        if (c == 0) {
            dest = NULL;
        } else {
            dest = dest_a;
            output_http_headers(total_size);
        }

        size = cat(dest, "events_service_files/GetEventProperties_1.xml", 0);
        if (c == 0)
            total_size = size;

        int included_any_topic = 0;
        memset(emitted, 0, sizeof(emitted));

        for (i = 0; i < event_count; i++) {
            if (!valid[i] || skip[i] || emitted[i])
                continue;

            // A single-level topic is a leaf and cannot be merged with siblings
            if (depth[i] == 1) {
                topic_level_open(l1s, sizeof(l1s), comps[i][0], 1);
                topic_level_close(l1e, sizeof(l1e), comps[i][0]);
                event_data_item(service_ctx.events[i].topic, data_name, data_type);
                build_event_source_descriptions(sources_xml, sizeof(sources_xml),
                                                &service_ctx.events[i]);
                size = emit_event_topic_block(dest, l1s, "", "", sources_xml,
                                              data_name, data_type, "", "", l1e);
                if (c == 0)
                    total_size += size;
                emitted[i] = 1;
                included_any_topic = 1;
                continue;
            }

            // Index of the last event sharing this L1 element: it closes L1
            int last_l1 = i;
            for (k = i; k < event_count; k++) {
                if (!valid[k] || skip[k] || (depth[k] < 2))
                    continue;
                if (strcmp(comps[k][0], comps[i][0]) == 0)
                    last_l1 = k;
            }

            for (j = i; j < event_count; j++) {
                if (!valid[j] || skip[j] || emitted[j] || (depth[j] < 2))
                    continue;
                if (strcmp(comps[j][0], comps[i][0]) != 0)
                    continue;

                l1s[0] = l1e[0] = l2s[0] = l2e[0] = l3s[0] = l3e[0] = '\0';

                if (j == i)
                    topic_level_open(l1s, sizeof(l1s), comps[j][0], 0);
                if (j == last_l1)
                    topic_level_close(l1e, sizeof(l1e), comps[j][0]);

                if (depth[j] == 2) {
                    // Two-level topic: L2 is the leaf (e.g. VideoSource/MotionAlarm)
                    topic_level_open(l2s, sizeof(l2s), comps[j][1], 1);
                    topic_level_close(l2e, sizeof(l2e), comps[j][1]);
                } else {
                    int first_l2 = 1;
                    int last_l2 = 1;

                    for (k = i; k < j; k++) {
                        if (!valid[k] || skip[k] || (depth[k] != 3))
                            continue;
                        if ((strcmp(comps[k][0], comps[j][0]) == 0)
                            && (strcmp(comps[k][1], comps[j][1]) == 0)) {
                            first_l2 = 0;
                            break;
                        }
                    }
                    for (k = j + 1; k < event_count; k++) {
                        if (!valid[k] || skip[k] || (depth[k] != 3))
                            continue;
                        if ((strcmp(comps[k][0], comps[j][0]) == 0)
                            && (strcmp(comps[k][1], comps[j][1]) == 0)) {
                            last_l2 = 0;
                            break;
                        }
                    }

                    if (first_l2)
                        topic_level_open(l2s, sizeof(l2s), comps[j][1], 0);
                    if (last_l2)
                        topic_level_close(l2e, sizeof(l2e), comps[j][1]);

                    topic_level_open(l3s, sizeof(l3s), comps[j][2], 1);
                    topic_level_close(l3e, sizeof(l3e), comps[j][2]);
                }

                event_data_item(service_ctx.events[j].topic, data_name, data_type);
                build_event_source_descriptions(sources_xml, sizeof(sources_xml),
                                                &service_ctx.events[j]);

                size = emit_event_topic_block(dest, l1s, l2s, l3s, sources_xml,
                                              data_name, data_type, l3e, l2e, l1e);
                if (c == 0)
                    total_size += size;
                emitted[j] = 1;
                included_any_topic = 1;
            }
        }

        // Fallback: if no topics are configured, still advertise Motion capability for compatibility (e.g., tinyCam)
        if (!included_any_topic) {
            // L1: tns1:VideoSource, L2: MotionAlarm, no L3
            topic_level_open(l1s, sizeof(l1s), "tns1:VideoSource", 0);
            topic_level_close(l1e, sizeof(l1e), "tns1:VideoSource");
            topic_level_open(l2s, sizeof(l2s), "MotionAlarm", 1);
            topic_level_close(l2e, sizeof(l2e), "MotionAlarm");
            strcpy(data_name, "State");
            strcpy(data_type, "xsd:boolean");
            // Spec: MotionAlarm source is "Source" (the video source token)
            snprintf(sources_xml, sizeof(sources_xml),
                     "<tt:SimpleItemDescription Name=\"Source\" Type=\"tt:ReferenceToken\"/>");
            size = emit_event_topic_block(dest, l1s, l2s, "", sources_xml,
                                          data_name, data_type, "", l2e, l1e);
            if (c == 0)
                total_size += size;
        }

        // Advertise PTZ preset events when PTZ is enabled (spec §5.11.1)
        if (service_ctx.ptz_node.enable == 1) {
            size = cat(dest, "events_service_files/GetEventProperties_PTZ.xml", 0);
            if (c == 0)
                total_size += size;
        }

        size = cat(dest, "events_service_files/GetEventProperties_3.xml", 0);
        if (c == 0)
            total_size += size;
    }

    return total_size;
}

int events_unsubscribe()
{
    int qs_size;
    char *qs_string;
    char *sub_id_s;
    int sub_id, sub_index;
    int i;

    // Subscription manager replies to address http://%s%s/onvif/events_service?sub=%d
    log_info("Unsubscribe request received");

    // Debug: log the QUERY_STRING and client IP address
    char *query_env = getenv("QUERY_STRING");
    char *client_ip = getenv("REMOTE_ADDR");
    log_debug("Client IP: '%s'", client_ip ? client_ip : "(unknown)");
    log_debug("QUERY_STRING environment variable: '%s'", query_env ? query_env : "(null)");

    // Get parameters from query string
    get_from_query_string(&qs_string, &qs_size, "sub");
    if ((qs_size == -1) || (qs_string == NULL)) {
        log_error("No sub parameter in query string for Unsubscribe method (client=%s, QUERY_STRING='%s')",
                  client_ip ? client_ip : "(unknown)",
                  query_env ? query_env : "(null)");
        // Error-time raw XML logging
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              client_ip,
                              "events_service",
                              "Unsubscribe",
                              "missing sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -1;
    }

    log_debug("Extracted sub parameter: '%.*s' (size=%d)", qs_size, qs_string, qs_size);

    sub_id_s = (char *) malloc((qs_size + 1) * sizeof(char));
    memset(sub_id_s, '\0', qs_size + 1);
    strncpy(sub_id_s, qs_string, qs_size);

    // Validate that the string contains only digits
    int valid = 1;
    for (i = 0; i < qs_size; i++) {
        if (sub_id_s[i] < '0' || sub_id_s[i] > '9') {
            valid = 0;
            break;
        }
    }

    if (!valid) {
        char *client_ip = getenv("REMOTE_ADDR");
        log_error("Invalid sub parameter (non-numeric) for Unsubscribe method from %s: '%s'", client_ip ? client_ip : "(unknown)", sub_id_s);
        // Error-time raw XML logging
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              client_ip,
                              "events_service",
                              "Unsubscribe",
                              "non-numeric sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        free(sub_id_s);
        send_fault("events_service",
                   "Receiver",
                   "wsrf-rw:ResourceUnknownFault",
                   "wsrf-rw:ResourceUnknownFault",
                   "Resource unknown",
                   "Invalid subscription ID format");
        return -2;
    }

    sub_id = atoi(sub_id_s);
    free(sub_id_s);

    if ((sub_id <= 0) || (sub_id > 65535)) {
        char *client_ip = getenv("REMOTE_ADDR");
        log_error("sub index out of range for Unsubscribe method from %s: %d (valid range: 1-65535)", client_ip ? client_ip : "(unknown)", sub_id);
        // Error-time raw XML logging
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              client_ip,
                              "events_service",
                              "Unsubscribe",
                              "sub out of range",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service",
                   "Receiver",
                   "wsrf-rw:ResourceUnknownFault",
                   "wsrf-rw:ResourceUnknownFault",
                   "Resource unknown",
                   "Subscription ID out of valid range");
        return -2;
    }

    log_debug("Unsubscribe request for subscription ID: %d", sub_id);

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_action_failed_fault("events_service", -3);
        return -3;
    }

    // Find subscription
    sem_memory_wait();
    sub_index = -1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].id == sub_id) {
            sub_index = i;
            break;
        }
    }
    if ((sub_index < 0) || (sub_index >= MAX_SUBSCRIPTIONS)) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("sub index out of range for PullMessages method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "SetSynchronizationPoint",
                              "subscription not found",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -4;
    }

    if (subs_evts->subscriptions[sub_index].used == SUB_UNUSED) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -5;
    }

    // Log subscription details before clearing
    subscription_type sub_type = subs_evts->subscriptions[sub_index].used;
    log_info("Unsubscribed: id=%d, slot=%d, type=%s", sub_id, sub_index, sub_type == SUB_PULL ? "PULL" : "PUSH");

    memset(&(subs_evts->subscriptions[sub_index]), '\0', sizeof(subscription_shm_t));
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    log_debug("Subscription data: subscription index %d", sub_index);

    long size = cat(NULL, "events_service_files/Unsubscribe.xml", 0);

    output_http_headers(size);

    return cat("stdout", "events_service_files/Unsubscribe.xml", 0);
}

int events_set_synchronization_point()
{
    int i;
    int qs_size;
    char *qs_string;
    char *sub_id_s;
    int sub_id, sub_index;

    // Subscription manager replies to address http://%s%s/onvif/events_service?sub=%d

    // Get parameters from query string
    get_from_query_string(&qs_string, &qs_size, "sub");
    if ((qs_size == -1) || (qs_string == NULL)) {
        log_error("No sub parameter in query string for Renew method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "SetSynchronizationPoint",
                              "missing sub",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -1;
    }
    sub_id_s = (char *) malloc((qs_size + 1) * sizeof(char));
    memset(sub_id_s, '\0', qs_size + 1);
    strncpy(sub_id_s, qs_string, qs_size);
    sub_id = atoi(sub_id_s);
    free(sub_id_s);
    if ((sub_id <= 0) || (sub_id > MAX_SUBSCRIPTIONS)) {
        log_error("sub index out of range for Renew method");
        // Error-time raw XML logging with specific reason
        size_t raw_sz = 0;
        const char *raw = get_raw_request_data(&raw_sz);
        log_xml_error_request(raw,
                              (int) raw_sz,
                              getenv("REMOTE_ADDR"),
                              "events_service",
                              "SetSynchronizationPoint",
                              "sub out of range",
                              getenv("REQUEST_URI"),
                              getenv("QUERY_STRING"));
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -2;
    }

    subs_evts = (shm_t *) create_shared_memory(0);
    if (subs_evts == NULL) {
        log_error("No shared memory found, is onvif_notify_server running?");
        send_action_failed_fault("events_service", -3);
        return -3;
    }

    // Find subscription
    sem_memory_wait();
    sub_index = -1;
    for (i = 0; i < MAX_SUBSCRIPTIONS; i++) {
        if (subs_evts->subscriptions[i].id == sub_id) {
            sub_index = i;
            break;
        }
    }
    if ((sub_index < 0) || (sub_index >= MAX_SUBSCRIPTIONS)) {
        sem_memory_post();
        destroy_shared_memory((void *) subs_evts, 0);
        log_error("sub index out of range for PullMessages method");
        send_fault("events_service", "Receiver", "wsrf-rw:ResourceUnknownFault", "wsrf-rw:ResourceUnknownFault", "Resource unknown", "");
        return -4;
    }

    log_info("SetSynchronizationPoint request received for subscription id=%d, index=%d", sub_id, sub_index);

    subs_evts->subscriptions[sub_index].push_need_sync = 1;

    for (i = 0; i < service_ctx.events_num && i < MAX_EVENTS; i++) {
        if (service_ctx.events[i].topic != NULL
            && is_topic_in_expression(subs_evts->subscriptions[sub_index].topic_expression, service_ctx.events[i].topic)) {
            subs_evts->events[i].pull_send_initialized |= (1 << sub_index);
            subs_evts->events[i].pull_notify |= (1 << sub_index);
            log_debug("Event %d matches topic expression %s", i, subs_evts->subscriptions[sub_index].topic_expression);
        }
    }
    sem_memory_post();
    destroy_shared_memory((void *) subs_evts, 0);

    long size = cat(NULL, "events_service_files/SetSynchronizationPoint.xml", 0);

    output_http_headers(size);

    return cat("stdout", "events_service_files/SetSynchronizationPoint.xml", 0);
}

int events_unsupported(const char *method)
{
    if (service_ctx.adv_fault_if_unknown == 1)
        send_action_failed_fault("events_service", -1);
    else
        send_empty_response("tev", (char *) method);
    return -1;
}
