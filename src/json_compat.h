/*
 * Compatibility helpers for the two json_config.h (jct) numeric layouts:
 *  - older jct: JsonValue.value.number is a plain numeric scalar
 *  - jct >= 1.2: JsonValue.value.number is JsonNumberValue with
 *                integer/real members
 *
 * The Makefile probes the installed header and defines
 * JSON_NUMBER_IS_STRUCT accordingly (0 or 1).
 */
#ifndef JSON_COMPAT_H
#define JSON_COMPAT_H

#include <json_config.h>

#ifndef JSON_NUMBER_IS_STRUCT
#define JSON_NUMBER_IS_STRUCT 0
#endif

#if JSON_NUMBER_IS_STRUCT
#define JSON_NUMBER_DOUBLE(v) ((double) (v)->value.number.real)
#define JSON_NUMBER_INT(v) ((long long) (v)->value.number.integer)
#else
#define JSON_NUMBER_DOUBLE(v) ((double) (v)->value.number)
#define JSON_NUMBER_INT(v) ((long long) (v)->value.number)
#endif

#endif /* JSON_COMPAT_H */
