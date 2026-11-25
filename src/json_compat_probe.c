/*
 * Build-time probe for the json_config.h (jct) numeric layout.
 *
 * This file is only compiled for syntax checking by the Makefile to decide
 * whether JsonValue.value.number is a plain scalar (older jct) or a
 * JsonNumberValue with an integer/real member (jct >= 1.2).
 * It is never linked into any binary.
 */
#include <json_config.h>

int main(void)
{
    JsonValue *p = 0;

    return (int) (p->value.number.real);
}
