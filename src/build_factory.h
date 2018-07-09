#pragma once

#include "rdb_parser_def.h"
#include "build_helper.h"

#include "build_header.h"
#include "build_body_db_selector.h"
#include "build_body_aux_fields.h"
#include "build_body_kv.h"
#include "build_footer.h"

#include "build_body_kv_load_value.h"

/* build error code */
#define RDB_PHASE_BUILD_OK                           0
#define RDB_PHASE_BUILD_ERROR_INVALID_PATH          -1
#define RDB_PHASE_BUILD_ERROR_PREMATURE             -2
#define RDB_PHASE_BUILD_ERROR_INVALID_MAGIC_STRING  -3
#define RDB_PHASE_BUILD_ERROR_INVALID_NODE_TYPE     -4

/* main phase */
enum RDB_PHASE {
	RDB_PHASE_NULL = 0,
	RDB_PHASE_HEADER,
	RDB_PHASE_BODY_DB_SELECTOR,
	RDB_PHASE_BODY_AUX_FIELDS,
	RDB_PHASE_BODY_KV,
	RDB_PHASE_FOOTER,

};

/* EOF */