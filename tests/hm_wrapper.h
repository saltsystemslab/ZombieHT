#ifndef HM_WRAPPER_H
#define HM_WRAPPER_H

#ifdef USE_ABSL
#include "absl_wrapper.h"
#elif USE_ICEBERG
#include "iceberg_wrapper.h"
#elif USE_CLHT
#include "clht_wrapper.h"
#else
#include "qfhm_wrapper.h"
#endif 

#endif
