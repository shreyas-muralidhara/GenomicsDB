/**
 * The MIT License (MIT)
 * Copyright (c) 2016-2017 Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

#ifndef GENOMICSDB_ERROR_H
#define GENOMICSDB_ERROR_H

#include<sys/types.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <string>

#define PRINT_ERROR(x) std::cerr << x << std::endl

#define SYSTEM_ERROR(PREFIX, MSG, PATH)                              \
  do {                                                               \
    std::string errmsg = PREFIX + "(" + __func__ + ") " + MSG;       \
    std::string errpath = PATH;                                      \
    if (errpath.length() > 0) {                                      \
      errmsg +=  "; path=" + errpath;                                \
    }                                                                \
    if (errno > 0) {                                                 \
      errmsg += "; errno=" + std::to_string(errno) + "(" + std::string(std::strerror(errno)) + ")"; \
    }                                                                \
    PRINT_ERROR(errmsg);                                             \
    errno = 0;                                                       \
  } while (false)

#define GENOMICSDB_ERROR(PREFIX, MSG)                                \
  do {                                                               \
    std::string errmsg = PREFIX + "(" + __func__ + ") " + MSG;       \
    PRINT_ERROR(errmsg);                                             \
  } while (false)

void reset_errno();

#define GENOMICSDB_INFO(PREFIX, MSG)                                \
  do {                                                              \
    std::string infomsg = PREFIX + "(" + __func__ + ") " + MSG;     \
    PRINT_ERROR(infomsg);                                            \
  } while (false)

void reset_errno();

#endif /* GENOMICSDB_ERROR_H */
