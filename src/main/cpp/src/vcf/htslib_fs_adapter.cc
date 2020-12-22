/**
 * The MIT License (MIT)
 * Copyright (c) 2020 Omics Data Automation, Inc.
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

#include "hfile_genomicsdb.h"

#include "tiledb.h"
#include "tiledb_storage.h"

#include <mutex>

std::once_flag initialize_once;

void genomicsdb_htslib_plugin_initialize(const char* filename) {
  if (strstr(filename, "://")) {
    std::call_once(initialize_once, [&](){
        auto htslib_scheme_handler = find_scheme_handler(filename);
        if (htslib_scheme_handler) {
           // Register genomicsdb supported schemes
          hfile_plugin_init(NULL);
        }
      });
  }
}

void *genomicsdb_filesystem_init(const char *filename) {
  TileDB_CTX* tiledb_ctx;
  TileDB_Config tiledb_config;
  memset(&tiledb_config, 0, sizeof(TileDB_Config));
  tiledb_config.home_ = filename;
  if (tiledb_ctx_init(&tiledb_ctx, &tiledb_config)) {
    errno = EIO;
    return NULL;
  }
  return tiledb_ctx;
}

size_t genomicsdb_filesize(void *context, const char *filename) {
  return file_size(reinterpret_cast<TileDB_CTX *>(context), filename);
}

ssize_t genomicsdb_filesystem_read(void *context, const char *filename, off_t offset, void *buffer, size_t length) {
  if (read_file(reinterpret_cast<TileDB_CTX *>(context), filename, offset, buffer, length)) {
    return -1;
  } else {
    return length;
  }
}

ssize_t genomicsdb_filesystem_write(void *context, const char *filename, const void *buffer, size_t nbytes) {
  if (write_file(reinterpret_cast<TileDB_CTX *>(context), filename, buffer, nbytes)) {
    return -1;
  } else {
    return nbytes;
  }
}

int genomicsdb_filesystem_close(void *context, const char *filename) {
  return close_file(reinterpret_cast<TileDB_CTX *>(context), filename);
}

