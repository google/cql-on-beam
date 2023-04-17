/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.fhir.cql.beam;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Predicate;
import java.util.stream.Stream;

/** Static utilities for loading file contents into a {@code List<String>}. */
final class FileLoader {
  private FileLoader() {}

  /**
   * Returns a list with an entry, containing the file's content, for each file in the specified
   * directory. Content is read as a UTF-8 string. Files in subdirectories are not considered.
   *
   * @param directory a path to a directory that contains the files to load
   * @param pathFilter a filter that controls which files are read into the returned list
   * @throws IOException when an error is encountered listing or reading files in the directory
   */
  public static ImmutableList<String> loadFilesInDirectory(
      Path directory, Predicate<Path> pathFilter) throws IOException {
    ImmutableList.Builder<String> result = ImmutableList.builder();
    try (Stream<Path> paths =
        Files.list(directory).filter(Files::isRegularFile).filter(pathFilter)) {
      for (Path path : (Iterable<Path>) paths::iterator) {
        result.add(Files.readString(path, StandardCharsets.UTF_8));
      }
    }
    return result.build();
  }
}
