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

import static com.google.common.base.Predicates.alwaysFalse;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileLoader}. */
@RunWith(JUnit4.class)
public class FileLoaderTest {
  @Rule public final TemporaryFolder tempFolder = new TemporaryFolder();

  private Path tempFolderPath() {
    return tempFolder.getRoot().toPath();
  }

  private void writeToNewTempFile(String content) throws IOException {
    Files.writeString(tempFolder.newFile().toPath(), content, StandardCharsets.UTF_8);
  }

  @Test
  public void loadFilesInDirectory_nonExistentDirectory() throws IOException {
    Path nonExistentDirectory = tempFolder.getRoot().toPath().resolve("foo");

    assertThrows(NoSuchFileException.class,
        () -> FileLoader.loadFilesInDirectory(nonExistentDirectory, alwaysTrue()));
  }

  @Test
  public void loadFilesInDirectory_emptyDirectory() throws IOException {
    Collection<String> result = FileLoader.loadFilesInDirectory(tempFolderPath(), alwaysTrue());
    assertThat(result).isEmpty();
  }

  @Test
  public void loadFilesInDirectory_readsOneFile() throws IOException {
    writeToNewTempFile("foo");

    Collection<String> result = FileLoader.loadFilesInDirectory(tempFolderPath(), alwaysTrue());
    assertThat(result).containsExactly("foo");
  }

  @Test
  public void loadFilesInDirectory_ignoresFilteredFiles() throws IOException {
    writeToNewTempFile("foo");

    Collection<String> result = FileLoader.loadFilesInDirectory(tempFolderPath(), alwaysFalse());
    assertThat(result).isEmpty();
  }

  @Test
  public void loadFilesInDirectory_readsMultipleFiles() throws IOException {
    writeToNewTempFile("foo");
    writeToNewTempFile("bar");

    Collection<String> result = FileLoader.loadFilesInDirectory(tempFolderPath(), alwaysTrue());
    assertThat(result).containsExactly("foo", "bar");
  }

  @Test
  public void loadFilesInDirectory_ignoresSubdirectories() throws IOException {
    Files.writeString(
        tempFolder.newFolder("subdirectory").toPath().resolve("file"),
        "content",
        StandardCharsets.UTF_8);

    Collection<String> result = FileLoader.loadFilesInDirectory(tempFolderPath(), alwaysTrue());
    assertThat(result).isEmpty();
  }
}
